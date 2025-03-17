import os
import json
import boto3
import requests
import time
from datetime import datetime, UTC, date
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

# Add a custom JSON encoder class that can handle date objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

# Create a session for connection pooling
http_session = requests.Session()

# Load environment variables from .env file
load_dotenv()

# Configuration
API_URL = "https://wq-api.lido.fi/v2/request-time"
BATCH_SIZE = 20
MAX_CONCURRENT_BATCHES = 5  # Increased from 2
TABLE_NAME = "lido_withdrawal_requests"
REGION_NAME = os.getenv('AWS_REGION', 'us-east-1')  # Use environment variable with fallback
MAX_REQUEST_ID = 80000  # Upper bound for request IDs

# Initialize AWS clients
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name=REGION_NAME
)
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name=REGION_NAME
)

def ensure_table_exists():
    """Ensure DynamoDB table exists, create it if it doesn't"""
    try:
        table = dynamodb.Table(TABLE_NAME)
        table.table_status
        print(f"Table {TABLE_NAME} exists")
        return table
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            # Create the table with optimized schema
            table = dynamodb.create_table(
                TableName=TABLE_NAME,
                KeySchema=[
                    {'AttributeName': 'status', 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': 'withdrawal_id', 'KeyType': 'RANGE'}  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'status', 'AttributeType': 'S'},
                    {'AttributeName': 'withdrawal_id', 'AttributeType': 'N'},
                    {'AttributeName': 'finalization_time', 'AttributeType': 'N'},
                    {'AttributeName': 'timestamp', 'AttributeType': 'S'}
                ],
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'finalization-index',
                        'KeySchema': [
                            {'AttributeName': 'status', 'KeyType': 'HASH'},
                            {'AttributeName': 'finalization_time', 'KeyType': 'RANGE'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        }
                    },
                    {
                        'IndexName': 'timestamp-index',
                        'KeySchema': [
                            {'AttributeName': 'withdrawal_id', 'KeyType': 'HASH'},
                            {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        }
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            # Wait for table creation
            table.meta.client.get_waiter('table_exists').wait(TableName=TABLE_NAME)
            print(f"Created table {TABLE_NAME}")
            return table
        else:
            raise

def get_last_processed_id():
    """Get the last processed ID from DynamoDB or S3"""
    try:
        # Check metadata file in S3
        response = s3_client.get_object(
            Bucket=os.environ.get('S3_BUCKET_NAME'),
            Key='metadata/last_processed_id.json'
        )
        data = json.loads(response['Body'].read().decode('utf-8'))
        return data.get('last_processed_id', 0)
    except Exception as e:
        print(f"Error getting last processed ID: {e}")
        return 73933  # Start from known ID if no previous state

def save_last_processed_id(last_id):
    """Save the last processed ID to S3"""
    try:
        s3_client.put_object(
            Bucket=os.environ.get('S3_BUCKET_NAME'),
            Key='metadata/last_processed_id.json',
            Body=json.dumps({'last_processed_id': last_id})
        )
    except Exception as e:
        print(f"Error saving last processed ID: {e}")

def fetch_single_batch(batch_ids, table):
    """Fetch and process a single batch of withdrawal IDs"""
    results = []
    non_finalized_found = False
    lowest_non_finalized_id = None
    max_retries = 3
    calculating_found = False
    
    retry_count = 0
    while retry_count < max_retries:
        try:
            params = [('ids', str(id_val)) for id_val in batch_ids]
            response = http_session.get(API_URL, params=params)
            
            if response.status_code != 200:
                print(f"Error: API returned {response.status_code} for IDs {batch_ids[0]}-{batch_ids[-1]}")
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(2 ** retry_count)
                    continue
                return [], None, False
            
            data = response.json()
            
            if not data:
                print(f"No data received for IDs {batch_ids[0]}-{batch_ids[-1]}")
                return [], None, False
            
            # Process each result
            if isinstance(data, list):
                current_time = datetime.now(UTC).isoformat()
                
                # Create a dictionary of results for faster lookup
                valid_results = {}
                for idx, result in enumerate(data):
                    id_val = batch_ids[idx]
                    if result is not None:
                        valid_results[id_val] = result
                        # Check if status is calculating
                        if result.get('status') == 'calculating':
                            calculating_found = True
                
                # Skip processing if no valid results
                if not valid_results:
                    return [], None, calculating_found
                
                # Process the withdrawal data
                for id_val, result in valid_results.items():
                    request_info = result.get('requestInfo', {}) or {}
                    withdrawal_data = {
                        'withdrawal_id': id_val,
                        'timestamp': current_time,
                        'finalization_in': request_info.get('finalizationIn'),
                        'finalization_at': request_info.get('finalizationAt'),
                        'type': request_info.get('type'),
                        'status': result.get('status'),
                        'next_calculation_at': result.get('nextCalculationAt')
                    }
                    
                    results.append(withdrawal_data)
                    
                    if result.get('status') != 'finalized':
                        non_finalized_found = True
                        if lowest_non_finalized_id is None or id_val < lowest_non_finalized_id:
                            lowest_non_finalized_id = id_val
            
            return results, lowest_non_finalized_id, calculating_found
            
        except Exception as e:
            print(f"Error processing batch {batch_ids[0]}-{batch_ids[-1]}: {e}")
            retry_count += 1
            if retry_count < max_retries:
                time.sleep(2 ** retry_count)
            else:
                return [], None, False
    
    return [], None, False

def fetch_withdrawal_data(start_id, table):
    """Fetch withdrawal data from the API using multiple threads"""
    all_results = []
    lowest_non_finalized_id = None
    current_id = start_id
    
    print(f"\nStarting data collection from ID {start_id} (upper bound: {MAX_REQUEST_ID})")
    batch_summaries = []
    
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_BATCHES) as executor:
        while current_id < MAX_REQUEST_ID:
            batch_start_id = current_id
            batch_futures = []
            
            # Prepare multiple batches
            for _ in range(MAX_CONCURRENT_BATCHES):
                if current_id >= MAX_REQUEST_ID:
                    break
                
                end_id = min(current_id + BATCH_SIZE, MAX_REQUEST_ID)
                batch_ids = list(range(current_id, end_id))
                
                future = executor.submit(fetch_single_batch, batch_ids, table)
                batch_futures.append((current_id, future))
                current_id += BATCH_SIZE
                time.sleep(1)
            
            if not batch_futures:
                break
            
            # Process completed batches
            batch_results = 0
            calculating_status_found = False
            
            for start_id, future in batch_futures:
                try:
                    results, batch_lowest_non_finalized, calculating_found = future.result()
                    if calculating_found:
                        calculating_status_found = True
                    
                    if results:
                        batch_results += len(results)
                        all_results.extend(results)
                        print(f"Batch {start_id}-{start_id + BATCH_SIZE - 1}: {len(results)} results")
                        
                        if batch_lowest_non_finalized:
                            if lowest_non_finalized_id is None or batch_lowest_non_finalized < lowest_non_finalized_id:
                                lowest_non_finalized_id = batch_lowest_non_finalized
                    else:
                        print(f"Empty batch {start_id}-{start_id + BATCH_SIZE - 1}")
                
                except Exception as e:
                    print(f"Error in batch {start_id}-{start_id + BATCH_SIZE - 1}: {e}")
            
            # Record batch summary
            batch_summaries.append({
                'batch_range': f"{batch_start_id}-{current_id-1}",
                'results': batch_results
            })
            
            # Stop if calculating status was found
            if calculating_status_found:
                print("Found calculating status - stopping further processing")
                break
            
            time.sleep(2)
    
    # Print collection summary
    print(f"\nData Collection Summary:")
    print(f"Total results collected: {len(all_results)}")
    print(f"Batch summaries:")
    for summary in batch_summaries:
        print(f"  Batch {summary['batch_range']}: {summary['results']} results")
    
    if lowest_non_finalized_id:
        print(f"Lowest non-finalized ID found: {lowest_non_finalized_id}")
    
    return all_results, lowest_non_finalized_id

def store_data_in_dynamodb(data_items, table):
    """Store withdrawal data in DynamoDB with optimized schema"""
    if not data_items:
        return
    
    try:
        with table.batch_writer() as batch:
            for item in data_items:
                # Convert withdrawal_id to number
                withdrawal_id = int(item['withdrawal_id'])
                
                # Calculate finalization_time if finalization_at exists
                finalization_time = None
                if 'finalization_at' in item:
                    try:
                        finalization_time = int(datetime.fromisoformat(item['finalization_at'].replace('Z', '+00:00')).timestamp())
                    except (ValueError, TypeError):
                        pass
                
                # Prepare item with new schema
                dynamo_item = {
                    'status': item['status'],
                    'withdrawal_id': withdrawal_id,
                    'timestamp': item['timestamp'],
                    'type': item.get('type', 'unknown'),
                    'finalization_in': item.get('finalization_in'),
                    'finalization_at': item.get('finalization_at'),
                    'next_calculation_at': item.get('next_calculation_at')
                }
                
                if finalization_time:
                    dynamo_item['finalization_time'] = finalization_time
                
                batch.put_item(Item=dynamo_item)
        
        print(f"Successfully stored {len(data_items)} items in DynamoDB")
        
    except Exception as e:
        print(f"Error storing data in DynamoDB: {e}")
        raise

def main():
    print("\n=== Starting Lido Withdrawal Tracker ===\n")
    
    # Ensure the table exists
    table = ensure_table_exists()
    
    # Get the last processed ID
    start_id = get_last_processed_id()
    print(f"Starting from ID: {start_id}")
    
    # Fetch and process withdrawal data
    data_items, lowest_non_finalized_id = fetch_withdrawal_data(start_id, table)
    
    if data_items:
        # Store data in DynamoDB
        store_data_in_dynamodb(data_items, table)
        
        # Save the lowest non-finalized ID for the next run
        if lowest_non_finalized_id:
            save_last_processed_id(lowest_non_finalized_id)
            print(f"\nNext run will start from ID: {lowest_non_finalized_id}")
        else:
            # If all requests are finalized, move to the next batch
            next_start_id = start_id + BATCH_SIZE
            save_last_processed_id(next_start_id)
            print(f"\nAll requests in this batch are finalized. Next run will start from ID: {next_start_id}")
    
    print("\n=== Lido Withdrawal Tracker Complete ===\n")

if __name__ == "__main__":
    main()