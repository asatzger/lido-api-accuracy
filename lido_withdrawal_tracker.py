import os
import json
import boto3
import requests
import time
from datetime import datetime, UTC
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

# Load environment variables from .env file
load_dotenv()

# Configuration
API_URL = "https://wq-api.lido.fi/v2/request-time"
BATCH_SIZE = 20
TABLE_NAME = "lido_withdrawal_requests"
REGION_NAME = os.getenv('AWS_REGION', 'us-east-1')  # Use environment variable with fallback
MAX_REQUEST_ID = 75000  # Upper bound for request IDs

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
            # Create the table with composite key
            table = dynamodb.create_table(
                TableName=TABLE_NAME,
                KeySchema=[
                    {'AttributeName': 'withdrawal_id', 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'withdrawal_id', 'AttributeType': 'N'},
                    {'AttributeName': 'timestamp', 'AttributeType': 'S'}
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
    
    retry_count = 0
    while retry_count < max_retries:
        try:
            params = [('ids', str(id_val)) for id_val in batch_ids]
            response = requests.get(API_URL, params=params)
            
            if response.status_code != 200:
                print(f"Error: API returned {response.status_code} for IDs {batch_ids[0]}-{batch_ids[-1]}")
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(2 ** retry_count)
                    continue
                return [], None
            
            data = response.json()
            
            if not data:
                print(f"No data received for IDs {batch_ids[0]}-{batch_ids[-1]}")
                return [], None
            
            # Process each result
            if isinstance(data, list):
                current_time = datetime.now(UTC).isoformat()
                for idx, result in enumerate(data):
                    id_val = batch_ids[idx]
                    if result is None:
                        continue
                    
                    # Check if we already have this record
                    try:
                        existing_records = table.query(
                            KeyConditionExpression='withdrawal_id = :id AND #ts = :ts',
                            ExpressionAttributeNames={'#ts': 'timestamp'},
                            ExpressionAttributeValues={
                                ':id': id_val,
                                ':ts': current_time
                            }
                        )
                        if existing_records['Items']:
                            continue
                    except Exception as e:
                        print(f"Error checking existing record for ID {id_val}: {e}")
                        continue
                    
                    # Process the withdrawal data
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
            
            return results, lowest_non_finalized_id
            
        except Exception as e:
            print(f"Error processing batch {batch_ids[0]}-{batch_ids[-1]}: {e}")
            retry_count += 1
            if retry_count < max_retries:
                time.sleep(2 ** retry_count)
            else:
                return [], None
    
    return [], None

def fetch_withdrawal_data(start_id, table):
    """Fetch withdrawal data from the API using multiple threads"""
    all_results = []
    lowest_non_finalized_id = None
    max_concurrent_batches = 2
    consecutive_empty_batches = 0
    max_empty_batches = 3
    current_id = start_id
    
    print(f"\nStarting data collection from ID {start_id} (upper bound: {MAX_REQUEST_ID})")
    batch_summaries = []
    
    with ThreadPoolExecutor(max_workers=max_concurrent_batches) as executor:
        while consecutive_empty_batches < max_empty_batches and current_id < MAX_REQUEST_ID:  # Add upper bound check
            batch_start_id = current_id
            batch_futures = []
            
            # Prepare multiple batches
            for _ in range(max_concurrent_batches):
                if current_id >= MAX_REQUEST_ID:  # Check before creating new batch
                    break
                
                # Ensure we don't exceed MAX_REQUEST_ID within a batch
                end_id = min(current_id + BATCH_SIZE, MAX_REQUEST_ID)
                batch_ids = list(range(current_id, end_id))
                
                future = executor.submit(fetch_single_batch, batch_ids, table)
                batch_futures.append((current_id, future))
                current_id += BATCH_SIZE
                time.sleep(1)
            
            if not batch_futures:  # No more batches to process
                break
            
            # Process completed batches
            empty_batch_count = 0  # Count empty batches in this set
            batch_results = 0
            
            for start_id, future in batch_futures:
                try:
                    results, batch_lowest_non_finalized = future.result()
                    if not results:  # This batch was empty
                        empty_batch_count += 1
                        print(f"Empty batch {start_id}-{start_id + BATCH_SIZE - 1}")
                    else:
                        batch_results += len(results)
                        all_results.extend(results)
                        print(f"Batch {start_id}-{start_id + BATCH_SIZE - 1}: {len(results)} results")
                        
                        if batch_lowest_non_finalized:
                            if lowest_non_finalized_id is None or batch_lowest_non_finalized < lowest_non_finalized_id:
                                lowest_non_finalized_id = batch_lowest_non_finalized
                
                except Exception as e:
                    print(f"Error in batch {start_id}-{start_id + BATCH_SIZE - 1}: {e}")
                    empty_batch_count += 1
            
            # Record batch summary
            batch_summaries.append({
                'batch_range': f"{batch_start_id}-{current_id-1}",
                'results': batch_results
            })
            
            # Update consecutive empty batches counter
            if empty_batch_count == max_concurrent_batches:  # All batches in this set were empty
                consecutive_empty_batches += 1
                print(f"All batches empty ({consecutive_empty_batches}/{max_empty_batches})")
            else:
                consecutive_empty_batches = 0  # Reset if we got any results
            
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
    """Store data items in DynamoDB"""
    if not data_items:
        print("No new data to store")
        return
    
    # Get current count
    try:
        response = table.scan(
            Select='COUNT'
        )
        initial_count = response['Count']
        print(f"Current items in DB: {initial_count}")
    except Exception as e:
        print(f"Error getting initial count: {e}")
        initial_count = 0
        
    print(f"Attempting to store {len(data_items)} new records in DynamoDB...")
    
    # Track successful writes
    success_count = 0
    with table.batch_writer() as batch:
        for item in data_items:
            try:
                batch.put_item(Item=item)
                success_count += 1
            except Exception as e:
                print(f"Error storing item {item.get('withdrawal_id')}: {e}")
    
    # Get new count
    try:
        response = table.scan(
            Select='COUNT'
        )
        final_count = response['Count']
        actual_added = final_count - initial_count
        print(f"\nDatabase Update Summary:")
        print(f"Initial count: {initial_count}")
        print(f"Attempted to add: {len(data_items)}")
        print(f"Successfully written: {success_count}")
        print(f"Final count: {final_count}")
        print(f"Actual new items: {actual_added}")
    except Exception as e:
        print(f"Error getting final count: {e}")

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