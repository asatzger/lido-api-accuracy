import os
import json
import boto3
import requests
import time
from datetime import datetime, UTC
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
API_URL = "https://wq-api.lido.fi/v2/request-time"
BATCH_SIZE = 19
TABLE_NAME = "lido_withdrawal_requests"
REGION_NAME = os.getenv('AWS_REGION', 'us-east-1')  # Use environment variable with fallback

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

def fetch_withdrawal_data(start_id, table):
    """Fetch withdrawal data from the API"""
    all_results = []
    current_id = start_id
    non_finalized_found = False
    lowest_non_finalized_id = None
    processed_ids = set()  # Track processed IDs to avoid duplicates
    
    while True:  # Changed to True to continue processing
        # Prepare batch of IDs
        ids = list(range(current_id, current_id + BATCH_SIZE))
        print(f"Fetching data for IDs {ids[0]} to {ids[-1]}")
        
        try:
            # Construct URL with proper query parameters
            params = [('ids', str(id_val)) for id_val in ids]
            response = requests.get(API_URL, params=params)
            
            # Add debug logging only for errors
            if response.status_code != 200:
                print(f"Error: API returned {response.status_code}")
                print(f"Response: {response.text}")
                break  # Exit the loop on API error
                
            response.raise_for_status()
            data = response.json()
            
            if not data:
                print("No data received from API")
                break
                
            # Process each result
            new_results = 0
            if isinstance(data, list):
                for idx, result in enumerate(data):
                    id_val = ids[idx]  # Get the ID from our original list
                    if result is None:
                        print(f"No data for ID {id_val}")
                        continue
                        
                    current_time = datetime.now(UTC).isoformat()
                    
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
                            print(f"Skipping duplicate record for ID {id_val} at {current_time}")
                            continue
                    except Exception as e:
                        print(f"Error checking for existing record for ID {id_val}: {e}")
                        continue
                    
                    # Try to get previous record to check if this is the first time it's finalized
                    try:
                        previous_records = table.query(
                            KeyConditionExpression='withdrawal_id = :id',
                            ExpressionAttributeValues={':id': id_val},
                            ScanIndexForward=False,  # Get most recent first
                            Limit=1
                        )
                        
                        was_previously_finalized = False
                        if previous_records['Items']:
                            was_previously_finalized = previous_records['Items'][0]['status'] == 'finalized'
                    except Exception as e:
                        print(f"Error checking previous status for ID {id_val}: {e}")
                        was_previously_finalized = False
                    
                    # Safely get nested values with defaults
                    request_info = result.get('requestInfo', {}) or {}  # Handle null requestInfo
                    withdrawal_data = {
                        'withdrawal_id': id_val,
                        'timestamp': current_time,
                        'finalization_in': request_info.get('finalizationIn'),
                        'finalization_at': request_info.get('finalizationAt'),
                        'type': request_info.get('type'),
                        'status': result.get('status'),
                        'next_calculation_at': result.get('nextCalculationAt')
                    }
                    
                    # Add first_finalized_at if this is the first time we see it finalized
                    if result.get('status') == 'finalized' and not was_previously_finalized:
                        withdrawal_data['first_finalized_at'] = current_time
                    
                    all_results.append(withdrawal_data)
                    new_results += 1
                    processed_ids.add(id_val)
                    
                    if result.get('status') != 'finalized':
                        non_finalized_found = True
                        if lowest_non_finalized_id is None or id_val < lowest_non_finalized_id:
                            lowest_non_finalized_id = id_val
            
            print(f"Found {new_results} valid results in this batch")
            
            # Move to the next batch
            current_id += BATCH_SIZE
            time.sleep(0.5)  # Add a small delay to avoid rate limiting
                
        except requests.exceptions.RequestException as e:
            print(f"Network error while fetching data: {e}")
            break
        except json.JSONDecodeError as e:
            print(f"Error decoding API response: {e}")
            print(f"Raw response: {response.text}")
            break
        except Exception as e:
            print(f"Unexpected error while fetching data: {e}")
            print(f"Response status code: {response.status_code}")
            print(f"Response text: {response.text}")
            break
    
    print(f"Total results collected: {len(all_results)}")
    if lowest_non_finalized_id:
        print(f"Lowest non-finalized ID found: {lowest_non_finalized_id}")
    
    return all_results, lowest_non_finalized_id

def store_data_in_dynamodb(data_items, table):
    """Store data items in DynamoDB"""
    if not data_items:
        print("No new data to store")
        return
        
    print(f"Storing {len(data_items)} new records in DynamoDB...")
    with table.batch_writer() as batch:
        for item in data_items:
            batch.put_item(Item=item)
    print(f"Successfully stored {len(data_items)} records")

def main():
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
            print(f"Next run will start from ID: {lowest_non_finalized_id}")
        else:
            # If all requests are finalized, move to the next batch
            next_start_id = start_id + BATCH_SIZE
            save_last_processed_id(next_start_id)
            print(f"All requests in this batch are finalized. Next run will start from ID: {next_start_id}")

if __name__ == "__main__":
    main()