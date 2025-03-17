import os
import boto3
import time
from datetime import datetime
from dotenv import load_dotenv
from decimal import Decimal
import json
from botocore.exceptions import ClientError
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Set
import queue
import threading

# Load environment variables
load_dotenv()

# Configuration
OLD_TABLE_NAME = "lido_withdrawal_requests"
NEW_TABLE_NAME = "lido_withdrawal_requests_v2"
REGION_NAME = os.getenv('AWS_REGION', 'us-east-1')

# Optimized configuration
MAX_WORKERS = 8  # Increased from 4
BATCH_SIZE = 25
CHUNK_SIZE = 100  # Increased from 50
MAX_RETRIES = 5
BACKOFF_BASE = 2
MAX_BATCH_WRITE = 25  # DynamoDB limit for batch writes
INITIAL_BACKOFF = 0.1  # Reduced from 1 second

# Initialize DynamoDB client with optimized config
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name=REGION_NAME,
    config=Config(
        max_pool_connections=50,
        connect_timeout=10,
        read_timeout=30,
        retries={'max_attempts': 3}
    )
)

def create_new_table():
    """Create the new table with optimized schema"""
    try:
        table = dynamodb.Table(NEW_TABLE_NAME)
        table.load()
        print(f"Table {NEW_TABLE_NAME} already exists")
        return table
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            try:
                table = dynamodb.create_table(
                    TableName=NEW_TABLE_NAME,
                    KeySchema=[
                        {'AttributeName': 'withdrawal_id', 'KeyType': 'HASH'},
                        {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'withdrawal_id', 'AttributeType': 'N'},
                        {'AttributeName': 'timestamp', 'AttributeType': 'S'},
                        {'AttributeName': 'status', 'AttributeType': 'S'},
                        {'AttributeName': 'finalization_time', 'AttributeType': 'N'}
                    ],
                    GlobalSecondaryIndexes=[
                        {
                            'IndexName': 'status-finalization-index',
                            'KeySchema': [
                                {'AttributeName': 'status', 'KeyType': 'HASH'},
                                {'AttributeName': 'finalization_time', 'KeyType': 'RANGE'}
                            ],
                            'Projection': {
                                'ProjectionType': 'ALL'
                            },
                            'ProvisionedThroughput': {
                                'ReadCapacityUnits': 10,
                                'WriteCapacityUnits': 10
                            }
                        }
                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 10,
                        'WriteCapacityUnits': 10
                    }
                )
                table.meta.client.get_waiter('table_exists').wait(TableName=NEW_TABLE_NAME)
                print(f"Created new table {NEW_TABLE_NAME}")
                return table
            except Exception as e:
                print(f"Error creating table: {e}")
                raise
        else:
            print(f"Error checking table existence: {e}")
            raise

def scan_segment(table_name: str, segment: int, total_segments: int, result_queue: queue.Queue):
    """Scan a single segment of the table with retries and backoff"""
    table = dynamodb.Table(table_name)
    items_processed = 0
    retry_count = 0
    
    try:
        scan_kwargs = {
            'TableName': table_name,
            'Segment': segment,
            'TotalSegments': total_segments,
            'ReturnConsumedCapacity': 'TOTAL',
            'ConsistentRead': True
        }
        
        while True:
            try:
                response = table.scan(**scan_kwargs)
                items = response.get('Items', [])
                
                if items:
                    # Sort items by withdrawal_id and timestamp to ensure consistent ordering
                    items.sort(key=lambda x: (int(x['withdrawal_id']), x.get('timestamp', '')))
                    result_queue.put((items, segment))
                    items_processed += len(items)
                    
                    if items_processed % 1000 == 0:
                        print(f"Segment {segment}: Processed {items_processed} items")
                
                if 'LastEvaluatedKey' not in response:
                    break
                    
                scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
                
                # Add delay between scans
                time.sleep(0.1)
                
            except ClientError as e:
                if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                    retry_count += 1
                    if retry_count > MAX_RETRIES:
                        print(f"Max retries exceeded for segment {segment}")
                        break
                    backoff = INITIAL_BACKOFF * (BACKOFF_BASE ** min(retry_count, 5))
                    print(f"Throughput exceeded in segment {segment}, backing off for {backoff:.1f}s (attempt {retry_count})...")
                    time.sleep(backoff)
                    continue
                raise
                
    except Exception as e:
        print(f"Error in segment {segment}: {e}")
    finally:
        result_queue.put((None, segment))  # Signal completion

def parallel_scan(table_name: str, segments: int = MAX_WORKERS) -> queue.Queue:
    """Perform parallel scan of the table"""
    result_queue = queue.Queue()
    threads = []
    
    for segment in range(segments):
        thread = threading.Thread(
            target=scan_segment,
            args=(table_name, segment, segments, result_queue)
        )
        thread.start()
        threads.append(thread)
        time.sleep(1)  # Stagger thread starts
    
    for thread in threads:
        thread.join()
    
    return result_queue

def batch_write_with_retries(table, items: List[Dict], max_retries: int = MAX_RETRIES):
    """Write items in optimized batches with retries"""
    if not items:
        return 0
    
    items_written = 0
    
    # Sort items by withdrawal_id and timestamp for consistent ordering
    items.sort(key=lambda x: (int(x['withdrawal_id']), x.get('timestamp', '')))
    
    # Process in batches of 25 (DynamoDB limit)
    for i in range(0, len(items), 25):
        batch = items[i:i + 25]
        retry_count = 0
        success = False
        
        while retry_count < max_retries and not success:
            try:
                with table.batch_writer() as writer:
                    for item in batch:
                        writer.put_item(Item=item)
                items_written += len(batch)
                success = True
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                retry_count += 1
                
                if error_code == 'ProvisionedThroughputExceededException':
                    if retry_count == max_retries:
                        print(f"Max retries exceeded for batch")
                        break
                    backoff = INITIAL_BACKOFF * (BACKOFF_BASE ** min(retry_count, 5))
                    time.sleep(backoff)
                    continue
                else:
                    print(f"Error writing batch: {error_code}")
                    break
        
        # Small delay between batches to avoid throttling
        time.sleep(0.05)
    
    return items_written

def process_items(items: List[Dict]) -> List[Dict]:
    """Process items with minimal logging"""
    processed_items = []
    finalization_data = {}  # Track finalization data by withdrawal_id
    
    # First pass: identify first finalization time for each withdrawal_id
    for item in sorted(items, key=lambda x: (int(x['withdrawal_id']), x.get('timestamp', ''))):
        wid = int(item['withdrawal_id'])
        timestamp = item.get('timestamp')
        if not timestamp:
            continue
            
        # Track the first finalized status timestamp
        if item.get('status') == 'finalized' and wid not in finalization_data:
            finalization_data[wid] = {
                'actual_finalization_at': timestamp,
                'finalization_time': int(time.time())  # Current timestamp for GSI
            }
    
    # Second pass: process all items
    seen_keys = set()  # Track unique primary keys
    for item in sorted(items, key=lambda x: (int(x['withdrawal_id']), x.get('timestamp', ''))):
        try:
            withdrawal_id = int(item['withdrawal_id'])
            timestamp = item.get('timestamp')
            
            if not timestamp:
                continue
                
            # Create unique key to prevent duplicates
            key = (withdrawal_id, timestamp)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            
            # Get finalization data if available
            finalization_info = finalization_data.get(withdrawal_id)
            
            new_item = {
                'withdrawal_id': withdrawal_id,
                'timestamp': timestamp,
                'status': item.get('status', 'unknown'),
                'type': item.get('type', 'unknown'),
                'finalization_in': item.get('finalization_in'),
                'estimated_finalization_at': item.get('finalization_at'),  # Store original estimate
                'next_calculation_at': item.get('next_calculation_at')
            }
            
            # Add actual finalization data if available
            if finalization_info:
                new_item['actual_finalization_at'] = finalization_info['actual_finalization_at']
                new_item['finalization_time'] = finalization_info['finalization_time']
            
            processed_items.append(new_item)
            
        except Exception as e:
            print(f"Error processing withdrawal_id {item.get('withdrawal_id')}: {str(e)}")
            continue
    
    return processed_items

def migrate_data():
    """Migrate data with improved efficiency"""
    old_table = dynamodb.Table(OLD_TABLE_NAME)
    new_table = dynamodb.Table(NEW_TABLE_NAME)
    total_migrated = 0
    processed_keys = set()
    
    print("\nStarting migration...")
    result_queue = parallel_scan(OLD_TABLE_NAME)
    segments_completed = set()
    
    # Create a thread pool for parallel processing of chunks
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        
        while not result_queue.empty():
            items, segment = result_queue.get()
            if items is not None:
                if segment in segments_completed:
                    continue
                segments_completed.add(segment)
                
                # Filter duplicates
                unique_items = []
                for item in items:
                    key = (int(item['withdrawal_id']), item.get('timestamp', ''))
                    if key not in processed_keys:
                        processed_keys.add(key)
                        unique_items.append(item)
                
                if not unique_items:
                    continue
                
                # Process in larger chunks with parallel execution
                for i in range(0, len(unique_items), CHUNK_SIZE):
                    chunk = unique_items[i:i + CHUNK_SIZE]
                    future = executor.submit(process_and_write_chunk, new_table, chunk, segment)
                    futures.append(future)
                
                # Process completed futures periodically
                completed = [f for f in futures if f.done()]
                for future in completed:
                    try:
                        items_written = future.result()
                        total_migrated += items_written
                    except Exception as e:
                        print(f"Error processing chunk: {str(e)}")
                futures = [f for f in futures if not f.done()]
        
        # Wait for remaining futures
        for future in futures:
            try:
                items_written = future.result()
                total_migrated += items_written
            except Exception as e:
                print(f"Error processing chunk: {str(e)}")
    
    print(f"\nMigration complete. Total items migrated: {total_migrated}")
    print(f"Processed segments: {sorted(list(segments_completed))}")
    return total_migrated

def process_and_write_chunk(table, chunk, segment):
    """Process and write a chunk of items"""
    processed_items = process_items(chunk)
    if processed_items:
        items_written = batch_write_with_retries(table, processed_items)
        print(f"Migrated {items_written} items from segment {segment}")
        return items_written
    return 0

def verify_migration():
    """Verify that the migration was successful"""
    old_table = dynamodb.Table(OLD_TABLE_NAME)
    new_table = dynamodb.Table(NEW_TABLE_NAME)
    
    old_count = old_table.scan(Select='COUNT')['Count']
    new_count = new_table.scan(Select='COUNT')['Count']
    
    print(f"\nMigration Verification:")
    print(f"Old table count: {old_count}")
    print(f"New table count: {new_count}")
    
    if new_count >= old_count:
        print("Migration verification successful!")
        return True
    else:
        print("Migration verification failed - new table has fewer items than old table")
        return False

def update_table_capacity(table_name: str, read_capacity: int = 50, write_capacity: int = 50):
    """Update the provisioned throughput of a table"""
    try:
        table = dynamodb.Table(table_name)
        
        # First check current capacity
        response = table.meta.client.describe_table(TableName=table_name)
        current_rcus = response['Table']['ProvisionedThroughput']['ReadCapacityUnits']
        current_wcus = response['Table']['ProvisionedThroughput']['WriteCapacityUnits']
        
        if current_rcus == read_capacity and current_wcus == write_capacity:
            print(f"Table {table_name} already at target capacity ({read_capacity} RCU, {write_capacity} WCU)")
            return True
            
        print(f"Updating {table_name} capacity from {current_rcus}/{current_wcus} to {read_capacity}/{write_capacity} RCU/WCU...")
        
        table.update(
            ProvisionedThroughput={
                'ReadCapacityUnits': read_capacity,
                'WriteCapacityUnits': write_capacity
            }
        )
        
        # Wait for the table to be active
        while True:
            try:
                response = table.meta.client.describe_table(TableName=table_name)
                status = response['Table']['TableStatus']
                if status == 'ACTIVE':
                    current_rcus = response['Table']['ProvisionedThroughput']['ReadCapacityUnits']
                    current_wcus = response['Table']['ProvisionedThroughput']['WriteCapacityUnits']
                    if current_rcus == read_capacity and current_wcus == write_capacity:
                        print(f"Table {table_name} is now ready with {read_capacity} RCU, {write_capacity} WCU")
                        break
                print(f"Waiting for capacity update... Current status: {status}")
                time.sleep(5)
            except Exception as e:
                print(f"Error checking table status: {e}")
                time.sleep(5)
        
        # Update GSI capacities if they exist
        try:
            table_info = table.meta.client.describe_table(TableName=table_name)
            if 'GlobalSecondaryIndexes' in table_info['Table']:
                for gsi in table_info['Table']['GlobalSecondaryIndexes']:
                    index_name = gsi['IndexName']
                    current_gsi_rcus = gsi['ProvisionedThroughput']['ReadCapacityUnits']
                    current_gsi_wcus = gsi['ProvisionedThroughput']['WriteCapacityUnits']
                    
                    if current_gsi_rcus == read_capacity and current_gsi_wcus == write_capacity:
                        print(f"GSI {index_name} already at target capacity")
                        continue
                        
                    print(f"Updating GSI {index_name} capacity...")
                    table.update(
                        GlobalSecondaryIndexUpdates=[
                            {
                                'Update': {
                                    'IndexName': index_name,
                                    'ProvisionedThroughput': {
                                        'ReadCapacityUnits': read_capacity,
                                        'WriteCapacityUnits': write_capacity
                                    }
                                }
                            }
                        ]
                    )
        except Exception as e:
            print(f"Warning: Error updating GSI capacities: {e}")
        
        return True
    except Exception as e:
        print(f"Error updating table capacity: {e}")
        return False

def main():
    try:
        # Create new table or get existing one
        print("Setting up target table...")
        table = create_new_table()
        
        # Print initial counts
        old_table = dynamodb.Table(OLD_TABLE_NAME)
        new_table = dynamodb.Table(NEW_TABLE_NAME)
        old_count = old_table.scan(Select='COUNT')['Count']
        new_count = new_table.scan(Select='COUNT')['Count']
        print(f"\nInitial counts:")
        print(f"Old table ({OLD_TABLE_NAME}): {old_count}")
        print(f"New table ({NEW_TABLE_NAME}): {new_count}")
        
        # Ask for confirmation
        response = input("\nDo you want to proceed with data migration? This will overwrite existing data. (yes/no): ")
        if response.lower() != 'yes':
            print("Migration cancelled by user.")
            return
            
        # Update table capacities only if needed
        print("\nChecking table capacities...")
        old_needs_update = update_table_capacity(OLD_TABLE_NAME, 200, 200)
        new_needs_update = update_table_capacity(NEW_TABLE_NAME, 200, 200)
            
        # Only wait if capacities were actually updated
        if old_needs_update or new_needs_update:
            print("Waiting for capacity updates to propagate...")
            time.sleep(60)
        else:
            print("Tables already at target capacity, proceeding with migration...")
        
        # Migrate data
        migrate_data()
        
        # Reset table capacities
        print("\nResetting table capacities...")
        update_table_capacity(OLD_TABLE_NAME, 5, 5)
        update_table_capacity(NEW_TABLE_NAME, 5, 5)
        
        # Verify migration
        if verify_migration():
            print("\nMigration completed successfully!")
            print(f"New table name: {NEW_TABLE_NAME}")
            print("\nNext steps:")
            print("1. Update your application code to use the new table")
            print("2. Once verified, you can delete the old table")
        else:
            print("\nMigration completed with verification warnings.")
            print("Please check the data manually before proceeding.")
    
    except Exception as e:
        print(f"\nError during migration: {e}")
        print("Migration failed. Please check the logs and try again.")

if __name__ == "__main__":
    main() 