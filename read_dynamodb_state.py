import os
import json
import boto3
from datetime import datetime
from dotenv import load_dotenv
from collections import defaultdict

# Load environment variables from .env file
load_dotenv()

# Configuration
TABLE_NAME = "lido_withdrawal_requests"
REGION_NAME = os.getenv('AWS_REGION', 'us-east-1')

# Initialize DynamoDB client
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name=REGION_NAME
)

def get_table_stats():
    """Get basic statistics about the table"""
    table = dynamodb.Table(TABLE_NAME)
    
    # Get status distribution
    status_counts = defaultdict(int)
    type_counts = defaultdict(int)
    finalized_count = 0
    non_finalized_count = 0
    total_items = 0
    
    # Scan the table for detailed stats
    last_evaluated_key = None
    
    while True:
        scan_kwargs = {
            'Limit': 1000  # Maximum allowed by DynamoDB
        }
        if last_evaluated_key:
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key
            
        response = table.scan(**scan_kwargs)
        
        total_items += len(response['Items'])
        for item in response['Items']:
            status = item.get('status')
            request_type = item.get('type')
            
            if status:
                status_counts[status] += 1
                if status == 'finalized':
                    finalized_count += 1
                else:
                    non_finalized_count += 1
                    
            if request_type:
                type_counts[request_type] += 1
        
        # Check if there are more pages
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
    
    return {
        'total_items': total_items,
        'status_distribution': dict(status_counts),
        'type_distribution': dict(type_counts),
        'finalized_count': finalized_count,
        'non_finalized_count': non_finalized_count
    }

def get_latest_entries(limit=10):
    """Get the most recent entries from the table"""
    table = dynamodb.Table(TABLE_NAME)
    
    # Get all entries and sort by timestamp
    all_entries = []
    last_evaluated_key = None
    
    while True:
        scan_kwargs = {
            'Limit': 1000  # Maximum allowed by DynamoDB
        }
        if last_evaluated_key:
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key
            
        response = table.scan(**scan_kwargs)
        
        for item in response['Items']:
            entry = {
                'withdrawal_id': item.get('withdrawal_id'),
                'timestamp': item.get('timestamp'),
                'status': item.get('status'),
                'type': item.get('type'),
                'finalization_in': item.get('finalization_in'),
                'finalization_at': item.get('finalization_at'),
                'next_calculation_at': item.get('nextCalculationAt'),
                'first_finalized_at': item.get('first_finalized_at')
            }
            all_entries.append(entry)
        
        # Check if there are more pages
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
    
    print(f"Total entries found: {len(all_entries)}")
    
    # Sort entries by timestamp in descending order (most recent first)
    all_entries.sort(key=lambda x: x['timestamp'], reverse=True)
    return all_entries[:limit]

def get_withdrawal_history(withdrawal_id):
    """Get the complete history of a specific withdrawal request"""
    table = dynamodb.Table(TABLE_NAME)
    all_items = []
    last_evaluated_key = None
    
    while True:
        query_kwargs = {
            'KeyConditionExpression': 'withdrawal_id = :id',
            'ExpressionAttributeValues': {':id': withdrawal_id},
            'ScanIndexForward': True,  # Get oldest first
            'Limit': 1000  # Maximum allowed by DynamoDB
        }
        if last_evaluated_key:
            query_kwargs['ExclusiveStartKey'] = last_evaluated_key
            
        response = table.query(**query_kwargs)
        all_items.extend(response['Items'])
        
        # Check if there are more pages
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
    
    return all_items

def get_highest_calculated_request():
    """Get the highest request ID with status 'calculated' at the latest timestamp"""
    table = dynamodb.Table(TABLE_NAME)
    calculated_entries = []
    last_evaluated_key = None
    
    while True:
        scan_kwargs = {
            'FilterExpression': '#status = :status',
            'ExpressionAttributeNames': {
                '#status': 'status'
            },
            'ExpressionAttributeValues': {
                ':status': 'calculated'
            },
            'Limit': 1000
        }
        if last_evaluated_key:
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key
            
        response = table.scan(**scan_kwargs)
        calculated_entries.extend(response['Items'])
        
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
    
    if not calculated_entries:
        return None
        
    # Sort by timestamp (descending) to get the latest entries
    calculated_entries.sort(key=lambda x: x['timestamp'], reverse=True)
    latest_timestamp = calculated_entries[0]['timestamp']
    
    # Filter entries with the latest timestamp
    latest_entries = [entry for entry in calculated_entries if entry['timestamp'] == latest_timestamp]
    
    # Find the highest request ID among the latest entries
    highest_request = max(latest_entries, key=lambda x: int(x['withdrawal_id']))
    return highest_request

def main():
    print(f"\n=== DynamoDB Table State for {TABLE_NAME} ===\n")
    
    # Get and display table statistics
    stats = get_table_stats()
    print("Table Statistics:")
    print(f"Total Items: {stats['total_items']}")
    print(f"Finalized Requests: {stats['finalized_count']}")
    print(f"Non-Finalized Requests: {stats['non_finalized_count']}")
    
    print("\nStatus Distribution:")
    for status, count in stats['status_distribution'].items():
        print(f"  {status}: {count}")
    
    print("\nRequest Type Distribution:")
    for req_type, count in stats['type_distribution'].items():
        print(f"  {req_type}: {count}")
    
    # Get and display highest calculated request
    highest_calculated = get_highest_calculated_request()
    if highest_calculated:
        print("\nHighest Calculated Request at Latest Timestamp:")
        print(f"Withdrawal ID: {highest_calculated['withdrawal_id']}")
        print(f"Timestamp: {highest_calculated['timestamp']}")
    else:
        print("\nNo calculated requests found")

    # Get and display latest entries
    print("\nLatest Entries (showing history):")
    latest = get_latest_entries(5)
    for entry in latest:
        print(f"\nWithdrawal ID: {entry['withdrawal_id']}")
        print(f"Status: {entry['status']}")
        print(f"Type: {entry['type']}")
        print(f"Timestamp: {entry['timestamp']}")
        if entry['finalization_at']:
            print(f"Finalization At: {entry['finalization_at']}")
        if entry['first_finalized_at']:
            print(f"First Finalized At: {entry['first_finalized_at']}")
        if entry['finalization_in']:
            print(f"Finalization In: {entry['finalization_in']}")
        if entry['next_calculation_at']:
            print(f"Next Calculation At: {entry['next_calculation_at']}")

if __name__ == "__main__":
    main() 