import os
import boto3
from botocore.config import Config
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
TABLE_NAME = "lido_withdrawal_requests_v2"
REGION_NAME = os.getenv('AWS_REGION', 'us-east-1')

# Initialize DynamoDB client with optimized configuration
boto_config = Config(
    region_name=REGION_NAME,
    max_pool_connections=50,
    retries={'max_attempts': 3}
)

# Initialize DynamoDB client
session = boto3.Session(
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name=REGION_NAME
)
dynamodb = session.resource('dynamodb', config=boto_config)
table = dynamodb.Table(TABLE_NAME)

def main():
    try:
        # Get initial count
        response = table.scan(Select='COUNT')
        count = response['Count']
        print(f"Initial count: {count}")
        
        # Handle pagination for large tables
        while 'LastEvaluatedKey' in response:
            print("Getting next page...")
            response = table.scan(
                Select='COUNT',
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            count += response['Count']
            print(f"Running total: {count}")
        
        print(f"\nFinal count: {count} items")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 