# Lido Withdrawal Tracker

This project tracks Lido unstETH withdrawal requests by querying the Lido API every 15 minutes using GitHub Actions, and stores the data in AWS DynamoDB.

## How It Works

1. The script runs every 15 minutes via GitHub Actions
2. It queries the [Lido API](https://wq-api.lido.fi/v2/request-time) for withdrawal request data
3. The script tracks the lowest non-finalized withdrawal ID to use as the starting point for the next run
4. Data is stored in AWS DynamoDB for persistence

## Setup Instructions

### AWS Setup

1. Create an AWS account if you don't have one
2. Create an S3 bucket to store metadata
3. Create an IAM user with permissions for DynamoDB and S3
4. Generate an access key and secret key for the IAM user

### GitHub Repository Setup

1. Fork or clone this repository
2. Add the following secrets to your GitHub repository:
   - `AWS_ACCESS_KEY_ID`: Your AWS access key
   - `AWS_SECRET_ACCESS_KEY`: Your AWS secret key
   - `S3_BUCKET_NAME`: The name of your S3 bucket

The GitHub Action will run automatically every 15 minutes, or you can trigger it manually from the Actions tab in your repository.

## Data Structure

The DynamoDB table (`lido_withdrawal_requests`) has the following columns:

- `withdrawal_id` (Primary Key): The withdrawal request ID
- `timestamp`: When the data was collected
- `finalization_in`: Number of Ethereum blocks until finalization
- `finalization_at`: Timestamp when the request will be finalized
- `type`: The request type
- `status`: Current status of the request
- `next_calculation_at`: When the next calculation will occur

## Customization

You can adjust the following parameters in `lido_withdrawal_tracker.py`:

- `BATCH_SIZE`: Number of IDs to query in a single API request
- `TABLE_NAME`: Name of the DynamoDB table
- `REGION_NAME`: AWS region to use