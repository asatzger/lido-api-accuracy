# Lido Withdrawal Tracker

This project tracks Lido unstETH withdrawal requests by querying the Lido API every 15 minutes using GitHub Actions, and stores the data in AWS DynamoDB.

## How It Works

1. The script runs every 15 minutes via GitHub Actions
2. It queries the [Lido API](https://wq-api.lido.fi/v2/request-time) for withdrawal request data
3. The script tracks the lowest non-finalized withdrawal ID to use as the starting point for the next run
4. Data is stored in AWS DynamoDB for persistence

### Important Note About Request IDs

Currently, the API does not provide a way to identify non-existent withdrawal request IDs (see [issue #269](https://github.com/lidofinance/withdrawals-api/issues/269)). As a workaround, you need to manually set an upper bound for request IDs in the code. This can be done by:

1. Checking the latest withdrawal request ID (e.g., using [this Dune query](https://dune.com/queries/4832920))
2. Setting `MAX_REQUEST_ID` in `lido_withdrawal_tracker.py` to a value slightly above the latest known ID

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

- `MAX_REQUEST_ID`: Upper bound for request IDs to query (needs manual updating)
- `BATCH_SIZE`: Number of IDs to query in a single API request (default: 20)
- `TABLE_NAME`: Name of the DynamoDB table
- `REGION_NAME`: AWS region to use
- `max_concurrent_batches`: Number of concurrent API requests (default: 2)
- `max_empty_batches`: Number of consecutive empty responses before stopping (default: 3)

## Rate Limiting

The script includes several measures to respect API rate limits:
- Concurrent batch requests are limited to 2 by default
- 1-second delay between submitting batches
- 2-second delay between batch sets
- Exponential backoff on failed requests