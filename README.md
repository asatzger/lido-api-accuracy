# Lido Withdrawal Tracker

This project tracks Lido unstETH withdrawal requests by querying the Lido API every 15 minutes using GitHub Actions, and stores the data in AWS DynamoDB. It also includes a daily analysis workflow that generates visualizations of withdrawal estimate accuracy.

## How It Works

### Withdrawal Tracking

1. The tracking script runs every 15 minutes via GitHub Actions
2. It queries the [Lido API](https://wq-api.lido.fi/v2/request-time) for withdrawal request data
3. The script tracks the lowest non-finalized withdrawal ID to use as the starting point for the next run
4. Data is stored in AWS DynamoDB for persistence

### Data Analysis

1. A daily analysis workflow runs once per day
2. It analyzes the accuracy of Lido's withdrawal time estimates
3. It generates visualizations and statistics
4. The analysis is published to GitHub Pages

### Important Note About Request IDs

Currently, the API does not provide a way to identify non-existent withdrawal request IDs (see [issue #269](https://github.com/lidofinance/withdrawals-api/issues/269)). As a workaround, you need to manually set an upper bound for request IDs in the code. This can be done by:

1. Checking the latest withdrawal request ID (e.g., using [this Dune query](https://dune.com/queries/4832920))
2. Setting `MAX_REQUEST_ID` in `lido_withdrawal_tracker.py` to a value slightly above the latest known ID

Current `MAX_REQUEST_ID` is set to 200000.

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
   - `AWS_REGION`: The AWS region to use (defaults to 'us-east-1')
3. Enable GitHub Pages for the repository (Settings > Pages)

## Workflows

### Withdrawal Tracker

The withdrawal tracker workflow runs every 15 minutes to collect data from the Lido API. You can also trigger it manually from the Actions tab in your repository.

```yaml
name: Lido Withdrawal Tracker
on:
  schedule:
    - cron: '*/15 * * * *'
  workflow_dispatch:
```

### Daily Analysis

The daily analysis workflow runs once per day to analyze the withdrawal data and publish results to GitHub Pages.

```yaml
name: Daily Withdrawal Analysis
on:
  schedule:
    - cron: '0 6 * * *'
  workflow_dispatch:
```

## Data Structure

The DynamoDB table (`lido_withdrawal_requests`) has the following schema:

- Primary Key: 
  - `status` (Partition key)
  - `withdrawal_id` (Sort key)
- Global Secondary Indexes:
  - `finalization-index`: Indexes by status and finalization time
  - `timestamp-index`: Indexes by withdrawal ID and timestamp
- Attributes:
  - `withdrawal_id`: The withdrawal request ID
  - `timestamp`: When the data was collected
  - `finalization_in`: Number of Ethereum blocks until finalization
  - `finalization_at`: Timestamp when the request will be finalized
  - `finalization_time`: Unix timestamp of finalization (if available)
  - `type`: The request type
  - `status`: Current status of the request
  - `next_calculation_at`: When the next calculation will occur

## Customization

You can adjust the following parameters in `lido_withdrawal_tracker.py`:

- `MAX_REQUEST_ID`: Upper bound for request IDs to query (currently 200000)
- `BATCH_SIZE`: Number of IDs to query in a single API request (default: 20)
- `MAX_CONCURRENT_BATCHES`: Number of concurrent API requests (currently 5)
- `TABLE_NAME`: Name of the DynamoDB table
- `REGION_NAME`: AWS region to use

## Rate Limiting

The script includes several measures to respect API rate limits:
- Concurrent batch requests are limited to 5
- 1-second delay between submitting batches
- 2-second delay between batch sets
- Exponential backoff on failed requests
- Early termination when withdrawal requests with 'calculating' status are found

## Analysis Results

The daily analysis produces a static HTML page with visualizations and statistics about the accuracy of Lido's withdrawal time estimates. This page is automatically published to GitHub Pages.

Key metrics include:
- Error distribution
- Accuracy by withdrawal type
- Cumulative accuracy by days difference
- Daily processing patterns

You can view the analysis results at: `https://<your-github-username>.github.io/<repository-name>/`