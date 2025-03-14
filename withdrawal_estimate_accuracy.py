import os
import json
import boto3
import pandas as pd
import numpy as np
import altair as alt
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from collections import defaultdict
import pytz
import base64
from io import BytesIO
import argparse

# Custom JSON encoder that can handle date and datetime objects
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

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

# Set max rows for Altair to prevent warnings with large datasets
alt.data_transformers.disable_max_rows()

def parse_timestamp(timestamp_str):
    """Parse timestamp string to datetime object"""
    if not timestamp_str:
        return None
    
    # Handle both ISO format and Unix timestamp (milliseconds)
    try:
        if isinstance(timestamp_str, str):
            # Handle Z suffix for UTC time
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        elif isinstance(timestamp_str, (int, float)):
            # Handle millisecond timestamps
            return datetime.fromtimestamp(timestamp_str / 1000, tz=pytz.UTC)
    except Exception as e:
        print(f"Error parsing timestamp {timestamp_str}: {e}")
        return None
    
    return None

def print_sample_data_structure(item):
    """Debug function to print the structure of data"""
    print("\nSample data structure:")
    for key, value in item.items():
        print(f"{key}: {type(value)} = {value}")

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

def get_finalized_withdrawal_data(test_mode=False, test_limit=10):
    """Get all finalized withdrawal requests with their history"""
    table = dynamodb.Table(TABLE_NAME)
    finalized_entries = []
    last_evaluated_key = None
    
    print("Scanning for finalized withdrawals...")
    
    while True:
        scan_kwargs = {
            'FilterExpression': '#status = :status',
            'ExpressionAttributeNames': {
                '#status': 'status'
            },
            'ExpressionAttributeValues': {
                ':status': 'finalized'
            },
            'Limit': 1000
        }
        if last_evaluated_key:
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key
            
        response = table.scan(**scan_kwargs)
        finalized_entries.extend(response['Items'])
        
        print(f"Found {len(finalized_entries)} finalized entries so far...")
        
        # In test mode, stop after reaching the limit
        if test_mode and len(finalized_entries) >= test_limit:
            finalized_entries = finalized_entries[:test_limit]
            print(f"Test mode: limiting to {test_limit} withdrawals")
            break
        
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
    
    print(f"Total finalized entries found: {len(finalized_entries)}")
    
    # Print debug information for first entry to see structure
    if finalized_entries:
        print_sample_data_structure(finalized_entries[0])
    
    return finalized_entries

def analyze_estimate_accuracy(test_mode=False, test_limit=10):
    """
    Analyze the accuracy of withdrawal time estimates against 
    actual finalization times.
    """
    finalized_withdrawals = get_finalized_withdrawal_data(test_mode, test_limit)
    
    # Filter withdrawals that have both finalization_at and first_finalized_at
    valid_withdrawals = []
    
    print("Processing withdrawal histories...")
    for withdrawal in finalized_withdrawals:
        withdrawal_id = withdrawal.get('withdrawal_id')
        if not withdrawal_id:
            continue
            
        # Get complete history for this withdrawal
        history = get_withdrawal_history(withdrawal_id)
        
        if not history:
            print(f"No history found for withdrawal {withdrawal_id}")
            continue
            
        # Track estimation changes over time
        estimates = []
        actual_finalized_at = None
        
        # Find the first record with finalized status to get actual finalization time
        finalized_items = [item for item in history if item.get('status') == 'finalized']
        if finalized_items:
            # Sort by timestamp to get the earliest finalized record
            finalized_items.sort(key=lambda x: x.get('timestamp', 0))
            first_finalized_item = finalized_items[0]
            
            # Try different possible field names for the finalization timestamp
            timestamp_fields = ['first_finalized_at', 'timestamp', 'finalization_at', 'finalizationAt']
            for field in timestamp_fields:
                if field in first_finalized_item:
                    actual_finalized_at = parse_timestamp(first_finalized_item[field])
                    if actual_finalized_at:
                        print(f"Found actual finalization time for {withdrawal_id} from field {field}")
                        break
        
        # Find all records with estimates
        for item in history:
            timestamp = item.get('timestamp')
            
            # Try different possible field names for estimation time
            estimate_fields = ['finalization_at', 'finalizationAt', 'finalization_in', 'finalizationIn', 'nextCalculationAt']
            finalization_estimate = None
            
            for field in estimate_fields:
                if field in item:
                    # For fields that store 'time until finalization' instead of absolute time
                    if field in ['finalization_in', 'finalizationIn']:
                        time_until = item[field]
                        if isinstance(time_until, (int, float)):
                            item_timestamp = parse_timestamp(timestamp)
                            if item_timestamp:
                                # Convert seconds/milliseconds to timedelta
                                if time_until > 1000000:  # Likely milliseconds
                                    finalization_estimate = item_timestamp + timedelta(milliseconds=time_until)
                                else:  # Likely seconds
                                    finalization_estimate = item_timestamp + timedelta(seconds=time_until)
                    else:
                        finalization_estimate = parse_timestamp(item[field])
                    
                    if finalization_estimate:
                        break
            
            if timestamp and finalization_estimate:
                estimates.append({
                    'timestamp': parse_timestamp(timestamp),
                    'finalization_at': finalization_estimate
                })
        
        # Only include withdrawals with both estimates and actual finalization
        if estimates and actual_finalized_at:
            valid_withdrawals.append({
                'withdrawal_id': withdrawal_id,
                'estimates': estimates,
                'actual_finalized_at': actual_finalized_at,
                'type': withdrawal.get('type', 'unknown')
            })
            print(f"Added withdrawal {withdrawal_id} with {len(estimates)} estimates")
    
    print(f"Found {len(valid_withdrawals)} withdrawals with valid estimation and finalization data")
    
    # Prepare data for analysis
    analysis_data = []
    
    for withdrawal in valid_withdrawals:
        actual_time = withdrawal['actual_finalized_at']
        
        for estimate in withdrawal['estimates']:
            if estimate['finalization_at'] and estimate['timestamp']:
                time_of_estimate = estimate['timestamp']
                estimated_time = estimate['finalization_at']
                
                # Calculate error in minutes
                error_minutes = (actual_time - estimated_time).total_seconds() / 60
                
                # Calculate how far in advance the estimate was made (in hours)
                hours_in_advance = (estimated_time - time_of_estimate).total_seconds() / 3600
                
                # Only include reasonable estimates (filter extreme outliers)
                if abs(error_minutes) < 60 * 24 * 7:  # Within a week
                    analysis_data.append({
                        'withdrawal_id': withdrawal['withdrawal_id'],
                        'withdrawal_type': withdrawal['type'],
                        'time_of_estimate': time_of_estimate,
                        'estimated_time': estimated_time,
                        'actual_time': actual_time,
                        'error_minutes': error_minutes,
                        'error_hours': error_minutes / 60,
                        'hours_in_advance': hours_in_advance,
                        'estimate_date': time_of_estimate.date(),
                        'absolute_error_hours': abs(error_minutes / 60)
                    })
    
    # Convert to DataFrame for easier analysis
    df = pd.DataFrame(analysis_data)
    
    if df.empty:
        print("No valid data for analysis")
        return None
    
    print(f"Analysis dataset created with {len(df)} estimation points")
    return df

def analyze_time_of_day(df):
    """Analyze when withdrawals are typically processed during the day"""
    # Extract hour from actual finalization time
    df['finalization_hour'] = df['actual_time'].dt.hour
    
    # Group by hour and calculate statistics
    hourly_stats = df.groupby('finalization_hour').agg({
        'withdrawal_id': 'count',
        'error_hours': ['mean', 'median', 'std']
    }).reset_index()
    
    hourly_stats.columns = ['hour', 'count', 'mean_error', 'median_error', 'std_error']
    
    return hourly_stats

def analyze_batch_processing(df):
    """Analyze batch processing patterns"""
    # Group withdrawals by date and hour of finalization
    df['finalization_date'] = df['actual_time'].dt.date
    df['finalization_hour'] = df['actual_time'].dt.hour
    
    # Count withdrawals per hour for each date
    batch_counts = df.groupby(['finalization_date', 'finalization_hour']).agg({
        'withdrawal_id': 'count',
        'error_hours': ['mean', 'median']
    }).reset_index()
    
    batch_counts.columns = ['date', 'hour', 'count', 'mean_error', 'median_error']
    
    # Calculate batch size statistics
    batch_stats = batch_counts.groupby('hour').agg({
        'count': ['mean', 'median', 'std', 'min', 'max']
    }).reset_index()
    
    batch_stats.columns = ['hour', 'mean_batch_size', 'median_batch_size', 'std_batch_size', 'min_batch_size', 'max_batch_size']
    
    return batch_counts, batch_stats

def analyze_accuracy_over_time(df):
    """Analyze how estimate accuracy changes as finalization time approaches"""
    # Calculate time until finalization for each estimate
    df['hours_until_finalization'] = (df['actual_time'] - df['time_of_estimate']).dt.total_seconds() / 3600
    
    # Group by time until finalization (in 6-hour intervals)
    df['time_until_group'] = pd.cut(df['hours_until_finalization'],
                                   bins=[0, 6, 12, 24, 48, 72, float('inf')],
                                   labels=['0-6h', '6-12h', '12-24h', '24-48h', '48-72h', '72h+'])
    
    # Calculate statistics for each time group
    accuracy_over_time = df.groupby('time_until_group').agg({
        'error_hours': ['mean', 'median', 'std', 'count'],
        'absolute_error_hours': ['mean', 'median']
    }).reset_index()
    
    accuracy_over_time.columns = ['time_until', 'mean_error', 'median_error', 'std_error', 'count', 
                                'mean_abs_error', 'median_abs_error']
    
    return accuracy_over_time

def analyze_individual_estimates(df):
    """
    Analyze how finalization time estimates change over time for individual withdrawals.
    This function tracks the progression of estimates for each withdrawal_id.
    """
    # Create a subset of data with just the needed columns
    tracking_df = df[['withdrawal_id', 'time_of_estimate', 'estimated_time', 'actual_time']].copy()
    
    # Sort by withdrawal_id and time of estimate
    tracking_df = tracking_df.sort_values(['withdrawal_id', 'time_of_estimate'])
    
    # Get the top 5 withdrawal IDs with most estimates for a more readable chart
    top_withdrawals = (df['withdrawal_id']
                      .value_counts()
                      .head(5)
                      .index
                      .tolist())
    
    # Filter to just these withdrawals
    top_tracking_df = tracking_df[tracking_df['withdrawal_id'].isin(top_withdrawals)]
    
    # Calculate hours until estimated finalization from each estimate point
    top_tracking_df['hours_until_estimated'] = (
        (top_tracking_df['estimated_time'] - top_tracking_df['time_of_estimate'])
        .dt.total_seconds() / 3600
    )
    
    # Calculate hours until actual finalization from each estimate point
    top_tracking_df['hours_until_actual'] = (
        (top_tracking_df['actual_time'] - top_tracking_df['time_of_estimate'])
        .dt.total_seconds() / 3600
    )
    
    return top_tracking_df

def generate_altair_visualizations(df):
    """Generate Altair visualizations from the analysis data"""
    visualizations = {}
    
    # Create a copy of the DataFrame for visualization
    df_viz = df.copy()
    
    # Ensure numeric columns are float
    df_viz['error_hours'] = df_viz['error_hours'].astype(float)
    df_viz['hours_in_advance'] = df_viz['hours_in_advance'].astype(float)
    df_viz['absolute_error_hours'] = df_viz['absolute_error_hours'].astype(float)
    df_viz['withdrawal_type'] = df_viz['withdrawal_type'].astype(str)
    df_viz['withdrawal_id'] = df_viz['withdrawal_id'].astype(str)
    
    # 1. Error distribution histogram
    error_hist = alt.Chart(df_viz).mark_bar().encode(
        alt.X('error_hours:Q', bin=alt.Bin(maxbins=50), title='Error (Actual - Estimated) in Hours'),
        alt.Y('count()', title='Frequency'),
        tooltip=['count()', alt.Tooltip('error_hours:Q', title='Error (hours)')]
    ).properties(
        title='Distribution of Estimate Errors (Hours)',
        width=600,
        height=400
    )
    
    # Add a rule for perfect estimate (Error = 0)
    perfect_line = alt.Chart(pd.DataFrame({'x': [0]})).mark_rule(
        color='red', 
        strokeDash=[6, 4],
        strokeWidth=2
    ).encode(x='x:Q')
    
    error_dist_chart = (error_hist + perfect_line)
    
    visualizations['error_distribution'] = error_dist_chart
    
    # 2. Error vs Estimation Lead Time
    scatter = alt.Chart(df_viz).mark_circle(opacity=0.7).encode(
        x=alt.X('hours_in_advance:Q', title='Hours Between Estimate and Estimated Completion'),
        y=alt.Y('error_hours:Q', title='Error (Actual - Estimated) in Hours'),
        color=alt.Color('withdrawal_type:N', title='Withdrawal Type'),
        size=alt.Size('absolute_error_hours:Q', scale=alt.Scale(range=[20, 200]), title='Absolute Error (hours)'),
        tooltip=['withdrawal_id', 'withdrawal_type', 'error_hours:Q', 'hours_in_advance:Q']
    ).properties(
        title='Estimate Error vs. How Far in Advance Estimate Was Made',
        width=700,
        height=400
    )
    
    # Add a rule for perfect estimate (Error = 0)
    zero_line = alt.Chart(pd.DataFrame({'y': [0]})).mark_rule(
        color='red', 
        strokeDash=[6, 4],
        strokeWidth=2
    ).encode(y='y:Q')
    
    error_vs_leadtime_chart = (scatter + zero_line)
    
    visualizations['error_vs_leadtime'] = error_vs_leadtime_chart
    
    # 3. Error distribution by withdrawal type
    boxplot = alt.Chart(df_viz).mark_boxplot().encode(
        x=alt.X('withdrawal_type:N', title='Withdrawal Type'),
        y=alt.Y('error_hours:Q', title='Error (Hours)'),
        color=alt.Color('withdrawal_type:N', title='Withdrawal Type')
    ).properties(
        title='Estimate Errors by Withdrawal Type',
        width=600,
        height=400
    )
    
    visualizations['error_by_type'] = boxplot
    
    # 4. Time of day analysis
    hourly_stats = analyze_time_of_day(df)  # Use original df for analysis
    hourly_stats = hourly_stats.copy()
    hourly_stats['hour'] = hourly_stats['hour'].astype(int)
    hourly_stats['count'] = hourly_stats['count'].astype(int)
    hourly_stats['mean_error'] = hourly_stats['mean_error'].astype(float)
    hourly_stats['median_error'] = hourly_stats['median_error'].astype(float)
    hourly_stats['std_error'] = hourly_stats['std_error'].astype(float)
    
    # Create a heatmap of withdrawal counts by hour
    heatmap = alt.Chart(hourly_stats).mark_rect().encode(
        x=alt.X('hour:O', title='Hour of Day (UTC)'),
        y=alt.Y('count:Q', title='Number of Withdrawals'),
        color=alt.Color('count:Q', title='Count'),
        tooltip=['hour:O', 'count:Q', 'mean_error:Q', 'median_error:Q']
    ).properties(
        title='Withdrawal Processing Time Distribution',
        width=600,
        height=400
    )
    visualizations['time_of_day'] = heatmap
    
    # 5. Batch processing patterns
    batch_counts, batch_stats = analyze_batch_processing(df)  # Use original df for analysis
    batch_counts = batch_counts.copy()
    batch_counts['date'] = batch_counts['date'].astype(str)  # Convert date to string for visualization
    batch_counts['hour'] = batch_counts['hour'].astype(int)
    batch_counts['count'] = batch_counts['count'].astype(int)
    batch_counts['mean_error'] = batch_counts['mean_error'].astype(float)
    batch_counts['median_error'] = batch_counts['median_error'].astype(float)
    
    # Create a line chart showing batch sizes over time
    batch_line = alt.Chart(batch_counts).mark_line().encode(
        x=alt.X('date:T', title='Date'),
        y=alt.Y('count:Q', title='Number of Withdrawals'),
        color=alt.Color('hour:O', title='Hour of Day'),
        tooltip=['date:T', 'hour:O', 'count:Q', 'mean_error:Q']
    ).properties(
        title='Batch Processing Patterns Over Time',
        width=800,
        height=400
    )
    visualizations['batch_patterns'] = batch_line
    
    # 6. Accuracy over time
    accuracy_over_time = analyze_accuracy_over_time(df)  # Use original df for analysis
    accuracy_over_time = accuracy_over_time.copy()
    accuracy_over_time['time_until'] = accuracy_over_time['time_until'].astype(str)
    accuracy_over_time['mean_error'] = accuracy_over_time['mean_error'].astype(float)
    accuracy_over_time['median_error'] = accuracy_over_time['median_error'].astype(float)
    accuracy_over_time['std_error'] = accuracy_over_time['std_error'].astype(float)
    accuracy_over_time['count'] = accuracy_over_time['count'].astype(int)
    accuracy_over_time['mean_abs_error'] = accuracy_over_time['mean_abs_error'].astype(float)
    accuracy_over_time['median_abs_error'] = accuracy_over_time['median_abs_error'].astype(float)
    
    # Create a line chart showing accuracy improvement
    accuracy_line = alt.Chart(accuracy_over_time).mark_line().encode(
        x=alt.X('time_until:N', title='Time Until Finalization'),
        y=alt.Y('mean_abs_error:Q', title='Mean Absolute Error (Hours)'),
        tooltip=['time_until:N', 'mean_abs_error:Q', 'median_abs_error:Q', 'count:Q']
    ).properties(
        title='Estimate Accuracy vs Time Until Finalization',
        width=600,
        height=400
    )
    visualizations['accuracy_over_time'] = accuracy_line
    
    # 7. NEW: Individual withdrawal estimates over time
    top_tracking_df = analyze_individual_estimates(df)  # Use original df for analysis
    
    # Convert datetimes to string for visualization
    individual_tracking = top_tracking_df.copy()
    individual_tracking['time_of_estimate'] = individual_tracking['time_of_estimate'].dt.strftime('%Y-%m-%d %H:%M:%S')
    individual_tracking['withdrawal_id'] = individual_tracking['withdrawal_id'].astype(str)
    individual_tracking['hours_until_estimated'] = individual_tracking['hours_until_estimated'].astype(float)
    individual_tracking['hours_until_actual'] = individual_tracking['hours_until_actual'].astype(float)
    
    # Create a selection for the withdrawal ID
    withdrawal_selection = alt.selection_point(fields=['withdrawal_id'], bind='legend')
    
    # Create a line chart showing how estimates changed for individual withdrawals
    estimates_line = alt.Chart(individual_tracking).mark_line().encode(
        x=alt.X('time_of_estimate:T', title='Time of Estimate'),
        y=alt.Y('hours_until_estimated:Q', title='Hours Until Estimated Finalization'),
        color=alt.Color('withdrawal_id:N', title='Withdrawal ID'),
        opacity=alt.condition(withdrawal_selection, alt.value(1), alt.value(0.2)),
        tooltip=['withdrawal_id', 'time_of_estimate', 'hours_until_estimated:Q', 'hours_until_actual:Q']
    ).properties(
        title='Individual Withdrawal Estimates Over Time',
        width=800,
        height=500
    ).add_params(
        withdrawal_selection
    )
    
    # Add reference lines for actual finalization times
    actual_finalization_refs = []
    
    for withdrawal_id in top_tracking_df['withdrawal_id'].unique():
        # Get the actual finalization time (same for all rows with this withdrawal_id)
        actual_time = top_tracking_df[top_tracking_df['withdrawal_id'] == withdrawal_id]['actual_time'].iloc[0]
        
        # Get the earliest estimate time for this withdrawal
        min_estimate_time = top_tracking_df[top_tracking_df['withdrawal_id'] == withdrawal_id]['time_of_estimate'].min()
        
        # Get the latest estimate time for this withdrawal
        max_estimate_time = top_tracking_df[top_tracking_df['withdrawal_id'] == withdrawal_id]['time_of_estimate'].max()
        
        # Calculate average hours until actual across all estimates for this withdrawal
        avg_hours_until_actual = top_tracking_df[top_tracking_df['withdrawal_id'] == withdrawal_id]['hours_until_actual'].mean()
        
        # Only add reference if we have valid times
        if pd.notna(actual_time) and pd.notna(min_estimate_time) and pd.notna(max_estimate_time):
            # Create a reference DataFrame for this withdrawal
            ref_df = pd.DataFrame({
                'withdrawal_id': [str(withdrawal_id), str(withdrawal_id)],
                'time_of_estimate': [min_estimate_time.strftime('%Y-%m-%d %H:%M:%S'), 
                                   max_estimate_time.strftime('%Y-%m-%d %H:%M:%S')],
                'hours_until_actual': [avg_hours_until_actual, avg_hours_until_actual]
            })
            
            # Create a reference line
            ref_line = alt.Chart(ref_df).mark_line(strokeDash=[6, 4], strokeWidth=2).encode(
                x='time_of_estimate:T',
                y='hours_until_actual:Q',
                color=alt.Color('withdrawal_id:N', title='Withdrawal ID'),
                opacity=alt.condition(withdrawal_selection, alt.value(0.7), alt.value(0.1))
            )
            
            actual_finalization_refs.append(ref_line)
    
    # Combine main chart with all reference lines
    for ref_line in actual_finalization_refs:
        estimates_line += ref_line
    
    visualizations['individual_estimates'] = estimates_line
    
    return visualizations

def calculate_statistics(df):
    """Calculate key statistics about the estimate accuracy"""
    stats = {}
    
    # Overall statistics
    stats['total_withdrawals'] = df['withdrawal_id'].nunique()
    stats['total_estimates'] = len(df)
    stats['mean_error_hours'] = df['error_hours'].mean()
    stats['median_error_hours'] = df['error_hours'].median()
    stats['std_error_hours'] = df['error_hours'].std()
    stats['mean_absolute_error'] = df['error_hours'].abs().mean()
    stats['median_absolute_error'] = df['error_hours'].abs().median()
    
    # Error ranges
    stats['within_1hour'] = (df['error_hours'].abs() <= 1).mean() * 100
    stats['within_6hours'] = (df['error_hours'].abs() <= 6).mean() * 100
    stats['within_12hours'] = (df['error_hours'].abs() <= 12).mean() * 100
    stats['within_24hours'] = (df['error_hours'].abs() <= 24).mean() * 100
    
    # Direction of error
    stats['early_estimates'] = (df['error_hours'] > 0).mean() * 100  # Positive error means actual time was later
    stats['late_estimates'] = (df['error_hours'] < 0).mean() * 100   # Negative error means actual time was earlier
    
    # By withdrawal type
    stats['by_type'] = df.groupby('withdrawal_type')['error_hours'].agg([
        'count', 'mean', 'median', 'std', 
        lambda x: x.abs().mean()
    ]).rename(columns={'<lambda_0>': 'mean_absolute_error'}).to_dict('index')
    
    # By estimate lead time (grouped)
    df['lead_time_group'] = pd.cut(df['hours_in_advance'], 
                                   bins=[0, 24, 48, 72, float('inf')],
                                   labels=['0-24h', '24-48h', '48-72h', '72h+'])
    
    stats['by_lead_time'] = df.groupby('lead_time_group')['error_hours'].agg([
        'count', 'mean', 'median', 'std', 
        lambda x: x.abs().mean()
    ]).rename(columns={'<lambda_0>': 'mean_absolute_error'}).to_dict('index')
    
    # Time of day statistics
    hourly_stats = analyze_time_of_day(df)
    stats['peak_processing_hour'] = hourly_stats.loc[hourly_stats['count'].idxmax(), 'hour']
    stats['peak_processing_count'] = hourly_stats['count'].max()
    
    # Batch processing statistics
    batch_counts, batch_stats = analyze_batch_processing(df)
    stats['mean_batch_size'] = batch_stats['mean_batch_size'].mean()
    stats['median_batch_size'] = batch_stats['median_batch_size'].mean()
    stats['max_batch_size'] = batch_stats['max_batch_size'].max()
    
    # Accuracy over time statistics
    accuracy_over_time = analyze_accuracy_over_time(df)
    stats['accuracy_improvement'] = {
        '0-6h': accuracy_over_time.loc[accuracy_over_time['time_until'] == '0-6h', 'mean_abs_error'].iloc[0],
        '6-12h': accuracy_over_time.loc[accuracy_over_time['time_until'] == '6-12h', 'mean_abs_error'].iloc[0],
        '12-24h': accuracy_over_time.loc[accuracy_over_time['time_until'] == '12-24h', 'mean_abs_error'].iloc[0],
        '24-48h': accuracy_over_time.loc[accuracy_over_time['time_until'] == '24-48h', 'mean_abs_error'].iloc[0],
        '48-72h': accuracy_over_time.loc[accuracy_over_time['time_until'] == '48-72h', 'mean_abs_error'].iloc[0],
        '72h+': accuracy_over_time.loc[accuracy_over_time['time_until'] == '72h+', 'mean_abs_error'].iloc[0]
    }
    
    return stats

def generate_html(stats, visualizations):
    """Generate HTML for the static page with Altair visualizations"""
    
    # HTML header with Vega-Lite and Vega-Embed libraries for Altair charts
    html_header = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Withdrawal Time Estimate Accuracy Analysis</title>
    <script src="https://cdn.jsdelivr.net/npm/vega@5"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }}
        h1, h2, h3 {{
            color: #2c3e50;
        }}
        .dashboard {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }}
        .stat-card {{
            background-color: #f8f9fa;
            border-radius: 8px;
            padding: 15px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }}
        .visualizations {{
            display: grid;
            grid-template-columns: 1fr;
            gap: 30px;
            margin-top: 40px;
        }}
        .chart-container {{
            background-color: white;
            border-radius: 8px;
            padding: 15px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            height: 500px;
            width: 100%;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th, td {{
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background-color: #f2f2f2;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .highlight {{
            font-weight: bold;
            color: #2980b9;
        }}
        .positive {{
            color: #e74c3c;
        }}
        .negative {{
            color: #27ae60;
        }}
        .note {{
            font-style: italic;
            color: #7f8c8d;
            margin-top: 8px;
        }}
        .section {{
            margin-bottom: 40px;
        }}
    </style>
</head>
<body>
    <h1>Withdrawal Time Estimate Accuracy Analysis</h1>
    <p>Analysis of the accuracy of withdrawal time estimates versus actual finalization times.</p>
    
    <div class="section">
        <h2>Key Statistics</h2>
        <div class="dashboard">
            <div class="stat-card">
                <h3>Overview</h3>
                <p>Total Withdrawals: <span class="highlight">{total_withdrawals}</span></p>
                <p>Total Estimates: <span class="highlight">{total_estimates}</span></p>
            </div>
            
            <div class="stat-card">
                <h3>Error Metrics</h3>
                <p>Mean Error: <span class="highlight">{mean_error:.2f} hours</span></p>
                <p>Median Error: <span class="highlight">{median_error:.2f} hours</span></p>
                <p>Mean Absolute Error: <span class="highlight">{mean_abs_error:.2f} hours</span></p>
                <p class="note">Positive error means actual time was later than estimated</p>
            </div>
            
            <div class="stat-card">
                <h3>Accuracy Ranges</h3>
                <p>Within 1 hour: <span class="highlight">{within_1h:.1f}%</span></p>
                <p>Within 6 hours: <span class="highlight">{within_6h:.1f}%</span></p>
                <p>Within 12 hours: <span class="highlight">{within_12h:.1f}%</span></p>
                <p>Within 24 hours: <span class="highlight">{within_24h:.1f}%</span></p>
            </div>
            
            <div class="stat-card">
                <h3>Estimate Direction</h3>
                <p>Early Estimates: <span class="positive">{early_est:.1f}%</span> (actual later than estimated)</p>
                <p>Late Estimates: <span class="negative">{late_est:.1f}%</span> (actual earlier than estimated)</p>
            </div>
        </div>
    </div>

    <div class="section">
        <h2>Processing Patterns</h2>
        <div class="dashboard">
            <div class="stat-card">
                <h3>Time of Day Analysis</h3>
                <p>Peak Processing Hour: <span class="highlight">{peak_hour:02d}:00 UTC</span></p>
                <p>Peak Processing Count: <span class="highlight">{peak_count}</span> withdrawals</p>
            </div>
            
            <div class="stat-card">
                <h3>Batch Processing</h3>
                <p>Mean Batch Size: <span class="highlight">{mean_batch:.1f}</span> withdrawals</p>
                <p>Median Batch Size: <span class="highlight">{median_batch:.1f}</span> withdrawals</p>
                <p>Largest Batch: <span class="highlight">{max_batch}</span> withdrawals</p>
            </div>
        </div>
    </div>

    <div class="section">
        <h2>Accuracy Over Time</h2>
        <div class="dashboard">
            <div class="stat-card">
                <h3>Estimate Accuracy by Time Until Finalization</h3>
                <p>0-6 hours: <span class="highlight">{acc_0_6:.2f} hours</span> error</p>
                <p>6-12 hours: <span class="highlight">{acc_6_12:.2f} hours</span> error</p>
                <p>12-24 hours: <span class="highlight">{acc_12_24:.2f} hours</span> error</p>
                <p>24-48 hours: <span class="highlight">{acc_24_48:.2f} hours</span> error</p>
                <p>48-72 hours: <span class="highlight">{acc_48_72:.2f} hours</span> error</p>
                <p>72+ hours: <span class="highlight">{acc_72_plus:.2f} hours</span> error</p>
            </div>
        </div>
    </div>
    """
    
    # Format the HTML header with statistics
    formatted_header = html_header.format(
        total_withdrawals=stats['total_withdrawals'],
        total_estimates=stats['total_estimates'],
        mean_error=stats['mean_error_hours'],
        median_error=stats['median_error_hours'],
        mean_abs_error=stats['mean_absolute_error'],
        within_1h=stats['within_1hour'],
        within_6h=stats['within_6hours'],
        within_12h=stats['within_12hours'],
        within_24h=stats['within_24hours'],
        early_est=stats['early_estimates'],
        late_est=stats['late_estimates'],
        peak_hour=stats['peak_processing_hour'],
        peak_count=stats['peak_processing_count'],
        mean_batch=stats['mean_batch_size'],
        median_batch=stats['median_batch_size'],
        max_batch=stats['max_batch_size'],
        acc_0_6=stats['accuracy_improvement']['0-6h'],
        acc_6_12=stats['accuracy_improvement']['6-12h'],
        acc_12_24=stats['accuracy_improvement']['12-24h'],
        acc_24_48=stats['accuracy_improvement']['24-48h'],
        acc_48_72=stats['accuracy_improvement']['48-72h'],
        acc_72_plus=stats['accuracy_improvement']['72h+']
    )
    
    # Table for withdrawal types
    type_table = """
    <div class="section">
        <h2>By Withdrawal Type</h2>
        <table>
            <tr>
                <th>Type</th>
                <th>Count</th>
                <th>Mean Error (h)</th>
                <th>Median Error (h)</th>
                <th>Mean Abs Error (h)</th>
            </tr>
    """
    
    for wtype, metrics in stats['by_type'].items():
        type_table += f"""
            <tr>
                <td>{wtype}</td>
                <td>{metrics['count']}</td>
                <td>{metrics['mean']:.2f}</td>
                <td>{metrics['median']:.2f}</td>
                <td>{metrics['mean_absolute_error']:.2f}</td>
            </tr>
        """
    
    type_table += "</table></div>"
    
    # Table for lead time groups
    lead_time_table = """
    <div class="section">
        <h2>By Lead Time</h2>
        <table>
            <tr>
                <th>Lead Time</th>
                <th>Count</th>
                <th>Mean Error (h)</th>
                <th>Median Error (h)</th>
                <th>Mean Abs Error (h)</th>
            </tr>
    """
    
    for lead_time, metrics in stats['by_lead_time'].items():
        if pd.isna(lead_time):
            continue
        lead_time_table += f"""
            <tr>
                <td>{lead_time}</td>
                <td>{metrics['count']}</td>
                <td>{metrics['mean']:.2f}</td>
                <td>{metrics['median']:.2f}</td>
                <td>{metrics['mean_absolute_error']:.2f}</td>
            </tr>
        """
    
    lead_time_table += "</table></div>"
    
    # Visualizations section with Altair charts
    vis_section = """
    <div class="section">
        <h2>Visualizations</h2>
        <div class="visualizations">
    """
    
    # Add each Altair visualization
    for i, (title, chart) in enumerate(visualizations.items()):
        chart_title = ' '.join(word.capitalize() for word in title.split('_'))
        chart_html = f"""
            <div class="chart-container">
                <h3>{chart_title}</h3>
                <div id="vis{i}" class="vis-container"></div>
            </div>
        """
        vis_section += chart_html
    
    vis_section += "</div></div>"
    
    # Notes and footer
    footer = """
    <div class="section">
        <h2>Notes</h2>
        <ul>
            <li>Analysis based on withdrawals that have both estimated finalization times and actual finalization times.</li>
            <li>Positive errors mean that the actual finalization occurred later than estimated.</li>
            <li>Negative errors mean that the actual finalization occurred earlier than estimated.</li>
            <li>Data collected from DynamoDB table "lido_withdrawal_requests".</li>
            <li>Withdrawals are processed in bulk once a day at roughly the same time.</li>
            <li>Analysis timestamp: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC') + """</li>
        </ul>
    </div>
    """
    
    # JavaScript to render the visualizations
    js_section = "<script>"
    
    for i, (title, chart) in enumerate(visualizations.items()):
        # Get chart as dictionary and use custom JSON encoder
        spec_dict = chart.to_dict()
        spec_str = json.dumps(spec_dict, cls=CustomJSONEncoder)
        js_section += f"""
        vegaEmbed('#vis{i}', {spec_str}, {{actions: false}}).catch(console.error);
        """
    
    js_section += "</script></body></html>"
    
    # Combine all sections
    full_html = formatted_header + type_table + lead_time_table + vis_section + footer + js_section
    
    return full_html

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Analyze withdrawal time estimate accuracy')
    parser.add_argument('--test', action='store_true', help='Run in test mode with limited data')
    parser.add_argument('--limit', type=int, default=10, help='Number of withdrawals to process in test mode (default: 10)')
    args = parser.parse_args()
    
    print("Starting withdrawal time estimate accuracy analysis...")
    if args.test:
        print(f"Running in test mode with {args.limit} withdrawals")
    
    # Analyze data
    df = analyze_estimate_accuracy(args.test, args.limit)
    if df is None or df.empty:
        print("No data available for analysis")
        return
        
    # Calculate statistics
    print("Calculating statistics...")
    stats = calculate_statistics(df)
    
    # Generate Altair visualizations
    print("Generating Altair visualizations...")
    visualizations = generate_altair_visualizations(df)
    
    # Generate HTML
    print("Creating HTML report...")
    html = generate_html(stats, visualizations)
    
    # Save HTML to file
    output_file = "index.html"
    with open(output_file, "w") as f:
        f.write(html)
    
    print(f"Analysis complete! Results saved to {output_file}")
    print(f"You can now host this file on GitHub Pages.")

if __name__ == "__main__":
    main() 