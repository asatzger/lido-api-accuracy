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
    """
    Analyze daily processing patterns of unique withdrawals
    Track how many unique withdrawals are processed each day
    """
    # Use actual finalization time to determine processing day
    df['processing_date'] = df['actual_time'].dt.date
    
    # Group by processing date and count unique withdrawal_ids
    daily_withdrawals = df.groupby('processing_date').agg({
        'withdrawal_id': pd.Series.nunique,  # Count unique withdrawals per day
        'withdrawal_type': 'first'  # Just to keep track of types
    }).reset_index()
    
    daily_withdrawals.columns = ['processing_date', 'unique_withdrawals', 'example_type']
    
    # Calculate statistics about daily processing
    stats = {
        'mean_daily_withdrawals': daily_withdrawals['unique_withdrawals'].mean(),
        'median_daily_withdrawals': daily_withdrawals['unique_withdrawals'].median(),
        'max_daily_withdrawals': daily_withdrawals['unique_withdrawals'].max(),
        'min_daily_withdrawals': daily_withdrawals['unique_withdrawals'].min(),
        'days_with_processing': len(daily_withdrawals)
    }
    
    return daily_withdrawals, stats

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
    
    # Calculate day-based accuracy instead of hour-based
    # Convert error_hours to days and take absolute value
    df['error_days'] = df['error_hours'].abs() / 24
    
    # Error ranges by days
    stats['correct_day'] = (df['error_days'] < 1).mean() * 100  # Less than 1 day difference
    stats['within_1day'] = (df['error_days'] < 2).mean() * 100  # Less than 2 days difference (i.e., +/- 1 day)
    stats['within_2days'] = (df['error_days'] < 3).mean() * 100
    stats['within_3days'] = (df['error_days'] < 4).mean() * 100
    stats['within_7days'] = (df['error_days'] < 8).mean() * 100
    
    # Direction of error - based on all withdrawals
    # For reporting, we still consider estimates within 1 hour of actual as correct
    correct_estimates = (df['error_hours'].abs() < 1).mean() * 100
    early_estimates = (df['error_hours'] > 1).mean() * 100  # Positive error means actual time was later
    late_estimates = (df['error_hours'] < -1).mean() * 100  # Negative error means actual time was earlier
    
    # Store these values
    stats['correct_estimates'] = correct_estimates
    stats['early_estimates'] = early_estimates
    stats['late_estimates'] = late_estimates
    
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
    
    # Calculate day-based accuracy distribution for cumulative chart
    max_days_error = min(int(df['error_days'].max()) + 1, 10)  # Cap at 10 days
    accuracy_by_days = []
    
    for day in range(max_days_error + 1):
        accuracy = (df['error_days'] < day).mean() * 100
        accuracy_by_days.append({
            'days': day,
            'cumulative_accuracy': accuracy
        })
    
    stats['accuracy_by_days'] = accuracy_by_days
    
    # Processing pattern statistics
    daily_withdrawals, processing_stats = analyze_batch_processing(df)
    stats['mean_daily_withdrawals'] = processing_stats['mean_daily_withdrawals']
    stats['median_daily_withdrawals'] = processing_stats['median_daily_withdrawals']
    stats['max_daily_withdrawals'] = processing_stats['max_daily_withdrawals']
    stats['days_with_processing'] = processing_stats['days_with_processing']
    
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
    
    return stats, daily_withdrawals

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
    
    # Add error_days column for day-based visualizations
    df_viz['error_days'] = df_viz['error_hours'] / 24
    
    # 1. Error distribution histogram - one bar per day
    # Round error_days to nearest integer for day-based binning
    df_viz['error_days_rounded'] = np.round(df_viz['error_days'])
    
    # Define the range of days to show
    max_days = min(7, int(np.ceil(df_viz['error_days_rounded'].abs().max())))
    day_values = list(range(-max_days, max_days + 1))
    
    # Create a bar chart with one bar per day
    error_hist = alt.Chart(df_viz).mark_bar().encode(
        alt.X('error_days_rounded:Q', 
              title='Error (Actual - Estimated) in Days',
              axis=alt.Axis(values=day_values, tickMinStep=1),
              scale=alt.Scale(domain=[-max_days, max_days])),
        alt.Y('count()', title='Frequency'),
        tooltip=['error_days_rounded:Q', 'count()']
    ).properties(
        title='Distribution of Estimate Errors (Days)',
        width=900,
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
    
    # 2. Error vs Estimation Lead Time - using actual completion time instead of expected
    # Calculate days between estimate time and actual completion time
    df_viz['days_until_actual'] = (df_viz['actual_time'] - df_viz['time_of_estimate']).dt.total_seconds() / (24 * 3600)
    
    # Add hour of estimate to help with sampling
    df_viz['estimate_hour'] = df_viz['time_of_estimate'].dt.strftime('%Y-%m-%d-%H')
    
    # Select one estimate per withdrawal ID per hour to reduce density
    df_viz_sampled = df_viz.sort_values('time_of_estimate').groupby(['withdrawal_id', 'estimate_hour']).first().reset_index()
    print(f"Reduced Error vs Leadtime chart data points from {len(df_viz)} to {len(df_viz_sampled)} (one per withdrawal ID per hour)")
    
    # Add a small random jitter to better visualize point density
    df_viz_sampled['error_days_jittered'] = df_viz_sampled['error_days'] + np.random.normal(0, 0.1, size=len(df_viz_sampled))
    
    scatter = alt.Chart(df_viz_sampled).mark_circle(opacity=0.7, size=60).encode(
        x=alt.X('days_until_actual:Q', 
                title='Days Between Estimate and Actual Completion Time',
                axis=alt.Axis(titleFontSize=14)),
        y=alt.Y('error_days_jittered:Q', 
                title='Error in Days (Positive = Actual later than estimated)',
                axis=alt.Axis(titleFontSize=14)),
        tooltip=['withdrawal_id', 
                alt.Tooltip('error_days_jittered:Q', format='.2f', title='Error (days)'), 
                alt.Tooltip('days_until_actual:Q', format='.2f', title='Days until actual completion')]
    ).properties(
        title=alt.TitleParams(
            'Estimate Error vs. Lead Time',
            subtitle='Shows how accuracy varies with how far in advance the estimate was made',
            fontSize=16
        ),
        width=900,
        height=400
    )
    
    # Add a rule for perfect estimate (Error = 0)
    zero_line = alt.Chart(pd.DataFrame({'y': [0]})).mark_rule(
        color='red', 
        strokeDash=[6, 4],
        strokeWidth=2
    ).encode(y='y:Q')
    
    # Add explanatory text annotations
    positive_text = alt.Chart(pd.DataFrame({
        'x': [df_viz_sampled['days_until_actual'].max() * 0.9],
        'y': [df_viz_sampled['error_days_jittered'].max() * 0.8],
        'text': ['Actual later than estimated']
    })).mark_text(fontSize=14, color='#e74c3c', fontWeight='bold').encode(
        x='x:Q',
        y='y:Q',
        text='text:N'
    )
    
    negative_text = alt.Chart(pd.DataFrame({
        'x': [df_viz_sampled['days_until_actual'].max() * 0.9],
        'y': [df_viz_sampled['error_days_jittered'].min() * 0.8],
        'text': ['Actual earlier than estimated']
    })).mark_text(fontSize=14, color='#27ae60', fontWeight='bold').encode(
        x='x:Q',
        y='y:Q',
        text='text:N'
    )
    
    # Add notes explaining chart features
    gap_note = alt.Chart(pd.DataFrame({
        'x': [df_viz_sampled['days_until_actual'].max() * 0.5],
        'y': [df_viz_sampled['error_days_jittered'].min() * 0.5],
        'text': ['Horizontal gaps represent regular update intervals']
    })).mark_text(fontSize=12, color='#666666', align='center').encode(
        x='x:Q',
        y='y:Q',
        text='text:N'
    )
    
    jitter_note = alt.Chart(pd.DataFrame({
        'x': [df_viz_sampled['days_until_actual'].max() * 0.5],
        'y': [df_viz_sampled['error_days_jittered'].min() * 0.35],
        'text': ['Points are jittered to better show density']
    })).mark_text(fontSize=12, color='#666666', align='center').encode(
        x='x:Q',
        y='y:Q',
        text='text:N'
    )
    
    error_vs_leadtime_chart = (scatter + zero_line + positive_text + negative_text)
    
    # Store the notes as separate variables to be used in HTML generation
    leadtime_chart_notes = [
        "Horizontal gaps represent regular update intervals",
        "Points are jittered to better show density"
    ]
    
    visualizations['error_vs_leadtime'] = error_vs_leadtime_chart
    visualizations['error_vs_leadtime_notes'] = leadtime_chart_notes
    
    # 4. New: Cumulative accuracy by day difference chart
    # Create a data frame with discrete whole day values (0, 1, 2, 3, etc.)
    max_days_error = min(int(df_viz['error_days'].abs().max()) + 1, 10)  # Cap at 10 days
    
    # Generate data for whole day values
    accuracy_by_days_df = pd.DataFrame([
        {"days": day, "cumulative_accuracy": (df_viz['error_days'].abs() < day).mean() * 100}
        for day in range(max_days_error + 1)
    ])
    
    # Create a line chart showing cumulative accuracy with discrete day values
    cumulative_accuracy = alt.Chart(accuracy_by_days_df).mark_line(point=True).encode(
        x=alt.X('days:Q', 
                title='Days Difference',
                axis=alt.Axis(values=list(range(max_days_error + 1)), tickMinStep=1),
                scale=alt.Scale(domain=[0, max_days_error])),
        y=alt.Y('cumulative_accuracy:Q', 
                title='Cumulative Percentage of Estimates', 
                scale=alt.Scale(domain=[0, 100])),
        tooltip=['days:Q', alt.Tooltip('cumulative_accuracy:Q', title='Cumulative Percentage', format='.1f')]
    ).properties(
        title='Cumulative Share of Estimates by Days Difference',
        width=900,
        height=400
    )
    
    visualizations['cumulative_accuracy'] = cumulative_accuracy
    
    # 5. Accuracy over time
    accuracy_over_time = analyze_accuracy_over_time(df)  # Use original df for analysis
    accuracy_over_time = accuracy_over_time.copy()
    
    # Define the correct order for time intervals
    time_order = ['0-6h', '6-12h', '12-24h', '24-48h', '48-72h', '72h+']
    
    accuracy_over_time['time_until'] = accuracy_over_time['time_until'].astype(str)
    accuracy_over_time['mean_error'] = accuracy_over_time['mean_error'].astype(float)
    accuracy_over_time['median_error'] = accuracy_over_time['median_error'].astype(float)
    accuracy_over_time['std_error'] = accuracy_over_time['std_error'].astype(float)
    accuracy_over_time['count'] = accuracy_over_time['count'].astype(int)
    accuracy_over_time['mean_abs_error'] = accuracy_over_time['mean_abs_error'].astype(float)
    accuracy_over_time['median_abs_error'] = accuracy_over_time['median_abs_error'].astype(float)
    
    # Create a line chart showing accuracy improvement with correct ordering
    accuracy_line = alt.Chart(accuracy_over_time).mark_line().encode(
        x=alt.X('time_until:N', title='Time Until Finalization', sort=time_order),
        y=alt.Y('mean_abs_error:Q', title='Mean Absolute Error (Hours)'),
        tooltip=['time_until:N', 'mean_abs_error:Q', 'median_abs_error:Q', 'count:Q']
    ).properties(
        title='Estimate Accuracy vs Time Until Finalization',
        width=900,
        height=400
    )
    visualizations['accuracy_over_time'] = accuracy_line
    
    # 6. NEW: Daily Withdrawal Processing with improved x-axis
    # Get daily withdrawal data
    _, daily_withdrawals = calculate_statistics(df)
    daily_withdrawals_viz = daily_withdrawals.copy()
    
    # Convert date to string for visualization
    daily_withdrawals_viz['processing_date_str'] = daily_withdrawals_viz['processing_date'].astype(str)
    
    # Get min and max dates for axis configuration
    date_range = pd.date_range(
        start=daily_withdrawals_viz['processing_date'].min(),
        end=daily_withdrawals_viz['processing_date'].max(),
        freq='D'
    )
    
    # Bar chart showing number of unique withdrawals processed each day
    daily_processing_chart = alt.Chart(daily_withdrawals_viz).mark_bar().encode(
        x=alt.X(
            'processing_date_str:T', 
            title='Processing Date', 
            axis=alt.Axis(
                format='%Y-%m-%d',
                tickCount=len(date_range) if len(date_range) <= 30 else 30,  # Limit ticks for large date ranges
                labelAngle=-45  # Angle labels for better readability
            ),
            scale=alt.Scale(
                domain=[date_range.min().isoformat(), date_range.max().isoformat()]
            )
        ),
        y=alt.Y('unique_withdrawals:Q', title='Number of Unique Withdrawals'),
        tooltip=['processing_date_str:T', 'unique_withdrawals:Q']
    ).properties(
        title='Number of Unique Withdrawals Processed Per Day',
        width=900,
        height=400
    )
    
    visualizations['daily_processing'] = daily_processing_chart
    
    # 7. Distribution of daily withdrawal counts with improved x-axis
    min_withdrawals = int(daily_withdrawals_viz['unique_withdrawals'].min())
    max_withdrawals = int(daily_withdrawals_viz['unique_withdrawals'].max())
    bin_step = max(1, (max_withdrawals - min_withdrawals) // 10)  # Create about 10 bins
    
    withdrawal_count_hist = alt.Chart(daily_withdrawals_viz).mark_bar().encode(
        x=alt.X(
            'unique_withdrawals:Q', 
            bin=alt.Bin(step=bin_step),  # Use fixed step size for bins
            title='Number of Withdrawals Per Day',
            scale=alt.Scale(domain=[min_withdrawals, max_withdrawals]),
            axis=alt.Axis(tickMinStep=1)  # Ensure whole number ticks
        ),
        y=alt.Y('count()', title='Frequency (Days)'),
        tooltip=['count()', alt.Tooltip('unique_withdrawals:Q', title='Withdrawals per Day')]
    ).properties(
        title='Distribution of Daily Withdrawal Processing Counts',
        width=900,
        height=400
    )
    
    visualizations['withdrawal_distribution'] = withdrawal_count_hist
    
    # 8. Individual withdrawal estimates over time
    top_tracking_df = analyze_individual_estimates(df)  # Use original df for analysis
    
    # Use a copy to avoid modifying the original
    individual_tracking = top_tracking_df.copy()
    
    # Format datetime columns to ISO format strings
    individual_tracking['time_of_estimate_str'] = individual_tracking['time_of_estimate'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    individual_tracking['withdrawal_id'] = individual_tracking['withdrawal_id'].astype(str)
    individual_tracking['hours_until_estimated'] = individual_tracking['hours_until_estimated'].astype(float)
    individual_tracking['hours_until_actual'] = individual_tracking['hours_until_actual'].astype(float)
    
    # Create a line chart showing how estimates changed for individual withdrawals
    # Use hours_until_actual as x-axis instead of time_of_estimate
    estimates_line = alt.Chart(individual_tracking).mark_line(strokeWidth=3).encode(
        x=alt.X('hours_until_actual:Q', 
                title='Hours Until Actual Finalization', 
                axis=alt.Axis(titleFontSize=14)),
        y=alt.Y('hours_until_estimated:Q', 
                title='Hours Until Estimated Finalization', 
                scale=alt.Scale(zero=False)),
        color=alt.Color('withdrawal_id:N', title='Withdrawal ID', legend=None),
        tooltip=['withdrawal_id', 
                'time_of_estimate_str:T', 
                alt.Tooltip('hours_until_estimated:Q', title='Estimated hours remaining'),
                alt.Tooltip('hours_until_actual:Q', title='Actual hours remaining')]
    ).properties(
        title='Individual Withdrawal Estimates vs Actual Finalization Time',
        width=900,
        height=500
    )
    
    # Add reference lines for actual finalization times
    actual_finalization_refs = []
    
    for withdrawal_id in individual_tracking['withdrawal_id'].unique():
        # Get subset for this withdrawal
        withdrawal_subset = individual_tracking[individual_tracking['withdrawal_id'] == withdrawal_id]
        
        # Calculate average hours until actual 
        avg_hours_until_actual = withdrawal_subset['hours_until_actual'].mean()
        
        # Create reference data for this withdrawal
        ref_df = pd.DataFrame({
            'withdrawal_id': [withdrawal_id],
            'hours_until_actual': [avg_hours_until_actual]
        })
        
        # Create a reference line as a rule
        ref_line = alt.Chart(ref_df).mark_rule(
            strokeDash=[6, 4],
            strokeWidth=2,
            color='red'
        ).encode(
            y='hours_until_actual:Q',
            color=alt.Color('withdrawal_id:N', legend=None)
        )
        
        actual_finalization_refs.append(ref_line)
    
    # Combine main chart with all reference lines
    individual_chart = estimates_line
    for ref_line in actual_finalization_refs:
        individual_chart += ref_line
    
    visualizations['individual_estimates'] = individual_chart
    
    return visualizations

def generate_html(stats, visualizations):
    """Generate HTML for the static page with Altair visualizations"""
    
    # HTML header with Vega-Lite and Vega-Embed libraries for Altair charts
    html_header = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Withdrawal Time Estimate Accuracy Analysis</title>
    <script src="https://cdn.jsdelivr.net/npm/vega@5.22.1"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@5.6.0"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6.21.0"></script>
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
        /* Side-by-side sections */
        .side-by-side-sections {{
            display: flex;
            justify-content: space-between;
            gap: 20px;
            margin-bottom: 40px;
        }}
        .half-section {{
            flex: 1;
            max-width: 48%;
        }}
        .chart-container {{
            background-color: white;
            border-radius: 8px;
            padding: 15px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            height: auto;
            min-height: 500px;
            width: 100%;
            overflow: hidden;
            margin-bottom: 30px;
        }}
        .vis-container {{
            width: 100%;
            height: 100%;
            min-height: 400px;
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
        .chart-notes {{
            margin-top: 15px;
            background-color: #f8f9fa;
            padding: 10px 15px;
            border-radius: 5px;
            font-size: 14px;
        }}
        .chart-notes ul {{
            margin: 8px 0 0 0;
            padding-left: 25px;
        }}
        .chart-notes li {{
            margin-bottom: 5px;
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
                <h3>Day-Based Accuracy</h3>
                <p>Correct Day: <span class="highlight">{correct_day:.1f}%</span></p>
                <p>Within ±1 day: <span class="highlight">{within_1day:.1f}%</span></p>
                <p>Within ±2 days: <span class="highlight">{within_2days:.1f}%</span></p>
                <p>Within ±3 days: <span class="highlight">{within_3days:.1f}%</span></p>
                <p>Within ±7 days: <span class="highlight">{within_7days:.1f}%</span></p>
            </div>
            
            <div class="stat-card">
                <h3>Estimate Direction</h3>
                <p>Early Estimates: <span class="positive">{early_est:.1f}%</span> (actual later than estimated)</p>
                <p>Late Estimates: <span class="negative">{late_est:.1f}%</span> (actual earlier than estimated)</p>
                <p class="note">Excludes estimates within ±1 hour of actual time (considered correct)</p>
            </div>
        </div>
    </div>

    <!-- Processing Patterns and Accuracy Over Time side by side -->
    <div class="side-by-side-sections">
        <!-- Left section: Processing Patterns -->
        <div class="half-section">
            <h2>Processing Patterns</h2>
            <div class="dashboard">
                <div class="stat-card">
                    <h3>Daily Processing</h3>
                    <p>Mean Withdrawals per Day: <span class="highlight">{mean_daily:.1f}</span></p>
                    <p>Median Withdrawals per Day: <span class="highlight">{median_daily:.1f}</span></p>
                    <p>Maximum Withdrawals per Day: <span class="highlight">{max_daily}</span></p>
                    <p>Days with Processing: <span class="highlight">{days_processing}</span></p>
                    <p class="note">Withdrawals are processed in bulk once per day</p>
                </div>
            </div>
        </div>
        
        <!-- Right section: Accuracy Over Time -->
        <div class="half-section">
            <h2>Accuracy Over Time</h2>
            <div class="dashboard">
                <div class="stat-card">
                    <h3>Estimate Accuracy by Time Until Finalization</h3>
                    <p>Mean Absolute Error (hours) by time remaining:</p>
                    <p>0-6 hours: <span class="highlight">{acc_0_6:.2f} hours</span></p>
                    <p>6-12 hours: <span class="highlight">{acc_6_12:.2f} hours</span></p>
                    <p>12-24 hours: <span class="highlight">{acc_12_24:.2f} hours</span></p>
                    <p>24-48 hours: <span class="highlight">{acc_24_48:.2f} hours</span></p>
                    <p>48-72 hours: <span class="highlight">{acc_48_72:.2f} hours</span></p>
                    <p>72+ hours: <span class="highlight">{acc_72_plus:.2f} hours</span></p>
                </div>
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
        correct_day=stats['correct_day'],
        within_1day=stats['within_1day'],
        within_2days=stats['within_2days'],
        within_3days=stats['within_3days'],
        within_7days=stats['within_7days'],
        early_est=stats['early_estimates'],
        late_est=stats['late_estimates'],
        mean_daily=stats['mean_daily_withdrawals'],
        median_daily=stats['median_daily_withdrawals'],
        max_daily=stats['max_daily_withdrawals'],
        days_processing=stats['days_with_processing'],
        acc_0_6=stats['accuracy_improvement']['0-6h'],
        acc_6_12=stats['accuracy_improvement']['6-12h'],
        acc_12_24=stats['accuracy_improvement']['12-24h'],
        acc_24_48=stats['accuracy_improvement']['24-48h'],
        acc_48_72=stats['accuracy_improvement']['48-72h'],
        acc_72_plus=stats['accuracy_improvement']['72h+']
    )
    
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
    
    # Add each Altair visualization - prepare chart specs but don't embed yet
    chart_specs = {}
    
    for i, (title, chart) in enumerate(visualizations.items()):
        # Skip the error_by_type chart and notes
        if title == 'error_by_type' or title.endswith('_notes'):
            continue
            
        chart_title = ' '.join(word.capitalize() for word in title.split('_'))
        
        # Add notes for Error vs Leadtime chart
        notes_html = ""
        if title == 'error_vs_leadtime' and f"{title}_notes" in visualizations:
            notes_html = '<div class="chart-notes"><strong>Notes:</strong><ul>'
            for note in visualizations[f"{title}_notes"]:
                notes_html += f"<li>{note}</li>"
            notes_html += "</ul></div>"
        
        chart_html = f"""
            <div class="chart-container">
                <h3>{chart_title}</h3>
                <div id="vis{i}" class="vis-container"></div>
                {notes_html}
            </div>
        """
        vis_section += chart_html
        
        # Convert to spec dict
        chart_specs[f"vis{i}"] = chart.to_dict()
    
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
            <li>Day-based accuracy metrics are more relevant than hour-based metrics due to the once-per-day processing pattern.</li>
            <li>The processing patterns section focuses on unique withdrawals processed each day, not individual estimates.</li>
            <li>For "Estimate Direction" statistics, estimates within ±1 hour of actual time are excluded (considered correct).</li>
            <li>The "true lead time" reference line in the Error vs Leadtime chart shows the theoretical boundary with 24-hour steps.</li>
            <li>Analysis timestamp: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC') + """</li>
        </ul>
    </div>
    """
    
    # JavaScript to render the visualizations - simpler embedding approach
    js_section = "<script>"
    
    # Add chart specs as a global variable
    specs_json = json.dumps(chart_specs, cls=CustomJSONEncoder)
    js_section += f"""
    // Chart specifications
    const chartSpecs = {specs_json};
    
    // Function to render all charts
    function renderCharts() {{
        // Render each chart
        Object.keys(chartSpecs).forEach(function(elemId) {{
            vegaEmbed('#' + elemId, chartSpecs[elemId], {{
                mode: "vega-lite",
                actions: false,
                renderer: "svg",
                logLevel: 'info'
            }}).catch(function(error) {{
                console.error('Error rendering chart', elemId, error);
                document.getElementById(elemId).innerHTML = 
                    '<p style="color:red">Error rendering chart: ' + error.message + '</p>';
            }});
        }});
    }}
    
    // Render charts when page loads
    document.addEventListener('DOMContentLoaded', renderCharts);
    """
    
    js_section += "</script></body></html>"
    
    # Combine all sections - remove type_table
    full_html = formatted_header + lead_time_table + vis_section + footer + js_section
    
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
    stats, _ = calculate_statistics(df)
    
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