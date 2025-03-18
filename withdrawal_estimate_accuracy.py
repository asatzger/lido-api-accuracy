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
import re

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
            # Check for withdrawal type in all records and use the most specific one
            withdrawal_type = None
            type_fields = ['type', 'request_type', 'withdrawal_type']
            
            # First check the main withdrawal record
            for field in type_fields:
                if field in withdrawal and withdrawal[field]:
                    withdrawal_type = withdrawal[field]
                    print(f"Found type '{withdrawal_type}' in main record for {withdrawal_id}")
                    break
            
            # If not found, check all history records
            if not withdrawal_type:
                for item in history:
                    for field in type_fields:
                        if field in item and item[field]:
                            withdrawal_type = item[field]
                            print(f"Found type '{withdrawal_type}' in history for {withdrawal_id}")
                            break
                    if withdrawal_type:
                        break
            
            # Default to 'unknown' if no type found
            if not withdrawal_type:
                print(f"No type found for withdrawal {withdrawal_id}, using 'unknown'")
                withdrawal_type = 'unknown'
            
            valid_withdrawals.append({
                'withdrawal_id': withdrawal_id,
                'estimates': estimates,
                'actual_finalized_at': actual_finalized_at,
                'type': withdrawal_type
            })
            print(f"Added withdrawal {withdrawal_id} with {len(estimates)} estimates and type '{withdrawal_type}'")
    
    print(f"Found {len(valid_withdrawals)} withdrawals with valid estimation and finalization data")
    
    # Print type distribution
    type_counts = {}
    for withdrawal in valid_withdrawals:
        type_counts[withdrawal['type']] = type_counts.get(withdrawal['type'], 0) + 1
    
    print("\nWithdrawal type distribution:")
    for type_name, count in type_counts.items():
        print(f"  {type_name}: {count} withdrawals")
    
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
    
    # Print withdrawal type distribution in the final dataset
    print("\nWithdrawal types in final analysis dataset:")
    type_distribution = df['withdrawal_type'].value_counts()
    for type_name, count in type_distribution.items():
        print(f"  {type_name}: {count} data points")
    
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
    
    # Generate data for whole day values
    accuracy_by_days_df = pd.DataFrame([
        {"days": day, "cumulative_accuracy": (df['error_days'].abs() < day).mean() * 100}
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
    
    stats['accuracy_by_days'] = accuracy_by_days_df.to_dict(orient='records')
    
    # Processing pattern statistics
    daily_withdrawals, processing_stats = analyze_batch_processing(df)
    stats['mean_daily_withdrawals'] = processing_stats['mean_daily_withdrawals']
    stats['median_daily_withdrawals'] = processing_stats['median_daily_withdrawals']
    stats['max_daily_withdrawals'] = processing_stats['max_daily_withdrawals']
    stats['days_with_processing'] = processing_stats['days_with_processing']
    
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
    
    # NEW: Box plot for errors by withdrawal type
    # Remove any empty or 'unknown' types
    df_viz_by_type = df_viz[df_viz['withdrawal_type'].notna() & 
                          (df_viz['withdrawal_type'] != '') & 
                          (df_viz['withdrawal_type'].str.lower() != 'unknown') &
                          (df_viz['withdrawal_type'].str.lower() != 'none')]
    
    print("\nPreparing withdrawal type data for visualization...")
    print(f"Original dataset size: {len(df_viz)}")
    print(f"After filtering empty/unknown types: {len(df_viz_by_type)}")
    
    # List all unique withdrawal types
    all_types = df_viz['withdrawal_type'].unique()
    print(f"All unique withdrawal types: {all_types}")
    
    # Count occurrences of each type
    type_counts = df_viz_by_type['withdrawal_type'].value_counts().reset_index()
    type_counts.columns = ['withdrawal_type', 'count']
    
    # Print the type counts
    print("\nCounts for each withdrawal type:")
    for _, row in type_counts.iterrows():
        print(f"  {row['withdrawal_type']}: {row['count']} data points")
    
    # Only include types with at least 5 data points
    valid_types = type_counts[type_counts['count'] >= 5]['withdrawal_type'].tolist()
    print(f"\nValid types with 5+ data points: {valid_types}")
    
    if not valid_types:
        print("WARNING: No valid withdrawal types with 5+ data points found!")
        error_by_type = alt.Chart(pd.DataFrame({'x': [0], 'y': [0]})).mark_text(
            text='No withdrawal type data available',
            fontSize=20
        ).encode(x='x:Q', y='y:Q').properties(
            width=900, 
            height=400,
            title="Estimation Error by Withdrawal Type"
        )
    
    else:
        # Filter to valid types
        df_viz_by_type = df_viz_by_type[df_viz_by_type['withdrawal_type'].isin(valid_types)]
        print(f"Final dataset for heatmap: {len(df_viz_by_type)} data points")
        
        # Create error day bins centered around full days (-0.5 to +0.5)
        # Similar to the error vs leadtime heatmap
        df_viz_by_type['error_days'] = df_viz_by_type['error_hours'] / 24
        
        min_error = int(np.floor(df_viz_by_type['error_days'].min())) - 0.5
        max_error = int(np.ceil(df_viz_by_type['error_days'].max())) + 0.5
        
        print(f"Setting bin boundaries from {min_error} to {max_error}")
        
        # Create bins and labels
        bins = np.arange(min_error, max_error + 1, 1)
        
        # Create numeric labels that can be properly sorted
        bin_values = list(range(int(min_error + 0.5), int(max_error + 0.5)))
        raw_labels = [f"{i}" if i < 0 else (f"+{i}" if i > 0 else "0") for i in bin_values]
        
        # Create a list of corresponding numeric values for sorting
        numeric_values = bin_values.copy()
        
        # Use the same labels for display
        display_labels = raw_labels
        
        # Create a mapping from raw to display labels
        label_map = dict(zip(raw_labels, display_labels))
        
        # Create explicit manual sort order with 0 in the middle
        # First place all positive values in descending order
        positive_labels = [label for label in raw_labels if label.startswith('+')]
        positive_labels.sort(key=lambda x: float(x.replace('+', '')), reverse=True)
        
        # Then place 0
        zero_label = ['0'] if '0' in raw_labels else []
        
        # Then place all negative values in descending order (less negative to more negative)
        negative_labels = [label for label in raw_labels if label.startswith('-')]
        negative_labels.sort(key=lambda x: float(x), reverse=True)
        
        # Combine them all
        custom_sort_order = positive_labels + zero_label + negative_labels
        
        # Create the bins using raw numeric labels for consistent sorting
        df_viz_by_type['error_day_bin_raw'] = pd.cut(
            df_viz_by_type['error_days'],
            bins=bins,
            labels=raw_labels,
            right=False  # Make bins left-inclusive
        )
        
        # Map to display labels
        df_viz_by_type['error_day_bin'] = df_viz_by_type['error_day_bin_raw'].map(label_map)
        
        # Create a complete grid of all possible type and error bin combinations
        error_bins = display_labels
        types = valid_types
        
        # Create meshgrid for types and error bins
        type_grid, error_grid = np.meshgrid(types, error_bins)
        
        # Create the base grid DataFrame
        grid_data = pd.DataFrame({
            'withdrawal_type': type_grid.ravel(),
            'error_day_bin': error_grid.ravel()
        })
        
        # Calculate counts for each grid cell
        counts = (df_viz_by_type.groupby(['withdrawal_type', 'error_day_bin'])
                 .size()
                 .reset_index(name='count'))
        
        # Merge the complete grid with actual counts, filling missing values with 0
        heatmap_data = pd.merge(grid_data, counts, 
                               on=['withdrawal_type', 'error_day_bin'],
                               how='left').fillna(0)
        
        # Filter out bins with zero counts for a cleaner visualization
        heatmap_data = heatmap_data[heatmap_data['count'] > 0]
        
        # Calculate percentage within each type (vertical columns sum to 100%)
        total_by_type = heatmap_data.groupby('withdrawal_type')['count'].sum().reset_index()
        heatmap_data = heatmap_data.merge(total_by_type, on='withdrawal_type', suffixes=('', '_total'))
        heatmap_data['percentage'] = (heatmap_data['count'] / heatmap_data['count_total'] * 100).round(1)
        
        # Format withdrawal type names for better display
        heatmap_data['display_type'] = heatmap_data['withdrawal_type'].apply(
            lambda x: ' '.join(word.capitalize() for word in re.findall(r'[A-Z]?[a-z]+', x))
            if re.findall(r'[A-Z]?[a-z]+', x) else x
        )
        
        # Define the correct order of withdrawal types
        type_order = ['buffer', 'vaultsBalance', 'rewardsOnly', 'validatorBalances', 'exitValidators']
        
        # Get the available types in our data
        available_types = heatmap_data['withdrawal_type'].unique().tolist()
        
        # Filter and order the types based on the predefined order
        ordered_types = [t for t in type_order if t in available_types]
        
        # Add any types that might be in our data but not in our predefined order
        for t in available_types:
            if t not in ordered_types:
                ordered_types.append(t)
                
        # Create a mapping for display types in the same order
        ordered_display_types = []
        for t in ordered_types:
            display = heatmap_data[heatmap_data['withdrawal_type'] == t]['display_type'].iloc[0] if len(heatmap_data[heatmap_data['withdrawal_type'] == t]) > 0 else t
            ordered_display_types.append(display)
        
        # Create heatmap using rect marks
        error_by_type_chart = alt.Chart(heatmap_data).mark_rect().encode(
            x=alt.X('display_type:N',
                    title='Withdrawal Type',
                    axis=alt.Axis(
                        labelAngle=-45,
                        titleFontSize=14
                    ),
                    sort=ordered_display_types),  # Use our custom order
            y=alt.Y('error_day_bin:O',
                    title='Error (Days)',
                    axis=alt.Axis(
                        titleFontSize=14,
                        grid=True
                    ),
                    sort=custom_sort_order),
            color=alt.Color('percentage:Q',
                           scale=alt.Scale(
                               scheme='viridis',
                               domain=[0, 100],
                               nice=False
                           ),
                           legend=alt.Legend(
                               title='Percentage of Estimates',
                               format='.1f'
                           )),
            stroke=alt.value('white'),  # Add white borders around cells
            strokeWidth=alt.value(0.5),  # Set border width
            tooltip=[
                alt.Tooltip('display_type:N', title='Withdrawal Type'),
                alt.Tooltip('error_day_bin:N', title='Error Range'),
                alt.Tooltip('count:Q', title='Number of Estimates'),
                alt.Tooltip('percentage:Q', title='Percentage', format='.1f')
            ]
        ).properties(
            title={
                'text': 'Error by Type',
                'subtitle': [
                    'Heatmap shows distribution of errors for each withdrawal type',
                    'Color intensity shows percentage of estimates within each type',
                    'Positive values: Actual completion later than estimated',
                    'Negative values: Actual completion earlier than estimated',
                    'Withdrawal types represent different mechanisms used to fulfill requests:',
                    'buffer: using buffered ETH, vaultsBalance: using ETH from vaults,',
                    'rewardsOnly: using projected rewards, validatorBalances: using scheduled withdrawals,',
                    'exitValidators: requiring additional validator exits'
                ],
                'fontSize': 16
            },
            width=900,
            height=400
        )
        
        # Apply configuration directly to the error_by_type_chart
        error_by_type = error_by_type_chart.configure_view(
            strokeWidth=0
        ).configure_axis(
            grid=True,
            gridOpacity=0.2,
            domain=True,
            domainWidth=2,
            tickSize=10,
            gridColor='white',
            gridWidth=1
        )
    
    visualizations['error_by_type'] = error_by_type
    
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
    
    # 2. Error vs Actual Completion Time - Heatmap Version
    df_viz_sampled = df_viz.copy()
    
    # Calculate hours until actual completion from hours_in_advance
    df_viz_sampled['hours_to_completion'] = df_viz_sampled['hours_in_advance']
    
    # Create bins centered around full hours (-0.5 to +0.5)
    df_viz_sampled['hours_to_completion_rounded'] = np.floor(df_viz_sampled['hours_to_completion'] + 0.5)
    
    # Print error ranges to ensure we're not cutting off data
    print("\nError ranges in data:")
    print(f"Min error days: {df_viz_sampled['error_days'].min():.2f}")
    print(f"Max error days: {df_viz_sampled['error_days'].max():.2f}")
    print(f"Max hours to completion: {df_viz_sampled['hours_to_completion'].max():.2f}")
    
    # Create error day bins centered around full days (-0.5 to +0.5)
    min_error = int(np.floor(df_viz_sampled['error_days'].min())) - 0.5
    max_error = int(np.ceil(df_viz_sampled['error_days'].max())) + 0.5
    
    print(f"\nSetting bin boundaries from {min_error} to {max_error}")
    
    # Create bins and labels
    bins = np.arange(min_error, max_error + 1, 1)
    labels = [f"{i}" if i < 0 else f"+{i}" for i in range(int(min_error + 0.5), int(max_error + 0.5))]
    
    df_viz_sampled['error_day_bin'] = pd.cut(
        df_viz_sampled['error_days'],
        bins=bins,
        labels=labels,
        right=False  # Make bins left-inclusive
    )
    
    # Get the maximum hours to completion (rounded up to nearest day in hours)
    max_hours = int(np.ceil(df_viz_sampled['hours_to_completion'].max() / 24) * 24)
    
    # Create a complete grid of all possible hour and error bin combinations
    hours = np.arange(0, max_hours + 1)  # Include all hours
    error_bins = labels
    
    # Create meshgrid for hours and error bins
    hour_grid, error_grid = np.meshgrid(hours, error_bins)
    
    # Create the base grid DataFrame
    grid_data = pd.DataFrame({
        'hours_to_completion_rounded': hour_grid.ravel(),
        'error_day_bin': error_grid.ravel()
    })
    
    # Calculate counts for each grid cell
    counts = (df_viz_sampled.groupby(['hours_to_completion_rounded', 'error_day_bin'])
             .size()
             .reset_index(name='count'))
    
    # Merge the complete grid with actual counts, filling missing values with 0
    heatmap_data = pd.merge(grid_data, counts, 
                           on=['hours_to_completion_rounded', 'error_day_bin'],
                           how='left').fillna(0)
    
    # Filter out bins with zero counts
    heatmap_data = heatmap_data[heatmap_data['count'] > 0]
    
    # Calculate percentage within each hour (vertical columns sum to 100%)
    total_by_hour = heatmap_data.groupby('hours_to_completion_rounded')['count'].sum().reset_index()
    heatmap_data = heatmap_data.merge(total_by_hour, on='hours_to_completion_rounded', suffixes=('', '_total'))
    heatmap_data['percentage'] = (heatmap_data['count'] / heatmap_data['count_total'] * 100).round(1)
    
    # Create heatmap using rect marks
    error_heatmap = alt.Chart(heatmap_data).mark_rect().encode(
        x=alt.X('hours_to_completion_rounded:O',
                title='Hours Until Actual Completion',
                axis=alt.Axis(
                    grid=True,
                    values=list(range(0, max_hours + 1, 6)),  # Only show labels every 6 hours
                    labelAngle=0,
                    titleFontSize=14
                )),
        y=alt.Y('error_day_bin:O',
                title='Error in Days (Actual - Estimated)',
                axis=alt.Axis(
                    titleFontSize=14,
                    grid=True
                ),
                sort=labels),  # Use the dynamically generated labels for sorting
        color=alt.Color('percentage:Q',
                       scale=alt.Scale(
                           scheme='viridis',
                           domain=[0, 100],
                           nice=False
                       ),
                       legend=alt.Legend(
                           title='Percentage of Estimates',
                           format='.1f'
                       )),
        stroke=alt.value('white'),  # Add white borders around cells
        strokeWidth=alt.value(0.5),  # Set border width
        tooltip=[
            alt.Tooltip('hours_to_completion_rounded:Q', title='Hours to Completion', format='.0f'),
            alt.Tooltip('error_day_bin:N', title='Error Range (Days)'),
            alt.Tooltip('count:Q', title='Number of Estimates'),
            alt.Tooltip('percentage:Q', title='Percentage', format='.1f')
        ]
    ).properties(
        title={
            'text': 'Estimation Error Distribution by Time to Completion',
            'subtitle': [
                'Heatmap shows distribution of errors for each hour until completion',
                'Color intensity shows percentage of estimates within each hour (columns sum to 100%)',
                'Bins are centered around full days (±0.5) and hours (±0.5)',
                'Positive error: Actual completion later than estimated',
                'Only bins with actual data are shown'
            ],
            'fontSize': 16
        },
        width=900,
        height=400
    )
    
    error_vs_leadtime_chart = error_heatmap.configure_view(
        strokeWidth=0
    ).configure_axis(
        grid=True,
        gridOpacity=0.2,
        domain=True,
        domainWidth=2,
        tickSize=10,
        gridColor='white',
        gridWidth=1
    )
    
    visualizations['error_vs_leadtime'] = error_vs_leadtime_chart
    
    # Remove the explanatory text since it's now in the subtitle
    visualizations.pop('error_vs_leadtime_notes', None)
    
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
            bin=True,  # Use default binning
            title='Number of Withdrawals Per Day'
        ),
        y=alt.Y('count():Q', title='Frequency (Days)'),
        tooltip=[
            alt.Tooltip('unique_withdrawals:Q', title='Withdrawals per Day', bin=True),
            alt.Tooltip('count():Q', title='Number of Days')
        ]
    ).properties(
        title='Distribution of Daily Withdrawal Processing Counts',
        width=900,
        height=400
    )
    
    visualizations['withdrawal_distribution'] = withdrawal_count_hist
    
    # 8. Individual withdrawal estimates over time - IMPROVED VERSION
    top_tracking_df = analyze_individual_estimates(df)  # Use original df for analysis
    
    # Use a copy to avoid modifying the original
    individual_tracking = top_tracking_df.copy()
    
    # Sample data more aggressively - group by 4-hour windows
    individual_tracking['time_window'] = individual_tracking['time_of_estimate'].dt.floor('4h')
    
    # Calculate trajectory length and number of estimates for each withdrawal
    trajectory_stats = individual_tracking.groupby('withdrawal_id').agg({
        'time_of_estimate': lambda x: (x.max() - x.min()).total_seconds() / 3600,  # Length in hours
        'hours_until_estimated': 'count'  # Number of estimates
    }).reset_index()
    
    # Rename columns for clarity
    trajectory_stats.columns = ['withdrawal_id', 'duration_hours', 'num_estimates']
    
    # Get the top 30 withdrawals with longest trajectories and at least 5 estimates
    top_withdrawal_ids = (trajectory_stats[trajectory_stats['num_estimates'] >= 5]
                         .nlargest(30, 'duration_hours')
                         ['withdrawal_id']
                         .tolist())
    
    individual_tracking = individual_tracking[
        individual_tracking['withdrawal_id'].isin(top_withdrawal_ids)
    ]
    
    # Group by time window
    individual_tracking = individual_tracking.groupby(['withdrawal_id', 'time_window']).agg({
        'hours_until_estimated': 'mean',
        'hours_until_actual': 'mean',
        'time_of_estimate': 'first'
    }).reset_index()
    
    # Format datetime columns to ISO format strings
    individual_tracking['time_of_estimate_str'] = individual_tracking['time_of_estimate'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    individual_tracking['withdrawal_id'] = individual_tracking['withdrawal_id'].astype(str)
    individual_tracking['hours_until_estimated'] = individual_tracking['hours_until_estimated'].astype(float)
    individual_tracking['hours_until_actual'] = individual_tracking['hours_until_actual'].astype(float)
    
    # Calculate perfect estimation line data
    max_hours = max(individual_tracking['hours_until_actual'].max(), 
                   individual_tracking['hours_until_estimated'].max())
    perfect_line_data = pd.DataFrame({
        'x': [0, max_hours],
        'y': [0, max_hours]
    })
    
    # Create base chart with perfect estimation line
    perfect_line = alt.Chart(perfect_line_data).mark_line(
        strokeDash=[6, 4],
        color='#666666',
        strokeWidth=1
    ).encode(
        x='x:Q',
        y='y:Q'
    )
    
    # Create main scatter plot with connected lines
    estimates_scatter = alt.Chart(individual_tracking).mark_line(
        point=True,  # Add points at each data point
        strokeWidth=1.5,  # Thinner lines for less visual clutter
        opacity=0.4  # More transparency for better overlap visibility
    ).encode(
        x=alt.X('hours_until_actual:Q',
                title='Hours Until Actual Finalization',
                axis=alt.Axis(titleFontSize=12)),
        y=alt.Y('hours_until_estimated:Q',
                title='Hours Until Estimated Finalization',
                axis=alt.Axis(titleFontSize=12)),
        color=alt.Color('withdrawal_id:N',
                       legend=None),  # Remove legend
        tooltip=[
            alt.Tooltip('withdrawal_id:N', title='Withdrawal ID'),
            alt.Tooltip('hours_until_estimated:Q', title='Estimated Hours', format='.1f'),
            alt.Tooltip('hours_until_actual:Q', title='Actual Hours', format='.1f'),
            alt.Tooltip('time_of_estimate_str:T', title='Time of Estimate')
        ]
    ).properties(
        title={
            'text': 'Individual Withdrawal Estimates vs Actual Time',
            'subtitle': [
                'Shows estimation trajectories for top 30 withdrawals with longest estimation periods (min. 5 estimates)',
                'Points above line: Overestimated time, Points below line: Underestimated time'
            ],
            'fontSize': 14,
            'subtitleFontSize': 12
        },
        width=900,
        height=500
    )
    
    # Combine charts and add configuration
    individual_chart = (perfect_line + estimates_scatter).configure_view(
        strokeWidth=0
    ).configure_axis(
        grid=True,
        gridOpacity=0.2,
        labelFontSize=11,
        titleFontSize=12
    )
    
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
    <title>Lido Withdrawals API - Estimate Accuracy Analysis</title>
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
        .intro {{
            background-color: #f8f9fa;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 30px;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        }}
        .intro h2 {{
            margin-top: 0;
            color: #34495e;
        }}
        .intro p {{
            margin-bottom: 10px;
        }}
        .api-endpoints {{
            background-color: #f1f3f5;
            padding: 15px;
            border-radius: 6px;
            margin: 10px 0;
        }}
        .api-endpoints code {{
            display: block;
            background-color: #fff;
            padding: 8px;
            margin: 5px 0;
            border-radius: 4px;
            font-family: monospace;
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
    </style>
</head>
<body>
    <h1>Lido Withdrawals API - Estimate Accuracy Analysis</h1>
    
    <div class="intro">
        <h2>About This Analysis</h2>
        <p>This analysis evaluates the accuracy of the Lido Withdrawals API's request-time endpoint, which estimates the time to withdrawal finalization for unfinalized stETH redemption requests within the Lido on Ethereum protocol.</p>
        
        <h3>Withdrawals API Overview</h3>
        <p>The Withdrawals API service provides utilities for estimating and tracking withdrawal waiting times in the Lido protocol. It serves two main use cases:</p>
        <ul>
            <li>Pre-request estimation: Users can estimate waiting times before placing withdrawal requests</li>
            <li>Request tracking: Users can monitor estimated waiting times for existing requests</li>
        </ul>
        
        <div class="api-endpoints">
            <h4>Key Endpoints:</h4>
            <code>GET https://wq-api.lido.fi/v2/request-time?ids=1&ids=2</code>
            <p class="note">Calculate time to withdrawal for specific request IDs</p>
            
            <code>GET https://wq-api.lido.fi/v2/request-time/calculate</code>
            <p class="note">Calculate time to withdrawal for current queue</p>
            
            <code>GET https://wq-api.lido.fi/v2/request-time/calculate?amount=32</code>
            <p class="note">Calculate time to withdrawal for specific stETH amount</p>
            
            <h4>Testnet (Holesky):</h4>
            <code>GET https://wq-api-holesky.testnet.fi/v2/request-time?ids=1&ids=2</code>
        </div>
    </div>
    
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

            <div class="stat-card">
                <h3>Processing Patterns</h3>
                <p>Mean Withdrawals per Day: <span class="highlight">{mean_daily:.1f}</span></p>
                <p>Median Withdrawals per Day: <span class="highlight">{median_daily:.1f}</span></p>
                <p>Maximum Withdrawals per Day: <span class="highlight">{max_daily}</span></p>
                <p>Days with Processing: <span class="highlight">{days_processing}</span></p>
                <p class="note">Withdrawals are processed in bulk once per day</p>
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
        days_processing=stats['days_with_processing']
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
    
    lead_time_table += "</table>"
    
    # Visualizations section with Altair charts
    vis_section = """
        <div class="visualizations">
    """
    
    # Add each Altair visualization - prepare chart specs but don't embed yet
    chart_specs = {}
    
    for i, (title, chart) in enumerate(visualizations.items()):
        # Skip the error_by_type chart and notes
        if title.endswith('_notes'):
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
                <h2>{chart_title}</h2>
                <div id="vis{i}" class="vis-container"></div>
                {notes_html}
            </div>
        """
        vis_section += chart_html
        
        # Convert to spec dict
        chart_specs[f"vis{i}"] = chart.to_dict()
    
    vis_section += "</div>"
    
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