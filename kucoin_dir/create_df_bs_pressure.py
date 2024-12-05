import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta

def parse_order_book_data(file_path):
    """
    Parse order book data from the given JSON file.
    """
    data_list = []
    with open(file_path, 'r') as f:
        data = json.load(f)

    # Extract the 'data' array
    entries = data['data']

    for entry in entries:
        timestamp = entry['time_received']  # Assuming this is the timestamp
        # Convert timestamp string to datetime object
        timestamp = datetime.strptime(timestamp, '%H:%M:%S.%f')

        # Extract bids and asks
        bids = entry.get('bids', [])  # Each bid is [price, size]
        asks = entry.get('asks', [])  # Each ask is [price, size]

        data_list.append({
            'timestamp': timestamp,
            'bids': bids,
            'asks': asks
        })

    # Create DataFrame
    order_book_df = pd.DataFrame(data_list)
    return order_book_df

def calculate_order_book_metrics(order_book_df, N):
    """
    Calculate bid pressure, ask pressure, and imbalance for the order book.
    """
    metrics_list = []

    for index, row in order_book_df.iterrows():
        timestamp = row['timestamp']
        bids = row['bids']
        asks = row['asks']

        # Convert bids and asks to DataFrames
        bids_df = pd.DataFrame(bids, columns=['price', 'size'])
        asks_df = pd.DataFrame(asks, columns=['price', 'size'])

        # Ensure 'size' is numeric
        bids_df['size'] = bids_df['size'].astype(float)
        asks_df['size'] = asks_df['size'].astype(float)

        # Sort bids descending by price and get top N
        bids_df = bids_df.sort_values(by='price', ascending=False).head(N)

        # Sort asks ascending by price and get top N
        asks_df = asks_df.sort_values(by='price', ascending=True).head(N)

        # Calculate bid pressure and ask pressure
        bid_pressure = bids_df['size'].sum()
        ask_pressure = asks_df['size'].sum()

        # Calculate imbalance
        total_pressure = bid_pressure + ask_pressure
        if total_pressure != 0:
            imbalance = (bid_pressure - ask_pressure) / total_pressure
        else:
            imbalance = np.nan

        metrics_list.append({
            'timestamp': timestamp,
            'bid_pressure': bid_pressure,
            'ask_pressure': ask_pressure,
            'imbalance': imbalance
        })

    # Create DataFrame with metrics
    metrics_df = pd.DataFrame(metrics_list)
    return metrics_df

def parse_match_data(file_path):
    """
    Parse match data from the given JSON file.
    """
    data_list = []
    with open(file_path, 'r') as f:
        data = json.load(f)

    # Extract the 'data' array
    entries = data['data']

    for entry in entries:
        timestamp = entry['time_received']  # Assuming this is the timestamp
        # Convert timestamp string to datetime object
        timestamp = datetime.strptime(timestamp, '%H:%M:%S.%f')

        price = float(entry['price'])
        size = float(entry['size'])
        side = entry['side']

        data_list.append({
            'timestamp': timestamp,
            'price': price,
            'size': size,
            'side': side
        })

    # Create DataFrame
    match_data_df = pd.DataFrame(data_list)
    return match_data_df

def calculate_match_data_metrics(match_data_df, interval_ms):
    """
    Calculate trade volume and trade speed for the match data.
    """
    # Set timestamp as index
    match_data_df = match_data_df.set_index('timestamp')

    # Resample data into intervals
    resampled_df = match_data_df.resample(f'{interval_ms}L').agg({
        'size': 'sum',       # Total trade volume
        'price': 'mean',     # Average price (optional)
        'side': 'count'      # Number of trades
    }).rename(columns={'size': 'trade_volume', 'side': 'trade_speed'})

    resampled_df = resampled_df.reset_index()
    return resampled_df

def synchronize_data(order_book_metrics_df, match_metrics_df):
    """
    Merge order book metrics and match data metrics on timestamp.
    """
    # Merge the DataFrames on timestamp
    merged_df = pd.merge_asof(
        order_book_metrics_df.sort_values('timestamp'),
        match_metrics_df.sort_values('timestamp'),
        on='timestamp',
        direction='nearest',
        tolerance=timedelta(milliseconds=50)
    )
    return merged_df

def main():
    # Parameters
    N = 5                # Number of top bids/asks to consider
    interval_ms = 100    # Interval in milliseconds

    # File paths
    order_book_file = '/root/trading_systems/kucoin_dir/kucoin_release_data_initial/2024-12-02_10-00_QUILL/QUILL_level2Depth5_data.json'
    match_data_file = '/root/trading_systems/kucoin_dir/kucoin_release_data_initial/2024-12-02_10-00_QUILL/QUILL_match_data.json'

    # Parse data
    order_book_df = parse_order_book_data(order_book_file)
    match_data_df = parse_match_data(match_data_file)

    # Calculate metrics
    order_book_metrics_df = calculate_order_book_metrics(order_book_df, N)
    match_metrics_df = calculate_match_data_metrics(match_data_df, interval_ms)

    # Synchronize data
    final_df = synchronize_data(order_book_metrics_df, match_metrics_df)

    # Output final DataFrame
    print(final_df)

if __name__ == '__main__':
    main()