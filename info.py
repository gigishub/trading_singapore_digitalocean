import pandas as pd
import json
from datetime import datetime
path_to_folder = '/root/trading_systems/kucoin_dir/kucoin_release_data_initial/2024-11-26_10-00_HSK'
# Load JSON data
with open(path_to_folder+'/HSK_TICKER_DATA.json') as f:
    ticker_data = json.load(f)

with open(path_to_folder+'/HSK_LEVEL1_DATA.json') as f:
    level1_data = json.load(f)

with open(path_to_folder+'/HSK_LEVEL2DEPTH5_DATA.json') as f:
    level2depth5_data = json.load(f)

with open(path_to_folder+'/HSK_MATCH_DATA.json') as f:
    match_data = json.load(f)

# Function to convert time to seconds from release time
def time_to_seconds(time_str, release_time_str):
    time_format = "%H:%M:%S.%f"
    time = datetime.strptime(time_str, time_format)
    release_time = datetime.strptime(release_time_str, time_format)
    return (time - release_time).total_seconds()

# Extract data and create DataFrames
def create_dataframe(data, release_time_str):
    records = []
    for entry in data['data']:
        time_received = entry.get('time_received')
        if time_received:
            seconds = time_to_seconds(time_received, release_time_str)
            records.append({'seconds': seconds, 'time_received': time_received})
    return pd.DataFrame(records)

ticker_df = create_dataframe(ticker_data, ticker_data['metadata']['release_time'])
level1_df = create_dataframe(level1_data, level1_data['metadata']['release_time'])
level2depth5_df = create_dataframe(level2depth5_data, level2depth5_data['metadata']['release_time'])
match_df = create_dataframe(match_data, match_data['metadata']['release_time'])

# Merge DataFrames on 'seconds' column
merged_df = ticker_df.merge(level1_df, on='seconds', suffixes=('_ticker', '_level1'))
merged_df = merged_df.merge(level2depth5_df, on='seconds', suffixes=('', '_level2depth5'))
merged_df = merged_df.merge(match_df, on='seconds', suffixes=('', '_match'))

# Set 'seconds' as the index
merged_df.set_index('seconds', inplace=True)

# Display the merged DataFrame
print(merged_df)