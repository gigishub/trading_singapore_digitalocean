import json
from datetime import datetime, timedelta
import os
# Base data template
base_data = {
    "exchange": "KuCoin",
    "url": "https://announcements.bybit.com/en-US/article/new-listing-mcg-usdt-grab-a-share-of-the-26-000-000-mcg-prize-pool-bltc105113c043f09d5/",
    "date_time_string": "Aug 2, 2024, 7:30PM",
    "pair": "XRPUSDT",
}

# Starting time
start_time = datetime.strptime("Nov 6, 2024, 10:30AM", "%b %d, %Y, %I:%M%p")
# INITALIZE COUNTER
minute_counter = 0

# Increment time by minutes
minute_increment = 15

# Number of files to generate
amount_of_files = 3

#directory_for_testing_files = "/root/trading_systems/bitget/new_pair_data_bitget"

directory_for_testing_files = "/root/trading_systems/kucoin_dir/new_pair_data_kucoin"

# Generate 15 JSON files
for i in range(amount_of_files):
    add_minutes = timedelta(minutes=minute_counter)
    # Increment time by i minutes
    current_time = start_time + add_minutes
    # Update the date_time_string in the base data
    base_data["date_time_string"] = current_time.strftime("%b %d, %Y, %I:%M%p")
    

    os.makedirs(directory_for_testing_files, exist_ok=True)
    # Save to a new JSON file
    filename = os.path.join(directory_for_testing_files, f"file_{i+1}.json")
    with open(filename, 'w') as json_file:
        json.dump(base_data, json_file, indent=4)
    minute_counter += minute_increment

print(f"{amount_of_files} JSON files have been created.")