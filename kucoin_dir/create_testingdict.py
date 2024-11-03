import json
from datetime import datetime, timedelta
import os
# Base data template
base_data = {
    "day_of_week": "fri",
    "hour": 10,
    "minute": 0,
    "second": 0,
    "url": "https://announcements.bybit.com/en-US/article/new-listing-mcg-usdt-grab-a-share-of-the-26-000-000-mcg-prize-pool-bltc105113c043f09d5/",
    "date_time_string": "Aug 2, 2024, 8:30PM",
    "pair": "XRPUSDT",
    "published_datetime": [
        2024,
        6,
        28,
        1,
        27,
        45
    ],    
    "min_buying_qty": 4,
    "buying_qty": 5
}

# Starting time
start_time = datetime.strptime("Nov 2, 2024, 3:00PM", "%b %d, %Y, %I:%M%p")
# INITALIZE COUNTER
minute_counter = 0

# Increment time by minutes
minute_increment = 15

# Number of files to generate
amount_of_files = 10


# Generate 15 JSON files
for i in range(amount_of_files):
    add_minutes = timedelta(minutes=minute_counter)
    # Increment time by i minutes
    current_time = start_time + add_minutes
    # Update the date_time_string in the base data
    base_data["date_time_string"] = current_time.strftime("%b %d, %Y, %I:%M%p")
    
    os.makedirs("/root/trading_systems/new_pair_data_kucoin", exist_ok=True)
    # Save to a new JSON file
    filename = os.path.join("/root/trading_systems/new_pair_data_kucoin", f"file_{i+1}.json")
    with open(filename, 'w') as json_file:
        json.dump(base_data, json_file, indent=4)
    minute_counter += minute_increment

print(f"{amount_of_files} JSON files have been created.")