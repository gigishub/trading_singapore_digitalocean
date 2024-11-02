import json 
from datetime import datetime
import os
import time

# 17/07/24

def main():

    duration = 20               #minutes
    iterations_per_second = 1   #iterations per second

    #directory_path = '/root/test_data'
    directory_path = '/root/new_pair_data'
    #directory_path = 'new_pair_data'

    json_files = [f for f in os.listdir(directory_path) if f.endswith('.json')]

    for json_file in json_files:
        json_file_path = os.path.join(directory_path, json_file)
        with open(json_file_path, 'r') as f:
            json_data = json.load(f)

        # get the time remaining to listing
        time_to_listing    = get_hours_to_listing(json_file_path)
        
        if time_to_listing > 2:
            print(f"time to listing {json_data['pair']} is more than 2 hours")
        
        if 0.05 < time_to_listing <= 0.25:
            #manually set the time count the time to listing
            #needs to be changed 
            time_to_wait    = get_hours_to_listing(json_file_path)
            print(f"listing close start waiting period {time_to_listing * 3600}")
            time.sleep(time_to_wait * 3600 - 5)

            #call function to collect data 
            get_live_token_info(json_data['pair'], category='spot', duration=duration, iterations_per_second=iterations_per_second, testsession=False)
        elif 0.25 < time_to_listing < 2:
            print(f"time to listing of {json_data['pair']} more 15 minutes but less than 2h")
        
        if time_to_listing > 2:
            print(f"time to list {json_data['pair']}:{time_to_listing} remaining\n") 

    print("loop finished")


def get_live_token_info(pair, category, duration=6, iterations_per_second=1, testsession=False):
    """
    Collects live token information for a specified duration and rate limit.

    Parameters:
    - pair (str): The trading pair to collect information for.
    - category (str): The category of trading (e.g., "spot").
    - duration (int): Duration of data collection in minutes.
    - iterations_per_second (int): Number of iterations allowed per second to adhere to rate limits.
    - testsession (bool): Flag to indicate if this is a test session.

    Returns:
    - None
    """

    import time
    import json
    import os
    import traceback
    from datetime import datetime
    try:
        start_time = time.time()
        session = create_session_bybit(test=testsession)
        info_list = []
        error_list = []

        print('Start info collection!')
        i = 0
        while True:
            rate_limit_requests(iterations_per_second=iterations_per_second)
            try:
                tickerInfo = session.get_tickers(category=category, symbol=pair)
                intrumentInfo = session.get_instruments_info(category=category, symbol=pair)
                orderBook = session.get_orderbook(category=category, symbol=pair)
                info_list.append({
                                i: {
                                    'tickerInfo': tickerInfo,
                                    'intrumentInfo': intrumentInfo,
                                    'orderBook': orderBook,
                                    'timestamp': datetime.now().strftime('%H:%M:%S')
                                    }})

            except Exception as e:
                error_list.append({
                                i: {
                                    'timestamp': datetime.now().strftime('%H:%M:%S'),
                                    'error': str(e),
                                    'traceback': traceback.format_exc()
                                    }})
            if time.time() - start_time > duration * 60:
                print(f'Successfully finished after {duration} minutes')
                break
            i += 1
    finally:
        save_data_from_live_collection(pair, start_time, info_list, error_list)


def save_data_from_live_collection(pair, start_time, info_list, error_list):
    """
    Saves collected data and errors to JSON files.

    Parameters:
    - pair (str): The trading pair information was collected for.
    - start_time (float): The start time of the data collection.
    - info_list (list): List of collected information.
    - error_list (list): List of errors encountered during data collection.

    Returns:
    - None
    """

    parent_dir = 'data_collection'
    os.makedirs(parent_dir, exist_ok=True)

    save_dir_for_pair = os.path.join(parent_dir, f'Info_{pair}_{datetime.fromtimestamp(start_time).strftime("%Y-%m-%d_%H-%M")}')
    if not os.path.exists(save_dir_for_pair):
        os.makedirs(save_dir_for_pair)  # Use makedirs to ensure parent directory is created if it doesn't exist
    with open(os.path.join(save_dir_for_pair, 'info.json'), 'w') as f:
        json.dump(info_list, f)
    with open(os.path.join(save_dir_for_pair, 'errorsCollectinInfo.json'), 'w') as f:
        json.dump(error_list, f)

def get_hours_to_listing (json_file_path):
    """
    Calculates the number of hours from the current time to a listing time specified in a JSON file.

    The function reads a JSON file specified by `json_file_path`, extracts a datetime string under the key
    'date_time_string', and converts it into a datetime object. It supports datetime strings with and without minutes.
    The function then calculates the difference between the current time and the listing time, returning the
    difference in hours.

    Parameters:
    - json_file_path (str): The file path to the JSON file containing the listing time information.

    Returns:
    - float: The number of hours from the current time to the listing time. This value can be negative if the
      listing time is in the past.

    Raises:
    - ValueError: If the datetime string in the JSON file does not match the expected format.
    """

    import json
    from datetime import datetime
    # Load the JSON data from the file
    with open(json_file_path, 'r') as f:
        new_pair_data = json.load(f)

    # Extract the time to execute from the JSON data
    time_to_execute_str = new_pair_data['date_time_string']

    # Convert the string to a datetime object
    try:
        time_to_execute = datetime.strptime(time_to_execute_str, "%b %d, %Y, %I:%M%p")
    except ValueError:
        # If it fails, try parsing without minutes
        time_to_execute = datetime.strptime(time_to_execute_str, "%b %d, %Y, %I%p")

    # Calculate the future time by adding hours to the current time
    time_to_listing = time_to_execute - datetime.now() 
    
    return time_to_listing.total_seconds() / 3600

def rate_limit_requests(iterations_per_second=40):
    import time
    """
    if called pauses the code for as long as the rate limit devided by second need to be 
    Parameters:
    - iterations_per_second: The number of requests allowed per second. Defaults to 40.

    retruns : the amount in seconds that function makes code sleep  
    """

    sleep_time = 1.0 / iterations_per_second  # Calculate sleep time in seconds
    time.sleep(sleep_time)  # Sleep to limit the rate of requests
    return sleep_time

def create_session_bybit(max_retries=100, test=True):
    """
    Creates a session with Bybit's API.

    Parameters:
    max_retries (int, optional): The maximum number of times to retry creating the session if a ReadTimeout occurs. Defaults to 5.
    test (bool, optional): Whether to create a testnet session or a live session. Defaults to True (testnet).

    Returns:
    HTTP: The created session, or None if the session could not be created after max_retries attempts.
    """
    from pybit.unified_trading import HTTP
    from requests.exceptions import ReadTimeout

    # If test is True, create a testnet session
    if test:
        for _ in range(max_retries):
            try:
                # Attempt to create the session
                session = HTTP(testnet=True,
                               api_key="YDpyCNtMWiNfKzvRtf",
                               api_secret="Tan1dxTFiTxTOIKtiHJu0Rub3q0n0JxlsQJ5")
                # If the session is created successfully, break the loop and return the session
                return session
            except ReadTimeout:
                # If a ReadTimeout occurs, print a message and retry
                print("ReadTimeout occurred, retrying...")
        else:
            # If the session could not be created after max_retries attempts, print a message and return None
            print(f"Failed to create session after {max_retries} retries")
            return None
    # If test is False, create a live session
    else:
        for _ in range(max_retries):
            try:
                # Attempt to create the session
                session = HTTP(testnet=False,
                               api_key="lZ9fMZoU1oTR5fWBNP",
                               api_secret="kcI4mAoQhd32gjXMPkEA2cjuyNC0rR0WHrpe")
                # If the session is created successfully, break the loop and return the session
                return session
            except ReadTimeout:
                # If a ReadTimeout occurs, print a message and retry
                print("ReadTimeout occurred, retrying...")
        else:
            # If the session could not be created after max_retries attempts, print a message and return None
            print(f"Failed to create session after {max_retries} retries")
            return None


if __name__ == "__main__":
    main()

