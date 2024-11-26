import aiohttp
import asyncio
import time
import logging
import orjson
from datetime import datetime
from typing import Optional
from datetime import timedelta
import traceback
from bitget_websocket_V2 import Bitget_websocket_collection
import sys
import os
import fcntl  # Import fcntl for file locking
import json

#*/1 * * * * /root/trading_systems/tradingvenv/bin/python /root/trading_systems/bitget/bitget_ws_trade.py >> /root/trading_systems/bitget/cronlogs/bitget_ws_trade.log 2>&1

# Configure logging with microsecond precision and function names
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

LOCK_FILE = '/tmp/bitget_ws_trade.lock'

async def main():


    lock_file = acquire_lock()
    try:
        logger.info("Starting script")

        directory = '/root/trading_systems/bitget/new_pair_data_bitget'
        testing = True
        testing_time = 2
        if testing:
            symbol = 'SWELL'
            scraper = Bitget_websocket_collection()
            release_date_time = datetime.now() + timedelta(seconds=testing_time)  # Example release time

            try:
                change_data = await scraper.get_price_by_release_time_trade(symbol, max_wait_time=10, release_time=release_date_time)
                if change_data:
                    print(f"Successfully retrieved {symbol} price: {change_data}")
            finally:
                await scraper.cleanup()

        ##################################################################
        # Testing with dictionary and retrieved time
        ###################################################################


        else:

            # Loop through announced pairs 
            for filename in os.listdir(directory):
                if filename.endswith(".json"):
                    with open(os.path.join(directory, filename)) as f:
                        new_pair_dict = json.load(f)
                        #logger.debug(f'loaded {filename}')
                
                try:
                    if new_pair_dict['pair']:
                        basecoin = new_pair_dict['pair'].split('USDT')[0]

                        # get the time remaining to listing in secodns 
                        # !!!can be combined to just input dicted and output remaining seconds!!!
                    
                        date_time_dict = parse_date_time_string(new_pair_dict['date_time_string'])
                        #datetime_to_listing_seconds = time_until_listing_seconds(date_time_dict)
                        release_date_time_str  = date_time_dict['formatted_string'] 
                        release_date_time = datetime.strptime(release_date_time_str, '%Y-%m-%d %H:%M:%S') 
                        datetime_to_listing_seconds = (release_date_time - datetime.now()).total_seconds()

                except Exception as e:
                    print(f"Error when parsing date time string:\n {e}")
                    traceback.print_exc()
                    continue


                # Check if listing is close to start
                if 0 < datetime_to_listing_seconds < 1200 :
                    logger.info(f'detected new pair {new_pair_dict["pair"]} at {new_pair_dict["date_time_string"]}')

                    try:
                        # create object to retrive price data
                        symbol = basecoin
                        websocket_object = Bitget_websocket_collection()

                        # returns the highest price at realease time
                        websocket_best_bid_price = await websocket_object.get_price_by_release_time_trade(symbol, max_wait_time=2, release_time=release_date_time)
                        logger.info(f'result: {websocket_best_bid_price}')


                    except Exception as e:
                        logger.error(f'when logging final information:\n {e}')
                    finally:
                        await websocket_object.cleanup()
                    logger.info('breaking loop after finding pair ')
                    break

    finally:
        release_lock(lock_file)
        print(f'{datetime.now()} script finished')
        print('======================================')



def acquire_lock():
    """Acquire a file lock to prevent multiple instances from running."""
    try:
        lock_file = open(LOCK_FILE, 'w')
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_file.write(str(os.getpid()))
        lock_file.flush()
        logger.info("Lock acquired.")
        return lock_file
    
    except BlockingIOError:
        logger.warning("Another instance is running. Exiting.")
        sys.exit(0)
    except Exception as e:
        logger.warning(f"Unexpected error acquiring lock: {e}")
        sys.exit(1)


def release_lock(lock_file):
    """Release the file lock."""
    try:
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()
        logger.info("Lock released.")
    except Exception as e:
        logger.error(f"Unexpected error releasing lock: {e}")


def parse_date_time_string(date_time_string):
    """
    Parses a date-time string into a dictionary containing its components.

    The function preprocesses the input string to remove commas and extra spaces.
    It then attempts to parse the string using a simplified list of datetime formats.
    If successful, it returns a dictionary with date-time components.

    Args:
        date_time_string (str): The date-time string to parse.

    Returns:
        dict: A dictionary containing the parsed date-time components or an error message.
    """
    import re
    from datetime import datetime

    # Preprocess the input string: remove commas and extra spaces
    date_time_string = date_time_string.replace(',', '')
    date_time_string = re.sub(r'\s+', ' ', date_time_string.strip())

    # List of possible datetime formats
    formats = [
        "%b %d %Y %I:%M:%S%p",  # e.g., "Jan 21 2027 12:09:09PM"
        "%b %d %Y %I:%M%p",     # e.g., "Jan 1 2027 12:09PM"
        "%b %d %Y %I%p",        # e.g., "Jan 1 2027 12PM"
        "%B %d %Y %I:%M:%S%p",  # e.g., "January 21 2027 12:09:09PM"
        "%B %d %Y %I:%M%p",     # e.g., "January 1 2027 12:09PM"
        "%B %d %Y %I%p",        # e.g., "January 1 2027 12PM"
    ]

    for fmt in formats:
        try:
            target_datetime = datetime.strptime(date_time_string, fmt)
            # Create the result dictionary
            result = {
                'year': target_datetime.year,
                'month': target_datetime.month,
                'day': target_datetime.day,
                'hour': target_datetime.hour,
                'minute': target_datetime.minute,
                'second': target_datetime.second,
                'day_of_week': target_datetime.strftime('%a').lower(),
                'formatted_string': target_datetime.strftime('%Y-%m-%d %H:%M:%S')
            }
            return result
        except ValueError:
            continue

    # If none of the formats work, return an error message
    return {'error': f"Date time string '{date_time_string}' does not match any known formats"}


if __name__ == "__main__":
    asyncio.run(main())