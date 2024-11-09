import sys
import os
import fcntl  # Import fcntl for file locking
import logging
import asyncio

# Configure logging with microsecond precision and function names
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

LOCK_FILE = '/tmp/kucoin_testing_match_ws.lock'

def acquire_lock():
    """Acquire a file lock to prevent multiple instances from running."""
    try:
        lock_file = open(LOCK_FILE, 'w')
        # Acquire an exclusive non-blocking lock
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_file.write(str(os.getpid()))
        lock_file.flush()
        logger.info("Lock acquired.")
        return lock_file
    except BlockingIOError:
        logger.warning("Another instance is running. Exiting.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error acquiring lock: {e}")
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


async def main():
    # Acquire the lock before importing heavy modules
    lock_file = acquire_lock()
    try:
        import aiohttp
        import asyncio
        import time
        import orjson
        from datetime import datetime, timedelta
        from typing import Optional
        import traceback
        from kucoin_websocket_collection import Kucoin_websocket_collection
        import json

        logger.info("Starting script")

        directory = '/root/trading_systems/kucoin_dir/new_pair_data_kucoin'
        testing = False
        testing_time = 90
        if testing:
            symbol = 'SWELL'
            scraper = Kucoin_websocket_collection()
            release_date_time = datetime.now() + timedelta(seconds=testing_time)  # Example release time

            try:
                change_data = await scraper.get_price_websocket_match_level3(
                    symbol,
                    max_wait_time=10,
                    release_time=release_date_time
                )
                if change_data:
                    logger.info(f"Successfully retrieved {symbol} price: {change_data}")
            finally:
                await scraper.cleanup()

        ##################################################################
        # Testing with dictionary and retrieved time
        ###################################################################

        else:
            for filename in os.listdir(directory):
                if filename.endswith(".json"):
                    with open(os.path.join(directory, filename)) as f:
                        new_pair_dict = json.load(f)

                try:
                    # Create symbol from pair
                    symbol = new_pair_dict['pair'].split('USDT')[0]

                    # Get the time remaining to listing in seconds
                    date_time_dict = parse_date_time_string(new_pair_dict['date_time_string'])
                    if 'error' in date_time_dict:
                        logger.error(date_time_dict['error'])
                        continue

                    release_date_time_str = date_time_dict['formatted_string']
                    release_date_time = datetime.strptime(release_date_time_str, '%Y-%m-%d %H:%M:%S')
                    datetime_to_listing_seconds = (release_date_time - datetime.now()).total_seconds()

                except Exception as e:
                    logger.error(f"Error when parsing date time string:\n {e}")
                    traceback.print_exc()
                    continue

                # Check if listing is close to start
                if 0 < datetime_to_listing_seconds < 1200:
                    try:
                        if datetime.now() > release_date_time:
                            logger.info('Release time has passed')
                            break
                    except Exception as e:
                        logger.error(f"Checking release time:\n {e}")

                    logger.info(f'Detected new pair {new_pair_dict["pair"]} at {new_pair_dict["date_time_string"]}')

                    scraper = Kucoin_websocket_collection()

                    try:
                        change_data = await scraper.get_price_websocket_match_level3(
                            symbol,
                            max_wait_time=10,
                            release_time=release_date_time
                        )
                        if change_data:
                            logger.info(f"Successfully retrieved {symbol} price: {change_data}")
                    finally:
                        await scraper.cleanup()
                    #break if pair has been dettected and wait for next cronjob
                    logger.debug('break for after detecting pair loop')
                    break
        logger.info(f'{datetime.now()} script finished')
    finally:
        release_lock(lock_file)

if __name__ == "__main__":

    # Use asyncio.run() if available, else use event loop
    if sys.version_info >= (3, 7):
        asyncio.run(main())
    else:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())