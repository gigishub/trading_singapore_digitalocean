import aiohttp
import asyncio
import time
import logging
import orjson
from datetime import datetime
from typing import Optional
from datetime import timedelta
import traceback
from kucoin_websocket_collectionV2 import KucoinWebsocketCollection
import sys
import os
import fcntl  # Import fcntl for file locking
import json

# */15 * * * * /root/trading_systems/tradingvenv/bin/python /root/trading_systems/kucoin_dir/kucoin_testing_level2_ws.py >> /root/trading_systems/kucoin_dir/cronlogs/kucoin_testing_level2_ws.log 2>&1

# Create a dedicated logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(message)s ', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

LOCK_FILE = '/tmp/kucoin_testing_ALL_channels.lock'
async def main():
    lock_file = acquire_lock()
    try:

        directory = '/root/trading_systems/kucoin_dir/new_pair_data_kucoin'
        testing = False
        
        if testing:
            symbol = 'SWELL'
            release_time = datetime.now() + timedelta(seconds=3)
            release_time = release_time.replace(microsecond=0)

            # Channels to use
            channel_names = ['ticker', 'level2', 'match', 'depth5']
            scrapers = []
            tasks = []

            # Create a new scraper object for each channel
            for channel_name in channel_names:
                scraper = KucoinWebsocketCollection()
                scrapers.append(scraper)
                channel = getattr(scraper, f'CHANNEL_{channel_name.upper()}')
                tasks.append(
                    scraper.get_price_at_release(
                        symbol=symbol,
                        release_time=release_time,
                        channel=channel
                    )
                )

            try:
                # Run all tasks concurrently
                results = await asyncio.gather(*tasks)

                # Print results with visual separation
                for channel_name, result in zip(channel_names, results):
                    logger.info(f"=" * 65)
                    logger.info(f"Results from channel: {channel_name}")
                    if result:
                        logger.info(json.dumps(result, indent=2))
                    else:
                        logger.info("No data retrieved.")
                logger.info("=" * 65)
            finally:
                # Clean up each scraper individually
                await asyncio.gather(*(scraper.cleanup() for scraper in scrapers))

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

                    # Channels to use
                    channel_names = ['ticker', 'level2', 'match', 'depth5']
                    scrapers = []
                    tasks = []

                    # Create a new scraper object for each channel
                    for channel_name in channel_names:
                        scraper = KucoinWebsocketCollection()
                        scrapers.append(scraper)
                        channel = getattr(scraper, f'CHANNEL_{channel_name.upper()}')
                        tasks.append(
                            scraper.get_price_at_release(
                                symbol=symbol,
                                release_time=release_date_time,
                                channel=channel,
                                max_wait_time=5
                            )
                        )

                    try:
                        # Run all tasks concurrently
                        results = await asyncio.gather(*tasks)

                        # Print results with visual separation
                        for channel_name, result in zip(channel_names, results):
                            logger.info(f"=" * 65)
                            logger.info(f"Results from channel: {channel_name}")
                            if result:
                                logger.info(json.dumps(result, indent=2))
                            else:
                                logger.info("No data retrieved.")
                        logger.info("=" * 65)
                    finally:
                        # Clean up each scraper individually
                        await asyncio.gather(*(scraper.cleanup() for scraper in scrapers))

                    # Break after detecting pair and wait for next cronjob
                    logger.debug('Break after detecting pair loop')
                    break

    finally:
        release_lock(lock_file)
        print(f'{datetime.now()} script finished')

def acquire_lock():
    """Acquire a file lock to prevent multiple instances from running."""
    try:
        lock_file = open(LOCK_FILE, 'w')
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_file.write(str(os.getpid()))
        lock_file.flush()
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