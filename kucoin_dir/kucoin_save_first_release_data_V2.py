import sys
import os
import fcntl  # For file locking
import logging
import asyncio
import aiohttp
import time
import orjson
from datetime import datetime, timedelta
import traceback
import json
import re
from kucoin_save_relase_data_class import Kucoin_save_ws_data

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Check if handlers are already added to the logger
if not logger.handlers:
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s', datefmt='%H:%M:%S')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

logger.propagate = False


# Kucoin retrive TEST New Main
#1,16,31,46 * * * * /root/trading_systems/tradingvenv/bin/python /root/trading_systems/kucoin_dir/kucoin_save_first_release_data_V2.py >> /root/trading_systems/kucoin_dir/cronlogs/kucoin_save_first_release_data_V2.log 2>&1

LOCK_FILE = '/tmp/kucoin_save_first_release_data_V2.lock'
async def main():
    lock_file = acquire_lock()
    try:
        logger.debug("Starting script")
        testing = False  
        directory = '/root/trading_systems/kucoin_dir/new_pair_data_kucoin'
        duration_to_run = 60  # time in minutes
        start_collect_before_release_sec = 30  # Start collecting data before release time

        async def execution(new_pair_dict):
            path_to_save = '/root/trading_systems/kucoin_dir/kucoin_release_data_initial'
            if new_pair_dict['tag'] == 'relisting':
                path_to_save = '/root/trading_systems/kucoin_dir/kucoin_relisting_data_relisting'


            try:
                parse_result = parse_new_pair_dict(new_pair_dict)
                if not parse_result:
                    return
                    
                basecoin, release_date_time, datetime_to_listing_seconds = parse_result
                logger.info(f'Detected new pair {basecoin} at {release_date_time}')
                
                # List all channel attributes from the class
                channels = [
                    Kucoin_save_ws_data.CHANNEL_TICKER,
                    Kucoin_save_ws_data.CHANNEL_LEVEL2,
                    Kucoin_save_ws_data.CHANNEL_MATCH,
                    Kucoin_save_ws_data.CHANNEL_DEPTH5,
                    Kucoin_save_ws_data.CHANNEL_SNAPSHOT,
                    Kucoin_save_ws_data.CHANNEL_LEVEL1
                ]

                channel_tasks = []
                ws_instances = []
                
                for channel in channels:
                    ws = Kucoin_save_ws_data(
                        symbol=basecoin,
                        release_time=release_date_time,
                        duration_minutes=duration_to_run,
                        saving_path=path_to_save,
                        channel=channel,
                        pre_release_seconds=start_collect_before_release_sec
                    )
                    ws_instances.append(ws)
                    try:
                        # Start the websocket connection
                        websocket_task = asyncio.create_task(ws.start_websocket())
                        # Process and save data
                        data_task = asyncio.create_task(ws.process_for_saving())
                        # Wait for both tasks to complete
                        channel_tasks.append(asyncio.gather(websocket_task, data_task))
                    except Exception as e:
                        logger.error(f"Error in collection process for channel {channel}: {e}")
                        
                # Run all channel tasks concurrently for this pair
                await asyncio.gather(*channel_tasks)
                logger.info(f'finished {basecoin}')

                
            except Exception as e:
                logger.error(f"Error processing pair {new_pair_dict.get('pair', 'unknown')}: {e}")
                traceback.print_exc()

        if not testing:
            pairs_to_process = []
            # Collect all eligible pairs
            for filename in os.listdir(directory):
                if filename.endswith(".json"):
                    with open(os.path.join(directory, filename)) as f:
                        new_pair_dict = json.load(f)
                    parse_result = parse_new_pair_dict(new_pair_dict)
                    if not parse_result:
                        continue
                    _, _, datetime_to_listing_seconds = parse_result

                    if 0 < datetime_to_listing_seconds < 1200:
                        pairs_to_process.append(new_pair_dict)
            
            # Create tasks for all pairs
            tasks_to_execute = []
            for new_pair_dict in pairs_to_process:
                tasks_to_execute.append(asyncio.create_task(execution(new_pair_dict)))
            
            # Execute all pair tasks concurrently
            await asyncio.gather(*tasks_to_execute)
            
        else:
            # Testing mode
            basecoin = 'XRP'  # Test symbol
            testing_time_offset = 2  # Time offset for testing
            release_date_time = datetime.now() + timedelta(seconds=testing_time_offset)
            release_date_time = release_date_time.replace(microsecond=0)
            test_pair_dict = {
                "pair": basecoin,
                "date_time_string": release_date_time.isoformat(),
                "tag": "initial_listing"
            }
            await execution(test_pair_dict)

    finally:
        release_lock(lock_file)
        print(f'{datetime.now()} script finished ')



def acquire_lock():
    """Acquire a file lock to prevent multiple instances from running."""
    try:
        lock_file = open(LOCK_FILE, 'w')
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_file.write(str(os.getpid()))
        lock_file.flush()
        logger.debug("Lock acquired.")
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
        logger.debug("Lock released.")
    except Exception as e:
        logger.error(f"Unexpected error releasing lock: {e}")


def parse_new_pair_dict(new_pair_dict):
    """Parse new pair data to extract basecoin and release time."""
    try:
        basecoin = new_pair_dict['pair'].split('USDT')[0]
        date_time_dict = parse_date_time_string(new_pair_dict['date_time_string'])
        if 'error' in date_time_dict:
            logger.error(date_time_dict['error'])
            return None
        release_date_time_str = date_time_dict['formatted_string']
        release_date_time = datetime.strptime(release_date_time_str, '%Y-%m-%d %H:%M:%S')
        datetime_to_listing_seconds = (release_date_time - datetime.now()).total_seconds()
        return basecoin, release_date_time, datetime_to_listing_seconds
    except Exception as e:
        logger.error(f"Error parsing date time string:\n {e}")
        traceback.print_exc()
        return None


def load_credetials():
    """Initialize the KuCoin client."""
    try:
        with open('/root/trading_systems/kucoin_dir/config_api.json') as config_file:
            config = json.load(config_file)

        return config
    except Exception as e:
        logger.error(f"loding credentials:\n {e}")
        return None


def parse_date_time_string(date_time_string):
    """Parse a date-time string into a dictionary."""
    import re
    date_time_string = date_time_string.replace(',', '')
    date_time_string = re.sub(r'\s+', ' ', date_time_string.strip())
    formats = [
        "%b %d %Y %I:%M:%S%p",
        "%b %d %Y %I:%M%p",
        "%b %d %Y %I%p",
        "%B %d %Y %I:%M:%S%p",
        "%B %d %Y %I:%M%p",
        "%B %d %Y %I%p",
    ]

    for fmt in formats:
        try:
            target_datetime = datetime.strptime(date_time_string, fmt)
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
    return {'error': f"Date time string '{date_time_string}' does not match any known formats"}

if __name__ == '__main__':
    asyncio.run(main())