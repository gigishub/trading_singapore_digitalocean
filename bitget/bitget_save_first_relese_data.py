import asyncio
import sys
import os
import fcntl  # For file locking
import logging
import json
import traceback
from datetime import datetime, timedelta

from bitget_websocket_V3 import BitgetWebsocketListen

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

# Bitget retrieve data and save new
#1,16,31,46 * * * * /root/trading_systems/tradingvenv/bin/python /root/trading_systems/bitget/bitget_save_first_relese_data.py >> /root/trading_systems/bitget/cronlogs/bitget_save_first_relese_data.log 2>&1

LOCK_FILE = '/tmp/bitget_save_first_release_data.lock'

async def main():
    lock_file = acquire_lock()
    try:
        logger.debug("Starting script")
        testing = False  
        directory = '/root/trading_systems/bitget/new_pair_data_bitget'
        testing_time_offset = 2  # Time offset for testing
        path_to_save = '/root/trading_systems/bitget/bitget_data_collection_NEW'
        time_span_for_saving = 60  # Time span for saving data after release


        if not testing:
            for filename in os.listdir(directory):
                if filename.endswith(".json"):
                    with open(os.path.join(directory, filename)) as f:
                        new_pair_dict = json.load(f)
                    parse_result = parse_new_pair_dict(new_pair_dict)
                    if not parse_result:
                        continue
                    basecoin, release_date_time, datetime_to_listing_seconds = parse_result

                    if 0 < datetime_to_listing_seconds < 1200:
                        logger.info(f'release time in {datetime_to_listing_seconds} seconds sleeping{datetime_to_listing_seconds-30}')
                        await asyncio.sleep(datetime_to_listing_seconds-30)
                        logger.info(f'Starting data collection for {basecoin} releasing at {release_date_time}')
                        datetime_to_listing_seconds = (release_date_time - datetime.now()).total_seconds()
                        
                        # Initialize websocket channels     
                        logger.info('initiating websocket')
                        ws_trade = BitgetWebsocketListen(basecoin, channel='trade')
                        ws_ticker = BitgetWebsocketListen(basecoin, channel='ticker')
                        ws_depth = BitgetWebsocketListen(basecoin, channel='books5')
                        
                        try:
                            # Start listening to all channels
                            listen_to_all_task = [
                                asyncio.create_task(ws_trade.start_websocket()),
                                asyncio.create_task(ws_ticker.start_websocket()),
                                asyncio.create_task(ws_depth.start_websocket())
                            ]

                            process_data_task = [
                                asyncio.create_task(ws_trade.process_for_saving()),
                                asyncio.create_task(ws_ticker.process_for_saving()),
                                asyncio.create_task(ws_depth.process_for_saving())
                            ]

                            logger.info('save data until 60 sec after release')
                            delay = (release_date_time + timedelta(seconds=time_span_for_saving) - datetime.now()).total_seconds()
                            await asyncio.sleep(delay)

                            # Cancel all tasks
                            for task in listen_to_all_task + process_data_task:
                                task.cancel()

                        except asyncio.CancelledError:
                            logger.info("Main task cancelled")
                        finally:
                            # Save data
                            await asyncio.gather(
                                ws_trade.save_data(path_to_save, release_date_time),
                                ws_ticker.save_data(path_to_save, release_date_time),
                                ws_depth.save_data(path_to_save, release_date_time)
                            )

                            # Cleanup
                            await asyncio.gather(
                                ws_trade.cleanup(), 
                                ws_ticker.cleanup(), 
                                ws_depth.cleanup()
                            )
                            
                            logger.debug('Break after detecting pair')
                            break

        else:
            # Testing mode
            basecoin = 'SWELL'  # Test symbol
            release_date_time = datetime.now() + timedelta(seconds=testing_time_offset)
            release_date_time = release_date_time.replace(microsecond=0)
            api_creds_dict = load_credentials()
            logger.info(f'Testing mode: {basecoin} at {release_date_time}')
            logger.info('initiating websocket')

            # Initialize websocket channels     
            ws_trade = BitgetWebsocketListen(basecoin, channel='trade')
            ws_ticker = BitgetWebsocketListen(basecoin, channel='ticker')
            ws_depth = BitgetWebsocketListen(basecoin, channel='books5')

            try:
                # Start listening to all channels
                listen_to_all_task = [
                    asyncio.create_task(ws_trade.start_websocket()),
                    asyncio.create_task(ws_ticker.start_websocket()),
                    asyncio.create_task(ws_depth.start_websocket())
                ]

                process_data_task = [
                    asyncio.create_task(ws_trade.process_for_saving()),
                    asyncio.create_task(ws_ticker.process_for_saving()),
                    asyncio.create_task(ws_depth.process_for_saving())
                ]

                logger.info('save data until 60 sec after release')
                delay = (release_date_time + timedelta(seconds=time_span_for_saving) - datetime.now()).total_seconds()
                await asyncio.sleep(delay)

                # Cancel all tasks
                for task in listen_to_all_task + process_data_task:
                    task.cancel()

            except asyncio.CancelledError:
                logger.info("Main task cancelled")
            finally:
                # Save data
                await asyncio.gather(
                    ws_trade.save_data(path_to_save, release_date_time),
                    ws_ticker.save_data(path_to_save, release_date_time),
                    ws_depth.save_data(path_to_save, release_date_time)
                )

                # Cleanup
                await asyncio.gather(
                    ws_trade.cleanup(), 
                    ws_ticker.cleanup(), 
                    ws_depth.cleanup()
                )

    finally:
        release_lock(lock_file)
        print(f'{datetime.now()} script finished V1')

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

def load_credentials():
    """Load API credentials."""
    try:
        with open('/root/trading_systems/bitget/config_api.json') as config_file:
            config = json.load(config_file)
        return config
    except Exception as e:
        logger.error(f"Loading credentials:\n {e}")
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