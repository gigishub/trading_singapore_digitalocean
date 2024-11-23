import aiohttp
import asyncio
import time
import logging
import orjson
from datetime import datetime, timedelta
import json
import os
import fcntl
import sys
from typing import List, Any, Dict
from development.kucoin_websocket_collectionV2 import KucoinWebsocketCollection

# Kucoin Retrive data from all channels
#1,16,31,46 * * * * /root/trading_systems/tradingvenv/bin/python /root/trading_systems/kucoin_dir/kucoin_all_channel_data_retrival.py >> /root/trading_systems/kucoin_dir/cronlogs/kucoin_all_channel_data_retrival.log 2>&1

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(message)s ', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

LOCK_FILE = '/tmp/kucoin_testing_ALL_channels.lock'

class KucoinReleaseDataManager:
    def __init__(self, symbol: str, release_time: datetime):
        self.symbol = symbol
        self.release_time = release_time
        self.data_queue = asyncio.Queue()
        self.received_data: Dict[str, List[Any]] = {}
        self.channels = ['ticker', 'level2', 'match', 'level2Depth5']
        self.stop_collection = asyncio.Event()
        
    async def listen_to_updates(self, channel: str):
        """Listen to WebSocket updates for a specific channel"""
        scraper = KucoinWebsocketCollection()
        self.received_data[channel] = []
        
        try:
            success = await scraper.initialize_websocket(self.symbol, channel)
            if not success:
                logger.error(f"Failed to initialize websocket for {channel}")
                return

            while not self.stop_collection.is_set():
                try:
                    msg = await asyncio.wait_for(
                        scraper.ws_connection.receive(),
                        timeout=0.001
                    )

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = orjson.loads(msg.data)
                        
                        
                        # Add timestamps

                        
                        self.received_data[channel].append(data)
                        
                        if data.get('type') == 'message' and 'data' in data:
                            processed_data = data['data']
                            current_time = datetime.now()
                            processed_data['time_before_que.put'] = current_time.strftime('%H:%M:%S.%f')[:-3]
                            processed_data['seconds_from_release'] = (current_time - self.release_time).total_seconds()

                            await self.data_queue.put((channel, processed_data))

                except asyncio.TimeoutError:
                    continue

        except Exception as e:
            logger.error(f"Error in WebSocket listening for {channel}: {e}")
        finally:
            await scraper.cleanup()

    async def process_data(self):
        """Process data from the queue"""
        while not self.stop_collection.is_set():
            try:
                channel, data = await asyncio.wait_for(self.data_queue.get(), timeout=0.001)
                data['time_after_queue.get'] = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                #logger.debug(f"Channel {channel}: {json.dumps(data, indent=2)}")
                self.data_queue.task_done()
            except asyncio.TimeoutError:
                continue

    async def save_data(self):
        """Save collected data to files"""
        saving_dir = f"/root/trading_systems/kucoin_dir/kucoin_data_collection/{self.release_time.strftime('%Y-%m-%d_%H-%M')}_{self.symbol}"
        os.makedirs(saving_dir, exist_ok=True)

        
        for channel, data in self.received_data.items():
            filename = os.path.join(saving_dir, f"{self.symbol}_{channel}_data.json")
            with open(filename, "w") as f:
                json.dump(data, f, indent=2)
        logger.info(f"Data saved to directory: {saving_dir}")

async def main():
    lock_file = acquire_lock()
    try:
        testing = False
        
        if testing:
            # Testing mode with immediate collection
            symbol = 'BTC'
            release_time = datetime.now() + timedelta(seconds=2)  # Start collecting 30 seconds from now
            logger.info(f'Starting test data collection for {symbol} releasing at {release_time}')
            
            data_manager = KucoinReleaseDataManager(symbol, release_time)
            tasks = []

            # Create tasks for each channel
            for channel in data_manager.channels:
                listen_task = asyncio.create_task(
                    data_manager.listen_to_updates(channel)
                )
                tasks.append(listen_task)

            # Add processing task
            process_task = asyncio.create_task(data_manager.process_data())
            tasks.append(process_task)

            try:
                # Collect data for 2 minutes (30 seconds before and 90 seconds after release)
                await asyncio.sleep(10)
                
                # Stop data collection
                data_manager.stop_collection.set()
                await asyncio.gather(*tasks, return_exceptions=True)
                
                # Save collected data
                await data_manager.save_data()
                
            except Exception as e:
                logger.error(f"Error during test data collection: {e}")
            finally:
                # Ensure all tasks are cancelled
                for task in tasks:
                    if not task.done():
                        task.cancel()

        else:
            # Production mode
            directory = '/root/trading_systems/kucoin_dir/new_pair_data_kucoin'
            
            for filename in os.listdir(directory):
                if not filename.endswith(".json"):
                    continue
                    
                with open(os.path.join(directory, filename)) as f:
                    new_pair_dict = json.load(f)

                try:
                    symbol = new_pair_dict['pair'].split('USDT')[0]
                    release_date_time_str = parse_date_time_string(new_pair_dict['date_time_string'])['formatted_string']
                    release_date_time = datetime.strptime(release_date_time_str, '%Y-%m-%d %H:%M:%S')
                    datetime_to_listing_seconds = (release_date_time - datetime.now()).total_seconds()

                except Exception as e:
                    logger.error(f"Error parsing date time: {e}")
                    continue

                # Check if we're within the collection window
                if 0 < datetime_to_listing_seconds < 1200:  # 20 minutes before listing
                    logger.info(f'realease time in {datetime_to_listing_seconds} seconds sleeping{datetime_to_listing_seconds-30}' )
                    await asyncio.sleep(datetime_to_listing_seconds-30)
                    logger.info(f'Starting data collection for {symbol} releasing at {release_date_time}')
                    datetime_to_listing_seconds = (release_date_time - datetime.now()).total_seconds()
                    
                    data_manager = KucoinReleaseDataManager(symbol, release_date_time)
                    tasks = []

                    # Create tasks for each channel
                    for channel in data_manager.channels:
                        listen_task = asyncio.create_task(
                            data_manager.listen_to_updates(channel)
                        )
                        tasks.append(listen_task)

                    # Add processing task
                    process_task = asyncio.create_task(data_manager.process_data())
                    tasks.append(process_task)

                    try:
                        # Wait until 1 minute after release time
                        wait_time = datetime_to_listing_seconds + 60  # 60 seconds after release
                        await asyncio.sleep(wait_time)
                        
                        # Stop data collection
                        data_manager.stop_collection.set()
                        await asyncio.gather(*tasks, return_exceptions=True)
                        
                        # Save collected data
                        await data_manager.save_data()
                        
                    except Exception as e:
                        logger.error(f"Error during data collection: {e}")
                    finally:
                        # Ensure all tasks are cancelled
                        for task in tasks:
                            if not task.done():
                                task.cancel()
                    
                    # Break after handling one release
                    break

    finally:
        release_lock(lock_file)
        logger.info(f'{datetime.now()} script finished')

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
    """
    import re
    from datetime import datetime

    # Preprocess the input string
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

if __name__ == "__main__":
    asyncio.run(main())