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
from bitget_strategy_V2 import BitgetStrategyTrader


# Create a dedicated logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

LOCK_FILE = '/tmp/bitget_trade_ticker_ws.lock'
async def main():
    lock_file = acquire_lock()
    try:
        directory = '/root/trading_systems/bitget/new_pair_data_bitget'
        testing = False
        testing_time = 2
        num_order = 8
        percent_of_buy =  25  # Adjust percentage difference for buy orders
        percent_of_sell = 10  # Adjust percentage difference for sell orders

        if testing:
            symbol = 'SWELL'
            size_for_testing = 100
            api_creds = load_credetials()
            strategy = BitgetStrategyTrader(
                api_creds['api_key'],
                api_creds['api_secret'],
                api_creds['api_passphrase']
            )
            ws = Bitget_websocket_collection()
            release_date_time = datetime.now() + timedelta(seconds=testing_time)  # Example release time

            try:
                ws_data = await ws.get_price_by_release_time_ticker(
                    symbol,
                    max_wait_time=1,
                    release_time=release_date_time
                )
                if ws_data:
                    logger.debug(f"Successfully retrieved {symbol} data: {ws_data}")
                    retrieved_price = ws_data['bidPr'] if ws_data['bidPr'] else ws_data['askPr']
                    logger.debug(f"Retrieved price: {retrieved_price}")

                    # Place multiple buy orders
                    buy_results = await strategy.multiple_buy_orders_percent_dif(
                        symbol=symbol,
                        base_price=retrieved_price,
                        size_for_testing=size_for_testing,
                        num_orders=num_order,
                        percentage_difference=percent_of_buy
                    )
                    logger.debug(f"Buy Order Results:{json.dumps(buy_results, indent=4)}")

                    # Check how many buy orders were successful
                    successful_buy_orders = [order for order in buy_results if order["success"]]
                    num_successful_buy_orders = len(successful_buy_orders)
                    logger.debug(f"Number of successful buy orders: {num_successful_buy_orders}")

                    if num_successful_buy_orders > 0:
                        # Place multiple sell orders based on the number of successful buy orders
                        sell_results = await strategy.multiple_sell_orders_percent_dif(
                            symbol=symbol,
                            base_price=retrieved_price,
                            size_for_testing=size_for_testing,
                            num_orders=num_successful_buy_orders,
                            percentage_difference=percent_of_sell
                        )
                        logger.debug(f"Sell Order Results:{json.dumps(sell_results, indent=4)}")
                    else:
                        logger.debug("No successful buy orders to place sell orders.")
            finally:
                await ws.cleanup()
                await strategy.close_client()

        else:
            # Load API credentials
            api_creds = load_credetials()
            strategy = BitgetStrategyTrader(
                api_creds['api_key'],
                api_creds['api_secret'],
                api_creds['api_passphrase']
            )

            # Loop through announced pairs
            for filename in os.listdir(directory):
                if filename.endswith(".json"):
                    with open(os.path.join(directory, filename)) as f:
                        new_pair_dict = json.load(f)
                        # logger.debug(f'loaded {filename}')

                try:
                    if new_pair_dict['pair']:
                        basecoin = new_pair_dict['pair'].split('USDT')[0]

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
                    logger.info(f"Detected new pair {new_pair_dict['pair']} at {new_pair_dict['date_time_string']}")

                    try:
                        # Create object to retrieve price data
                        symbol = basecoin
                        ws = Bitget_websocket_collection()

                        # Returns the highest price at release time
                        ws_data = await ws.get_price_by_release_time_ticker(
                            symbol,
                            max_wait_time=1,
                            release_time=release_date_time
                        )
                        if ws_data:
                            logger.debug(f"Successfully retrieved {symbol} data: {ws_data}")
                            retrieved_price = ws_data['bidPr'] if ws_data['bidPr'] else ws_data['askPr']
                            logger.debug(f"Retrieved price: {retrieved_price}")

                            # Place multiple buy orders
                            buy_results = await strategy.multiple_buy_orders_percent_dif(
                                symbol=symbol,
                                base_price=retrieved_price,
                                num_orders=num_order,
                                percentage_difference=percent_of_buy
                            )
                            logger.debug(f"Buy Order Results:{json.dumps(buy_results, indent=4)}")

                            # Check how many buy orders were successful
                            successful_buy_orders = [order for order in buy_results if order["success"]]
                            num_successful_buy_orders = len(successful_buy_orders)
                            logger.debug(f"Number of successful buy orders: {num_successful_buy_orders}")

                            if num_successful_buy_orders > 0:
                                # Place multiple sell orders based on the number of successful buy orders
                                #await asyncio.sleep(0.1)  # Add a delay before placing sell orders
                                sell_results = await strategy.multiple_sell_orders_percent_dif(
                                    symbol=symbol,
                                    base_price=retrieved_price,
                                    num_orders=num_successful_buy_orders,
                                    percentage_difference=percent_of_sell
                                )
                                logger.debug(f"Sell Order Results:{json.dumps(sell_results, indent=4)}")
                            else:
                                logger.debug("No successful buy orders to place sell orders.")
                    except Exception as e:
                        logger.error(f"Error during trading execution:\n {e}")
                        traceback.print_exc()

                    finally:
                        await ws.cleanup()
                        await strategy.close_client()
                    logger.info('Breaking loop after processing pair')
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
        #logger.info("Lock acquired.")
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
        #logger.info("Lock released.")
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



def load_credetials():
    """Initialize the KuCoin client."""
    try:
        with open('/root/trading_systems/bitget/api_creds.json') as config_file:
            config = json.load(config_file)

        return config
    except Exception as e:
        logger.error(f"loding credentials:\n {e}")
        return None



if __name__ == "__main__":
    asyncio.run(main())