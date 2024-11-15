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
from kucoin_websocket_collection import Kucoin_websocket_collection
from kucoin_order_strategy import KucoinStrategyTrader
from kucoin.exceptions import KucoinAPIException

# Configure logging with microsecond precision and function names
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

LOCK_FILE = '/tmp/kucoin_trading_match_ws.lock'

async def main():
    lock_file = acquire_lock()
    try:
        logger.debug("Starting script")
        testing = False  
        directory = '/root/trading_systems/kucoin_dir/new_pair_data_kucoin'
        testing_time_offset = 2
        percent_of_price_buy = 3 # Setting limit order to buy n% of the retrieved price
        percent_of_price_sell = 1.3  # Setting limit order to sell n% of the retrieved price
        max_wait_time_for_execution = 10  # Time to wait for price
        num_buy_order_to_send = 20    # Number of buy orders to send with offset time
        time_offset_ms = 20

        if not testing:
            for filename in os.listdir(directory):
                if filename.endswith(".json"):
                    with open(os.path.join(directory, filename)) as f:
                        new_pair_dict = json.load(f)
                    parse_result = parse_new_pair_dict(new_pair_dict)
                    if not parse_result:
                        continue
                    #resulst from parsing created dictonary to enable trading
                    basecoin, release_date_time, datetime_to_listing_seconds = parse_result

                    if 0 < datetime_to_listing_seconds < 1200:

                        logger.info(f'Detected new pair {new_pair_dict["pair"]} at {new_pair_dict["date_time_string"]}')
                        api_creds_dict = load_credetials()
                        try:
                            # Initialize the trading strategy
                            strategy = KucoinStrategyTrader(
                                api_key=api_creds_dict['api_key'],
                                api_secret=api_creds_dict['api_secret'],
                                api_passphrase=api_creds_dict['api_passphrase']
                            )
                            # initialise price retrival from websocket
                            ws = Kucoin_websocket_collection()
                            ws_match_channel_response = await ws.get_price_websocket_match_level3(basecoin,
                                                                                                max_wait_time=max_wait_time_for_execution,
                                                                                                release_time=release_date_time)
                            ws_price = float(ws_match_channel_response['price'])
                            size , decimal_to_round = order_size_and_rounding(ws_price)
                            limit_buy_price = round(ws_price * percent_of_price_buy, decimal_to_round)
                            limit_sell_price = round(ws_price * percent_of_price_sell, decimal_to_round)

                            # Run the trading strategy
                            await strategy.multiple_buy_order_offset_time(
                                symbol=basecoin +'-USDT',
                                limit_buy_price=limit_buy_price,
                                limit_sell_price=limit_sell_price,
                                size=size,
                                num_orders=num_buy_order_to_send,
                                time_offset_ms=time_offset_ms,
                            )
                        except Exception as e:
                            logger.error(f"Strategy execution error: {str(e)}")
                            traceback.print_exc()
                        finally:
                            await ws.cleanup()
                            await strategy.close_client()

                        logger.debug('Break after detecting pair')
                        break
        else:
            basecoin = 'XRP'  # Test symbol
            release_date_time = datetime.now() + timedelta(seconds=testing_time_offset)
            api_creds_dict = load_credetials()

            try:
                # Initialize the trading strategy
                strategy = KucoinStrategyTrader(
                    api_key=api_creds_dict['api_key'],
                    api_secret=api_creds_dict['api_secret'],
                    api_passphrase=api_creds_dict['api_passphrase']
                )
                # initialise price retrival from websocket
                ws = Kucoin_websocket_collection()
                ws_match_channel_response = await ws.get_price_websocket_match_level3(basecoin,
                                                                                        max_wait_time=max_wait_time_for_execution,
                                                                                        release_time=release_date_time)
                ws_price = float(ws_match_channel_response['price'])
                
                size , decimal_to_round = order_size_and_rounding(ws_price)
                limit_buy_price = round(ws_price * percent_of_price_buy, decimal_to_round)
                limit_sell_price = round(ws_price * percent_of_price_sell, decimal_to_round)


                # Run the trading strategy
                await strategy.multiple_buy_order_offset_time(
                    symbol=basecoin +'-USDT',
                    limit_buy_price=limit_buy_price,
                    limit_sell_price=limit_sell_price,
                    size=size,
                    num_orders=num_buy_order_to_send,
                    time_offset_ms=time_offset_ms,
                )

            except Exception as e:
                logger.error(f"Strategy execution error: {str(e)}")
                traceback.print_exc()
            finally:
                await ws.cleanup()
                await strategy.close_client()

            
    finally:
        release_lock(lock_file)
        print(f'{datetime.now()} script finished')
        print('======================================V2')


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

def order_size_and_rounding(token_price):
    """Determine order size and decimal rounding based on token price."""
    size = ''
    decimal_to_round = 0
    token_price = float(token_price)
    if token_price < 0.000009:
        decimal_to_round = 9
        size = '1000100'
    elif token_price < 0.00009:
        decimal_to_round = 8
        size = '100100'
    elif token_price < 0.0009:
        decimal_to_round = 7
        size = '10100'
    elif token_price < 0.009:
        decimal_to_round = 6
        size = '1010'
    elif token_price < 0.09:
        decimal_to_round = 5
        size = '110'
    elif token_price < 0.9:
        decimal_to_round = 4
        size = '11'
    elif token_price < 9:
        decimal_to_round = 2
        size = '1'
    else:
        decimal_to_round = 1
        size = '1'
    return size, decimal_to_round

if __name__ == '__main__':
    asyncio.run(main())