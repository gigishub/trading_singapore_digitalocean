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
from typing import Optional

from kucoin_websocket_collection import Kucoin_websocket_collection
from hf_kucoin_order import KucoinHFTrading
from kucoin.exceptions import KucoinAPIException

# Configure logging with microsecond precision and function names
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

LOCK_FILE = '/tmp/kucoin_trading_level2_ws.lock'


async def main():
    lock_file = acquire_lock()
    try:
        logger.debug("Starting script")
        directory = '/root/trading_systems/kucoin_dir/new_pair_data_kucoin'
        testing = False
        testing_time = 2
        adjust_ask_for_limit = 1.3 # if ask price is received Setting limit order to buy n% of the actual ask price
        adjust_bid_for_limt = 1.3 # if buy price is received Setting limit order to buy n% of the actual bid price

        percent_of_price_sell = 0.8  # Setting limit order to sell n% of the limit order buy price the price i set to execute at 
        max_wait_time_for_execution = 10  # Time to wait for price

        if testing:
            symbol = 'SWELL'
            basecoin = symbol
            release_date_time = datetime.now() + timedelta(seconds=testing_time)
            client = initialize_client()
            if client:
                await execute_trading_strategy(
                    client, basecoin, release_date_time, max_wait_time_for_execution,
                    adjust_ask_for_limit, adjust_bid_for_limt, percent_of_price_sell
                )
            else:
                logger.error("Client initialization failed during testing")
        else:
            for filename in os.listdir(directory):
                if filename.endswith(".json"):
                    with open(os.path.join(directory, filename)) as f:
                        new_pair_dict = json.load(f)

                    parse_result = parse_new_pair_dict(new_pair_dict)
                    if not parse_result:
                        continue

                    basecoin, release_date_time, datetime_to_listing_seconds = parse_result

                    if 0 < datetime_to_listing_seconds < 1200:
                        if datetime.now() > release_date_time:
                            logger.info('Release time has passed')
                            break

                        logger.info(f'Detected new pair {new_pair_dict["pair"]} at {new_pair_dict["date_time_string"]}')
                        client = initialize_client()
                        if not client:
                            continue

                        await execute_trading_strategy(
                            client, basecoin, release_date_time, max_wait_time_for_execution,
                            adjust_ask_for_limit, adjust_bid_for_limt, percent_of_price_sell
                        )

                        logger.debug('Break after detecting pair')
                        break
    finally:
        release_lock(lock_file)
        print(f'{datetime.now()} script finished')
        print('======================================')


async def execute_trading_strategy(client, basecoin, release_date_time, max_wait_time_for_execution, adjust_ask_for_limit, adjust_bid_for_limt,percent_of_price_sell):
    """Execute the trading strategy."""
    try:
        scraper = Kucoin_websocket_collection()
        websocket_release_data = await scraper.get_price_websocket_level2(
            symbol=basecoin,
            max_wait_time=2,
            release_time=release_date_time
        )
        ask_price = websocket_release_data['asks']
        if ask_price:
            ask_price = float(websocket_release_data['asks'][0][0])
        bid_price = websocket_release_data['bids']
        if bid_price:
            bid_price = float(websocket_release_data['bids'][0][0])
        logger.debug(f'\n\nAsk price: {ask_price} \nBid price: {bid_price}\n')  

        if ask_price and not bid_price:
            websocket_release_price = ask_price
            percent_of_price_buy = adjust_ask_for_limit
        
        if bid_price and not ask_price:
            websocket_release_price = bid_price
            percent_of_price_buy = adjust_bid_for_limt

        if websocket_release_price and datetime.now() < release_date_time + timedelta(seconds=max_wait_time_for_execution):
            try:
                size, decimal_to_round = order_size_and_rounding(websocket_release_price)
                response_buy_order, limit_order_buy_price = await place_buy_limit_order(
                    websocket_release_price, decimal_to_round, size, client, basecoin, percent_of_price_buy
                )
                if response_buy_order:
                    logger.debug(f'Buy order response: {response_buy_order}')
                    logger.debug(f'Buy order price: {limit_order_buy_price}')
                else:
                    logger.error("Buy order failed.")
                    return
            except Exception as e:
                logger.error(f'Error in buying execution: {e}')
                traceback.print_exc()
                return

            try:
                response_sell_order = await place_sell_limit_order(
                    limit_order_buy_price, size, client, basecoin, percent_of_price_sell, decimal_to_round
                )
                if response_sell_order:
                    logger.debug(f'Sell order response: {response_sell_order}')
                else:
                    logger.error("Sell order failed.")
            except Exception as e:
                logger.error(f'Error in selling execution: {e}')
                traceback.print_exc()
        else:
            logger.info(f'Price retrieved too slow / price: {websocket_release_price}')
    except Exception as e:
        logger.error(f'Error in websocket_release_price: {e}')
        traceback.print_exc()
    finally:
        await scraper.cleanup()

async def place_buy_limit_order(token_price, decimal_to_round, size, client: KucoinHFTrading, basecoin: str, percent_of_price_buy=0.2):
    try:
        errorcount = 0
        price_buy = None

        while True:
            if errorcount > 2:
                logger.error('Error count > 2')
                break
            price_buy = round(token_price * percent_of_price_buy, decimal_to_round)
            logger.info(f'Buying price: {price_buy}')

            try:
                order_response = client.place_order_with_timing(
                    symbol=f'{basecoin}-USDT',
                    side='buy',
                    order_type='limit',
                    size=str(size),
                    price=str(price_buy)
                )
                logger.info(f'Limit buy order price: {price_buy}')

                return order_response, price_buy
            except KucoinAPIException as e:
                logger.error(f"KucoinAPIException occurred: {str(e)}")
                error_code = e.code
                if '400370' in error_code:
                    match = re.search(r"Max\. price: ([0-9]*\.?[0-9]+)", str(e))
                    if match:
                        max_price = float(match.group(1))
                        logger.info(f'Extracted max price: {max_price}')
                        price_buy = max_price
                        logger.info(f'Adjusted price_buy: {price_buy}')
                errorcount += 1
            except Exception as e:
                logger.error(f'Failed to execute buy order: {e}')
                errorcount += 1

    except Exception as e:
        logger.error(f"place_buy_limit_order:\n {e}")
        traceback.print_exc()
    return None, None

async def place_sell_limit_order(token_price, size, client: KucoinHFTrading, basecoin: str, percent_of_price_sell=1, decimal_to_round=4):
    try:
        symbol = f'{basecoin}-USDT'
        price_sell = round(token_price * percent_of_price_sell, decimal_to_round)
        sell_qty = round(float(size), 1)  # Adjust quantity precision if needed

        order_response = client.place_order_with_timing(
            symbol=symbol,
            side='sell',
            order_type='limit',
            size=str(sell_qty),
            price=str(price_sell)
        )
        logger.info(f'Limit sell order price: {price_sell}')

        return order_response

    except KucoinAPIException as e:
        logger.error(f"KucoinAPIException Selling: {str(e)}")
        traceback.print_exc()
    except Exception as e:
        logger.error(f"Error in place_sell_limit_order:\n {e}")
        traceback.print_exc()
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

def initialize_client():
    """Initialize the KuCoin client."""
    try:
        with open('/root/trading_systems/kucoin_dir/config_api.json') as config_file:
            config = json.load(config_file)
        client = KucoinHFTrading(
            api_key=config['api_key'],
            api_secret=config['api_secret'],
            api_passphrase=config['api_passphrase'],
            debug=False
        )
        return client
    except Exception as e:
        logger.error(f"Error initializing KuCoin HF trading class:\n {e}")
        return None



if __name__ == "__main__":
    asyncio.run(main())