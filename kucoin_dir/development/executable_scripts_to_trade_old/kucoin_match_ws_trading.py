import sys
import os
import fcntl  # Import fcntl for file locking
import logging
import asyncio
import aiohttp
import time
import orjson
from datetime import datetime, timedelta
from typing import Optional
import traceback
from kucoin_websocket_collection import Kucoin_websocket_collection
import json
from hf_kucoin_order import KucoinHFTrading
from kucoin.exceptions import KucoinAPIException
import re



# Configure logging with microsecond precision and function names
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

LOCK_FILE = '/tmp/kucoin_match_ws_trading.lock'

async def main():
    lock_file = acquire_lock()
    try:
        logger.info("Starting script")

        directory = '/root/trading_systems/kucoin_dir/new_pair_data_kucoin'
        testing = False
        testing_time = 5
        percent_of_price_buy = 2.5 # setting limit order to buy n% above the retrived price
        percent_of_price_sell = 0.8 # setting limit order to sell n% above the ACTUAL buy price NOT the retrived price
        max_wait_time_for_execution = 1 # time to wait for price if passed no order execution but continues to retrive price


        if not testing:
            for filename in os.listdir(directory):
                if filename.endswith(".json"):
                    with open(os.path.join(directory, filename)) as f:
                        new_pair_dict = json.load(f)

                try:
                    # Create symbol from pair
                    basecoin = new_pair_dict['pair'].split('USDT')[0]
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

                    try:

                        with open('/root/trading_systems/kucoin_dir/config_api.json') as config_file:
                            config = json.load(config_file)

                        client = KucoinHFTrading(
                            api_key=config['api_key'],
                            api_secret=config['api_secret'],
                            api_passphrase=config['api_passphrase'],
                            debug=False  # Set to False for production
                            )
                        #logger.info(f'client initialized')

                    except Exception as e:
                        logger.error(f"when initialising KuCoin HF trading Class:\n {e}")

                    try:
                        symbol = new_pair_dict['pair'].split('USDT')[0]                   
                        scraper = Kucoin_websocket_collection() 
                        websocket_release_data  = await scraper.get_price_websocket_match_level3(
                                                                                        symbol,
                                                                                        max_wait_time=1,
                                                                                        release_time=release_date_time)
                        websocket_release_price = float(websocket_release_data['price'])
                        logger.debug(f'Price retrived: {websocket_release_price}')
                        if websocket_release_price and datetime.now() < release_date_time + timedelta(seconds= max_wait_time_for_execution):
                            #buying execution 
                            try:
                                size, decimal_to_round = order_size_and_rounding(websocket_release_price)
                                response_buy_order = await place_buy_limit_order(websocket_release_price, decimal_to_round, size, client, basecoin, percent_of_price_buy)
                                logger.debug(f'Buy order response: {response_buy_order[0]}')
                                limit_order_buy_price = response_buy_order[1]
                                logger.debug(f'Buy order price: {limit_order_buy_price}')
                            except Exception as e:
                                logger.error(f'Error in buying execution: {e}')
                                traceback.print_exc()

                            try:
                                # using limit order buy price as referce to sell
                                response_sell_order = await place_sell_limit_order(limit_order_buy_price, size, client, basecoin, percent_of_price_sell,decimal_to_round)
                                logger.debug(f'Sell order response: {response_sell_order}')
                            except Exception as e:
                                logger.error(f'Error in selling execution: {e}')
                                traceback.print_exc()
                    
                        else:
                            logger.info(f'Price retrived to slow /price: {websocket_release_price}')
                    except Exception as e:
                        logger.error(f'Error in websocket_release_price: {e}')
                        traceback.print_exc()
                    finally:
                        await scraper.cleanup()
                        logger.debug('break for after detecting pair loop')
                        break

        
        if testing:
            symbol = 'SWELL'
            scraper = Kucoin_websocket_collection()
            release_date_time = datetime.now() + timedelta(seconds=testing_time)  # Example release time

            try:
                change_data = await scraper.get_price_websocket_match_level3(
                    symbol,
                    max_wait_time=40,
                    release_time=release_date_time
                )
                if change_data:
                    logger.info(f"Successfully retrieved {symbol} price: {change_data['price']}")
            except Exception as e:
                logger.error(f"Error in testing: {e}")


    finally:
        release_lock(lock_file)
        print(f'{datetime.now()} script finished')
        print('======================================')



async def place_buy_limit_order(token_price, decimal_to_round,size,client: KucoinHFTrading, basecoin: str,
                                percent_of_price_buy=0.2):
    try:
        errorcount = 0
        price_buy = None

        while True:
            try:
                if errorcount > 2:
                    logger.error('Error count > 2')
                    break
                price_buy = round(token_price * percent_of_price_buy, decimal_to_round)
                logger.info(f'Buying price: {price_buy} at {datetime.now().strftime("%H:%M:%S.%f")[:-3]}')

                # Place the buy limit order using KucoinHFTrading
                order_response = client.place_order_with_timing(
                    symbol=basecoin+'-USDT',
                    side='buy',
                    order_type='limit',
                    size=str(size),
                    price=str(price_buy)
                )

                logger.info(f'Buy order executed at: {datetime.now().strftime("%H:%M:%S.%f")[:-3]}: {order_response}')
                logger.info(f'Limit buy order price: {price_buy}')

                if order_response.get('success'):
                    return order_response , price_buy  # Exit after successful order

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
        logger.error(f" place_buy_limit_order:\n {e}")
        traceback.print_exc()


async def place_sell_limit_order(token_price, size, client: KucoinHFTrading, basecoin: str,
                                 percent_of_price_sell=1,decimal_to_round=4):
    try:
        decimal_to_round = decimal_to_round  # Adjust based on your requirements
        symbol = f'{basecoin}-USDT'
        price_sell = round(token_price * percent_of_price_sell, decimal_to_round)
        sell_qty = round(float(size), 1)  # Adjust quantity precision if needed

        # Place the sell limit order using KucoinHFTrading
        order_response = client.place_order_with_timing(
            symbol=basecoin+'-USDT',
            side='sell',
            order_type='limit',
            size=str(sell_qty),
            price=str(price_sell)
        )

        logger.info(f'Sell order executed at {datetime.now().strftime("%H:%M:%S.%f")[:-3]}: {order_response}')
        logger.info(f'Limit sell order price: {price_sell}')

        return order_response

    except KucoinAPIException as e:
        logger.error(f"KucoinAPIException Selling: {str(e)}")
        traceback.print_exc()
    except Exception as e:
        logger.error(f"Error in place_sell_limit_order:\n {e}")
        traceback.print_exc()



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


def order_size_and_rounding(token_price):
        # initilazing size 
        size = ''
        dezimal_to_round = 0
        if token_price < 0.000009:
            dezimal_to_round = 9
            size = '1000100'
        elif token_price < 0.00009:
            dezimal_to_round = 8
            size = '100100'
        elif token_price < 0.0009:
            dezimal_to_round = 7
            size = '10100'
        elif token_price < 0.009:
            dezimal_to_round = 6
            size = '1010'
        elif token_price < 0.09:
            dezimal_to_round = 5
            size = '110'
        elif token_price < 0.9:
            dezimal_to_round = 4
            size ='11'
        elif token_price < 9:
            dezimal_to_round = 2
            size ='1'

        return size, dezimal_to_round



if __name__ == "__main__":
    asyncio.run(main())