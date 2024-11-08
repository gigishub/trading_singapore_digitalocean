import os
import time
import hmac
import hashlib
import base64
import json
import logging
import asyncio
import aiohttp
from datetime import datetime, timedelta
import traceback
from bitget_websocket_class import BitgetWebSocketScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) 

async def main():
    directory = '/root/trading_systems/bitget/new_pair_data_bitget'
    percent_of_price_buy = 1.10 # setting limit order to buy n% above the retrived price
    percent_of_price_sell = 0.91 # setting limit order to sell n% above the Current price

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
                # API credentials from environment variables
                with open('/root/trading_systems/bitget/api_creds.json', 'r') as file:
                    api_creds = json.load(file)

                api_key = api_creds['api_key']
                secret_key = api_creds['secret_key']
                passphrase = api_creds['passphrase']
                # API endpoint
                base_url = 'https://api.bitget.com'
            except Exception as e:
                logger.error(f'when loading API credentials:\n {e}')
            
            try:

                try:
                    # create object to retrive price data
                    symbol = basecoin
                    websocket_object = BitgetWebSocketScraper()
                    timing_formart = '%H:%M:%S.%f'
                    websocket_price_release = await websocket_object.get_price_by_release_time(symbol, max_wait_time=2, release_time=release_date_time)
                    price_first_retrived_time = datetime.now().strftime(timing_formart)
                    logger.debug(f'Price retrived after release: {websocket_price_release}')

                except Exception as e:
                    logger.error(f'when using websocketclass:\n {e}')
                    traceback.print_exc()

                try:
                    if websocket_price_release:
                        buy_qty ,decimal_to_round_prcie,decimal_to_round_qty = order_size_and_rounding(websocket_price_release)
                        buy_price = round(websocket_price_release * percent_of_price_buy, decimal_to_round_prcie)
                        pair = new_pair_dict['pair']
                        async with aiohttp.ClientSession() as session:
                
                            # buy order
                            before_buy_time = datetime.now().strftime(timing_formart)
                            logger.debug(f'before execution of buylimit (order price: {buy_price})')
                            order_response_buy = await place_buy_limit_order(session, base_url, api_key, secret_key, passphrase, pair,buy_price, buy_qty)
                            after_buy_execution_time = datetime.now().strftime(timing_formart)
                            logger.debug(f'buy order: -{order_response_buy[0]["msg"]} -request time: {order_response_buy[0]["requestTime"]} -order ID: {order_response_buy[0]["data"]["orderId"]}')
                            
                            # sell order
                            websocketprice = await websocket_object.get_current_price(symbol)
                            sell_price = round(websocketprice * percent_of_price_sell, decimal_to_round_prcie)
                            
                            
                            sell_qty = round(float(buy_qty)*0.998, decimal_to_round_qty)
                            logger.debug(f'before execution of selllimit (order price: {sell_price})')
                            before_sell_time = datetime.now().strftime(timing_formart)
                            order_response_sell = await place_sell_limit_order(session, base_url, api_key, secret_key, passphrase, pair,sell_price, sell_qty)
                            after_sell_execution_time = datetime.now().strftime(timing_formart)
                            logger.debug(f'sell order: -{order_response_sell[0]["msg"]} -request time: {order_response_sell[0]["requestTime"]} -order ID: {order_response_sell[0]["data"]["orderId"]}')

                except Exception as e:
                    logger.error(f'while initialsing buying session or buying / selling execution :\n {e}')


            finally:
                try:
                    logger.info('--------------------------------------------------------------')
                    logger.info(        'execution finished moving to logging')
                    logger.info('--------------------------------------------------------------')
                    logger.info(f'{price_first_retrived_time[:-3]} time price retrived after release /price: {websocket_price_release}')
                    logger.info(f'{before_buy_time[:-3]} time just before buying / buylimit order price:{buy_price}')
                    logger.info(f'{after_buy_execution_time[:-3]} time after buy execution')
                    logger.info(f'{order_response_buy[1]:.3f} time needed for buy execution')
                    logger.info(f'{before_sell_time[:-3]} time before sell excution /selllimit order prcie: {sell_price}')
                    logger.info(f'{after_sell_execution_time[:-3]} time after sell execution')
                    logger.info(f'{order_response_sell[1]:.3f} time needed for sell execution')
                    logger.info(f'buy order resposne: / -{order_response_buy[0]["msg"]}- / -request time: {order_response_buy[0]["requestTime"]}- / -order ID: {order_response_buy[0]["data"]["orderId"]}')
                    logger.info(f'sell order response: / -{order_response_sell[0]["msg"]}- / -request time: {order_response_sell[0]["requestTime"]}- / -order ID: {order_response_sell[0]["data"]["orderId"]}')

                except Exception as e:
                    logger.error(f'when logging final information:\n {e}')
                await websocket_object.cleanup()


    logger.info('finshed main loop')
            

def parse_date_time_string(date_time_string):
    """
    Parses a date-time string into a dictionary containing its components.

    The function attempts to parse the input string using a list of predefined
    datetime formats. If the string matches one of the formats, it returns a 
    dictionary with the following keys:
    - year: The year as an integer.
    - month: The month as an integer.
    - day: The day as an integer.
    - hour: The hour as an integer.
    - minute: The minute as an integer.
    - second: The second as an integer.
    - day_of_week: The abbreviated day name in lowercase.
    - formatted_string: The date-time formatted as 'YYYY-MM-DD HH:MM:SS'.

    If the string does not match any of the formats, it returns a dictionary 
    with an error message.

    Args:
        date_time_string (str): The date-time string to parse.

    Returns:
        dict: A dictionary containing the parsed date-time components or an 
              error message.
    """
    # List of possible datetime formats
    formats = [
        "%b %d, %Y, %I:%M%p",    # e.g., "Jan 1, 2027, 12:09PM"
        "%b %d, %Y, %I%p",       # e.g., "Jan 1, 2027, 12PM"
        "%b %d %Y, %I:%M%p",     # e.g., "Jan 1 2027, 12:09PM" (missing comma after day)
        "%b %d %Y, %I%p",        # e.g., "Jan 1 2027, 12PM" (missing comma after day)
        "%b %d, %Y %I:%M%p",     # e.g., "Jan 1, 2027 12:09PM" (missing comma before time)
        "%b %d, %Y %I%p",        # e.g., "Jan 1, 2027 12PM" (missing comma before time)
        "%b %d %Y %I:%M%p",      # e.g., "Jan 1 2027 12:09PM" (missing both commas)
        "%b %d %Y %I%p",         # e.g., "Jan 1 2027 12PM" (missing both commas)
        "%B %d, %Y, %I:%M%p",    # e.g., "January 1, 2027, 12:09PM" (full month name)
        "%B %d, %Y, %I%p",       # e.g., "January 1, 2027, 12PM" (full month name)
        "%B %d %Y, %I:%M%p",     # e.g., "January 1 2027, 12:09PM" (full month name, missing comma after day)
        "%B %d %Y, %I%p",        # e.g., "January 1 2027, 12PM" (full month name, missing comma after day)
        "%B %d, %Y %I:%M%p",     # e.g., "January 1, 2027 12:09PM" (full month name, missing comma before time)
        "%B %d, %Y %I%p",        # e.g., "January 1, 2027 12PM" (full month name, missing comma before time)
        "%B %d %Y %I:%M%p",      # e.g., "January 1 2027 12:09PM" (full month name, missing both commas)
        "%B %d %Y %I%p",         # e.g., "January 1 2027 12PM" (full month name, missing both commas)
        "%b %d, %Y, %I:%M:%S%p", # e.g., "Jan 21, 2027, 12:09:09PM"
        "%b %d %Y, %I:%M:%S%p",  # e.g., "Jan 21 2027, 12:09:09PM" (missing comma after day)
        "%b %d, %Y %I:%M:%S%p",  # e.g., "Jan 21, 2027 12:09:09PM" (missing comma before time)
        "%b %d %Y %I:%M:%S%p",   # e.g., "Jan 21 2027 12:09:09PM" (missing both commas)
        "%B %d, %Y, %I:%M:%S%p", # e.g., "January 21, 2027, 12:09:09PM" (full month name)
        "%B %d %Y, %I:%M:%S%p",  # e.g., "January 21 2027, 12:09:09PM" (full month name, missing comma after day)
        "%B %d, %Y %I:%M:%S%p",  # e.g., "January 21, 2027 12:09:09PM" (full month name, missing comma before time)
        "%B %d %Y %I:%M:%S%p"    # e.g., "January 21 2027 12:09:09PM" (full month name, missing both commas)
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
                'day_of_week': target_datetime.strftime('%a').lower(),  # Abbreviated day name
                'formatted_string': target_datetime.strftime('%Y-%m-%d %H:%M:%S')
            }
            return result
        except ValueError:
            continue
    
    # If none of the formats work, return an error message in the dictionary
    return {'error': f"Date time string '{date_time_string}' does not match any known formats"}










def get_signature(secret_key, timestamp, method, request_path, body=''):
    message = f'{timestamp}{method}{request_path}{body}'
    mac = hmac.new(bytes(secret_key, encoding='utf-8'), bytes(message, encoding='utf-8'), digestmod='sha256')
    d = mac.digest()
    return base64.b64encode(d).decode('utf-8')

def get_headers(api_key, passphrase, timestamp, sign):
    headers = {
        'Content-Type': 'application/json',
        'ACCESS-KEY': api_key,
        'ACCESS-SIGN': sign,
        'ACCESS-TIMESTAMP': timestamp,
        'ACCESS-PASSPHRASE': passphrase,
        'locale': 'en-US'
    }
    return headers

async def place_buy_limit_order(session, base_url, api_key, secret_key, passphrase, pair, price, quantity):
    timestamp = str(int(time.time() * 1000))
    method = 'POST'
    request_path = '/api/v2/spot/trade/place-order'
    body = {
        'symbol': pair,
        'side': 'buy',       # 'buy' order
        'orderType': 'limit',
        'price': str(price),
        'size': str(quantity),
        'force': 'gtc'      # Good Till Cancelled
    }
    body_json = json.dumps(body)
    sign = get_signature(secret_key, timestamp, method, request_path, body_json)
    headers = get_headers(api_key, passphrase, timestamp, sign)
    url = base_url + request_path
    
    # Measure execution time
    start_time = time.time()
    async with session.post(url, headers=headers, data=body_json) as response:
        end_time = time.time()
        execution_time = end_time - start_time
        logger.debug(f'Buy order placed in {execution_time:.6f} seconds')
        
        if response.status == 200:
            return await response.json(), execution_time
        else:
            logger.error(f'placing buy order: {await response.text()}')
            return None

async def place_sell_limit_order(session, base_url, api_key, secret_key, passphrase, pair, price, quantity):
    timestamp = str(int(time.time() * 1000))
    method = 'POST'
    request_path = '/api/v2/spot/trade/place-order'
    body = {
        'symbol': pair,
        'side': 'sell',       # 'sell' order
        'orderType': 'limit',
        'price': str(price),
        'size': str(quantity),
        'force': 'gtc'      # Good Till Cancelled
    }
    body_json = json.dumps(body)
    sign = get_signature(secret_key, timestamp, method, request_path, body_json)
    headers = get_headers(api_key, passphrase, timestamp, sign)
    url = base_url + request_path
    
    # Measure execution time
    start_time = time.time()
    async with session.post(url, headers=headers, data=body_json) as response:
        end_time = time.time()
        execution_time = end_time - start_time
        logger.debug(f'Sell order placed in {execution_time:.6f} seconds')
        
        if response.status == 200:
            return await response.json(), execution_time
        else:
            logger.error(f'placing sell order: {await response.text()}')
            return None



async def place_limit_order_session(session,base_url, api_key, secret_key, passphrase, pair, side, price, quantity):
    timestamp = str(int(time.time() * 1000))
    method = 'POST'
    request_path = '/api/v2/spot/trade/place-order'
    body = {
        'symbol': pair,
        'side': side,       # 'buy' or 'sell'
        'orderType': 'limit',
        'price': str(price),
        'size': str(quantity),
        'force': 'gtc'      # Good Till Cancelled
    }
    body_json = json.dumps(body)
    sign = get_signature(secret_key, timestamp, method, request_path, body_json)
    headers = get_headers(api_key, passphrase, timestamp, sign)
    url = base_url + request_path
    
    # Measure execution time
    start_time = time.time()
    async with session.post(url, headers=headers, data=body_json) as response:
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f'Order placed in {execution_time:.6f} seconds')
        
        if response.status == 200:
            return await response.json()
        else:
            logger.error(f'Error placing order: {await response.text()}')
            return None


def order_size_and_rounding(token_price):
        # initilazing size 
        size = ''
        dezimal_to_round_price = 0
        if token_price < 0.000009:
            dezimal_to_round_price = 9
            dezimal_to_round_qty = 0
            size = '1000100'
        elif token_price < 0.00009:
            dezimal_to_round_price = 8
            dezimal_to_round_qty = 0
            size = '100100'
        elif token_price < 0.0009:
            dezimal_to_round_price = 7
            dezimal_to_round_qty = 0
            size = '10100'
        elif token_price < 0.009:
            dezimal_to_round_price = 6
            dezimal_to_round_qty = 0
            size = '1010'
        elif token_price < 0.09:
            dezimal_to_round_price = 5
            dezimal_to_round_qty = 1
            size = '110'
        elif token_price < 0.9:
            dezimal_to_round_price = 4
            dezimal_to_round_qty = 2
            size ='11'
        elif token_price < 9:
            dezimal_to_round_price = 2
            dezimal_to_round_qty = 4
            size ='1'


        return size, dezimal_to_round_price , dezimal_to_round_qty


# Example usage
if __name__ == '__main__':
    asyncio.run(main())