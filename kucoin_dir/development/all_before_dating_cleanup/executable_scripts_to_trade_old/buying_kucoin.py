import time
import re
import os 
import json 
import traceback
from kucoin.client import Client
from datetime import datetime, timedelta
import asyncio
from kucoin.exceptions import KucoinAPIException
import logging
from kucoin_dir.development.Kucoin_websocket_speed_update import KucoinWebSocketScraper
from hf_kucoin_order import KucoinHFTrading
import sys
import fcntl  # Import fcntl for file locking

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Explicitly set logger level

LOCK_FILE = '/tmp/buying_kucoin.lock'

def main():
    lock_file = acquire_lock()
    try:
        directory = '/root/trading_systems/kucoin_dir/new_pair_data_kucoin'
        percent_of_price_buy = 1.4 # setting limit order to buy n% above the retrived price
        percent_of_price_sell = 0.8 # setting limit order to sell n% above the ACTUAL buy price NOT the retrived price
        max_wait_time_for_execution = 0.5 # time to wait for price if passed no order execution but continues to retrive price
        # Loop through announced pairs 
        for filename in os.listdir(directory):
            if filename.endswith(".json"):
                with open(os.path.join(directory, filename)) as f:
                    new_pair_dict = json.load(f)

            try:
                #create URL to scrape price data
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
                try:
                    if datetime.now() > release_date_time:
                        logger.info('release time has passed')
                        break
                except Exception as e:
                    logger.error(f"checking release time:\n {e}")

                logger.info(f'detected new pair {new_pair_dict["pair"]} at {new_pair_dict["date_time_string"]}')
                    
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

            # Example usage
                async def scraper(client               = client,
                                basecoin               = basecoin,
                                percent_of_price_buy   = percent_of_price_buy, 
                                percent_of_price_sell  = percent_of_price_sell):

                    symbol = basecoin
                    scraper = KucoinWebSocketScraper()
                    try:
                        websocket_release_price = await scraper.get_price_websocket(symbol, max_wait_time=2,release_time=release_date_time)
                        
                        
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

                            try:
                                # using limit order buy price as referce to sell
                                response_sell_order = await place_sell_limit_order(limit_order_buy_price, size, client, basecoin, percent_of_price_sell)
                                logger.debug(f'Sell order response: {response_sell_order}')
                            except Exception as e:
                                logger.error(f'Error in selling execution: {e}')
                    
                        else:
                            logger.info(f'Price retrived to slow /price: {websocket_release_price}')

                    
                    finally:
                        await scraper.cleanup()

                # Run the asynchronous scraper
                asyncio.run(scraper())

                # if loop has detected pair break and wait fornext cronjob
                logger.debug('break for after detecting pair loop')
                break

        print(f'{datetime.now().strftime("%H:%M:%S")} loop finished sleeping 1 seconds before exit')
        print('branch update No. 5')
        time.sleep(1)

    
    finally:
         release_lock(lock_file)  

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
                                 percent_of_price_sell=1):
    try:
        decimal_to_round = 6  # Adjust based on your requirements
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

def price_execute_order_logger(token_price, client, basecoin,percent_of_price_buy=0.2,percent_of_price_sell=1):
    try:
        # Order details
        size, dezimal_to_round = order_size_and_rounding(token_price)
        symbol = f'{basecoin}-USDT'
        errorcount = 0

        while True:
            try:
                if errorcount > 2:
                    logger.error('Error count > 2')
                    break
                price_buy = round(token_price * percent_of_price_buy, dezimal_to_round)
                logger.info(f'Buying price: {price_buy} at {datetime.now().strftime("%H:%M:%S.%f")[:-3]}')
                
                # Check if the price is within the allowed range
                if errorcount > 1:
                    price_buy = round(float(max_price), dezimal_to_round)
                    logger.info(f'Adjusted price_buy: {price_buy}')

                order_buy = client.create_limit_order(symbol, 'buy', price_buy, size)
                logger.info(f'Buy order executed at: {datetime.now().strftime("%H:%M:%S.%f")[:-3]}: {order_buy}')
                logger.info(f'limitbuy order price: {price_buy}')
                if order_buy:
                    break
                    
            except KucoinAPIException as e:
                logger.error(f"KucoinAPIException occurred: {str(e)}")
                # If the exception has specific attributes, you can access them directly
                if hasattr(e, 'message'):
                    logger.error(f"Error message: {e.message}")
                if hasattr(e, 'code'):
                    logger.error(f"Error code: {e.code}")
                
                error_code = e.code
                if '400370' in error_code:
                    match = re.search(r"Max\. price: ([0-9]*\.?[0-9]+)", str(e))
                    if match:
                        max_price = float(match.group(1))
                        logger.info(f'Extracted max price: {max_price}')
                        # Adjust price_buy based on max_price if needed
                        price_buy = max_price
                        logger.info(f'Adjusted price_buy: {price_buy}')
                errorcount += 1

            except Exception as e:
                logger.error(f'Failed to execute buy order: {e}')
                errorcount += 1

        # Create own sell function
        try:
            sell_qty = round(float(size),1)# * 0.99
            price_sell = round((price_buy * percent_of_price_sell), dezimal_to_round)
            order_sell = client.create_limit_order(symbol, 'sell', price_sell, str(sell_qty))
            logger.info(f'Sell order executed at {datetime.now().strftime("%H:%M:%S.%f")[:-3]}: {order_sell}')
            logger.info(f'limitsell order price : {price_sell}')

        except KucoinAPIException as e:
            logger.error(f"KucoinAPIException Selling: {str(e)}")
            # If the exception has specific attributes, you can access them directly byturing into string
            # if hasattr(e, 'message'):
            #     logger.error(f"Error message: {e.message}")
            # if hasattr(e, 'code'):
            #     logger.error(f"Error code: {e.code}")

            # # Error handling with error code 
            # if '200004' in e.code:
            #     logger.error('Insufficient balance')
            # traceback.print_exc()
        
    except Exception as e:
        logger.error(f"Error in price execution function:\n {e}")
        traceback.print_exc()



def wait_until_listing(time_waiting_in_seconds):
    """
    Waiting and updating when time is close to mitigate time lagging.
    
    This function continuously checks the remaining time until the listing and sleeps for different intervals
    based on the remaining time. It prints messages when the time to listing is less than certain thresholds.
    
    Args:
        time_waiting_in_seconds (float): The time in seconds until the listing.
    """
    two_seconds_mark = False
    while True:
        if 0.3 > time_waiting_in_seconds > 0:
            print('time to listing is less than 0.3 seconds: ', datetime.now())
            break
        elif 2 > time_waiting_in_seconds > 0:
            if not two_seconds_mark:
                two_seconds_mark = True
                print('time to listing is less than 2 seconds')
            time.sleep(0.1)
        elif 15 > time_waiting_in_seconds > 0:
            time.sleep(1)
        elif 15 < time_waiting_in_seconds > 0:
            time.sleep(10)
        else:
            print('time to listing is less than 0 seconds')
            break

    


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



def time_until_listing_seconds(date_time_dict):
            # Define the target token and date and time
        year = date_time_dict["year"]
        month = date_time_dict["month"]
        day = date_time_dict["day"]
        hour = date_time_dict["hour"]
        minute = date_time_dict["minute"]
        second = date_time_dict["second"]
        
        time_to_listing_seconds = (datetime(year, month, day, hour, minute, second) - datetime.now()).total_seconds() # Calculate the time difference in seconds
        #print(f'listing in: {time_to_listing_seconds} seconds')
        return time_to_listing_seconds



def parse_date_time_string_regex(date_time_string):
    # Regular expression to match the date and time components
    regex = r"(\w{3}) (\d{1,2}), (\d{4}), (\d{1,2})(?::(\d{2}))?(?::(\d{2}))?(\w{2})"
    
    match = re.match(regex, date_time_string)
    if match:
        month_str, day, year, hour, minute, second, am_pm = match.groups()
        
        # Convert month abbreviation to month number
        datetime_obj = datetime.strptime(month_str, "%b")
        month = datetime_obj.month
        
        # Convert year, day, and hour to integers
        year = int(year)
        day = int(day)
        hour = int(hour) % 12 + (12 if am_pm.upper() == "PM" else 0)
        
        # Check if minute and second are present, else default to 0
        minute = int(minute) if minute else 0
        second = int(second) if second else 0
        
        # Return the components as a dictionary
        return {
            "year": year,
            "month": month,
            "day": day,
            "hour": hour,
            "minute": minute,
            "second": second
        }
    else:
        print("Date time format is incorrect")
        # Return an empty dictionary or handle the incorrect format as needed
        return {}



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
    main()