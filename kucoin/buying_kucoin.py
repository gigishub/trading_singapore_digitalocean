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

import sys
sys.path.append('/root/1.code_on_server')
from update_kucoinclass import KucoinWebSocketScraper




#update5 branch 01.11.24

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(funcName)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Explicitly set logger level


def main():
    os.chdir('/root/1.code_on_server')
    directory = '/root/new_pair_data_kucoin'
    #xpath_to_find = "//*[@id='trade4-xl-container']/section[1]/div/div[1]/div[3]/div[1]/div[1]/span"
    percent_of_price_buy = 0.2 # setting limit order to buy n% above the retrived price
    percent_of_price_sell = 1.2 # setting limit order to sell n% above the ACTUAL buy price NOT the retrived price

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
                logger.info(f'detected new pair {new_pair_dict["pair"]} at {new_pair_dict["date_time_string"]} {datetime.now()}')
            except Exception as e:
                logger.error(f"Error when logging new pair: {e}")
                
            try:
                # API credentials
                api_key = '66ba4dd1d3e67a000108330f'
                api_secret = 'c5465410-7eca-43a4-86ac-c63c0e9b8da5'
                api_passphrase = 'buhyabuhna'

                # Initialize the client
                client = Client(api_key, api_secret, api_passphrase)
                logger.info(f'client initialized {datetime.now()}')

                #trading_rules_for_token(trading_rules,pair=new_pair_dict['pair'])

                # does not connect to client need print statements to debug

            except Exception as e:
                logger.info(f"Error when initialising kucoain:\n {e}")

        # Example usage
            async def scraper(client               = client,
                            basecoin               = basecoin,
                            percent_of_price_buy   = percent_of_price_buy, 
                            percent_of_price_sell  = percent_of_price_sell):
                

                symbol = basecoin
                scraper = KucoinWebSocketScraper()
                try:
                    price = await scraper.get_price_websocket(symbol, max_wait_time=2,release_time=release_date_time)
                    if price:
                        logger.info(f'get_release_price function returned: {price}')

                        price_execute_order_logger(token_price=price,
                                        client=client,basecoin=basecoin,
                                        percent_of_price_buy= percent_of_price_buy, 
                                        percent_of_price_sell=percent_of_price_sell)


                finally:
                    await scraper.cleanup()

            
            
            
            # Run the asynchronous scraper
            asyncio.run(scraper())



            

    print(f'loop finished sleeping 1 seconds before exit {datetime.now()}')
    print('branch update No. 5')
    time.sleep(1)



def price_execute_order(token_price, client, basecoin, percent_of_price_buy=0.2, percent_of_price_sell=1):
    try:
        # Order details
        size, dezimal_to_round = order_size_and_rounding(token_price)
        symbol = f'{basecoin}-USDT'
        errorcount = 0

        while True:
            try:
                if errorcount > 2:
                    print('Error count > 2')
                    break
                price_buy = round(token_price * percent_of_price_buy, dezimal_to_round)
                print(f'Buying price: {price_buy} at {datetime.now().strftime("%H:%M:%S.%f")[:-3]}')
                
                # Check if the price is within the allowed range
                if errorcount > 1:
                    price_buy = round(float(max_price), dezimal_to_round)
                    print(f'Adjusted price_buy: {price_buy}')

                order_buy = client.create_limit_order(symbol, 'buy', price_buy, size)
                print(f'Buy order executed at: {datetime.now().strftime("%H:%M:%S.%f")[:-3]}: {order_buy}')
                print(f'limitbuy order price: {price_buy}')
                if order_buy:
                    break
                    
            except KucoinAPIException as e:
                print(f"KucoinAPIException occurred: {str(e)}")
                # If the exception has specific attributes, you can access them directly
                if hasattr(e, 'message'):
                    print(f"Error message: {e.message}")
                if hasattr(e, 'code'):
                    print(f"Error code: {e.code}")
                
                error_code = e.code
                if '400370' in error_code:
                    match = re.search(r"Max\. price: ([0-9]*\.?[0-9]+)", str(e))
                    if match:
                        max_price = float(match.group(1))
                        print(f'Extracted max price: {max_price}')
                        # Adjust price_buy based on max_price if needed
                        price_buy = max_price
                        print(f'Adjusted price_buy: {price_buy}')
                errorcount += 1

            except Exception as e:
                print(f'Failed to execute buy order: {e}')
                errorcount += 1

        # Create own sell function
        try:
            sell_qty = float(size) * 0.99
            price_sell = round((price_buy * percent_of_price_sell), dezimal_to_round)
            order_sell = client.create_limit_order(symbol, 'sell', price_sell, str(sell_qty))
            print(f'Sell order executed at {datetime.now().strftime("%H:%M:%S.%f")[:-3]}: {order_sell}')
            print(f'limitsell order price : {price_sell}')

        except KucoinAPIException as e:
            print(f"KucoinAPIException occurred: {str(e)}")
            # If the exception has specific attributes, you can access them directly
            if hasattr(e, 'message'):
                print(f"Error message: {e.message}")
            if hasattr(e, 'code'):
                print(f"Error code: {e.code}")

            # Error handling with error code 
            if '200004' in e.code:
                print('Insufficient balance')
            # traceback.print_exc()
        
    except Exception as e:
        print(f"Error in price execution function:\n {e}")
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

        # # Wait to sell 
        # wait_time = 2
        # logger.info(f'Waiting {wait_time} seconds before selling')
        # time.sleep(wait_time)

        # Create own sell function
        try:
            sell_qty = float(size) * 0.99
            price_sell = round((price_buy * percent_of_price_sell), dezimal_to_round)
            order_sell = client.create_limit_order(symbol, 'sell', price_sell, str(sell_qty))
            logger.info(f'Sell order executed at {datetime.now().strftime("%H:%M:%S.%f")[:-3]}: {order_sell}')
            logger.info(f'limitsell order price : {price_sell}')

        except KucoinAPIException as e:
            logger.error(f"KucoinAPIException occurred: {str(e)}")
            # If the exception has specific attributes, you can access them directly
            if hasattr(e, 'message'):
                logger.error(f"Error message: {e.message}")
            if hasattr(e, 'code'):
                logger.error(f"Error code: {e.code}")

            # Error handling with error code 
            if '200004' in e.code:
                logger.error('Insufficient balance')
            # traceback.print_exc()
        
    except Exception as e:
        logger.error(f"Error in price execution function:\n {e}")
        traceback.print_exc()

def trading_rules_for_token(trading_rules,pair):
    # Retrieve trading rules for all trading pairs

    # Find the trading rules for a specific symbol
    symbol = pair
    for rule in trading_rules:
        if rule['symbol'] == symbol:
            min_size = float(rule['baseMinSize'])
            size_increment = float(rule['baseIncrement'])
            min_price = float(rule['quoteMinSize'])
            price_increment = float(rule['quoteIncrement'])
            return  
        print() 
        print(f"Trading rules for {symbol} at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
        print(f"Minimum size: {min_size}")
        print(f"Size increment: {size_increment}")
        print(f"Minimum price: {min_price}")
        print(f"Price increment: {price_increment}")
        print()
            



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
            dezimal_to_round = 10
            size = '1000100'
        elif token_price < 0.00009:
            dezimal_to_round = 9
            size = '100100'
        elif token_price < 0.0009:
            dezimal_to_round = 8
            size = '10100'
        elif token_price < 0.009:
            dezimal_to_round = 7
            size = '1010'
        elif token_price < 0.09:
            dezimal_to_round = 6
            size = '110'
        elif token_price < 0.9:
            dezimal_to_round = 5
            size ='11'
        elif token_price < 9:
            dezimal_to_round = 2
            size ='1'

        return size, dezimal_to_round


if __name__ == "__main__":
    main()






            # # create scraper function to run asyncronatically
            # async def scraper2(client=client,basecoin=basecoin,
            #                     percent_of_price_buy=percent_of_price_buy,
            #                     percent_of_price_sell=percent_of_price_sell,
            #                     offset_release_time=offset_release_time):
            #     try:
            #         # Time of token release
            #         release_time = release_date_time - timedelta(seconds=offset_release_time)
            #         #logger.info(f'Release time: {release_time}')
                    
            #         # Initiate scraping object
            #         scraper = FastKucoinWebSocket()
            #         price, status = await scraper.get_price(basecoin, release_time)
            #         logger.info(f'get_release_price function returned: {price} at {datetime.now().strftime("%H:%M:%S.%f")[:-3]}')
                    
            #         price_execute_order_logger(token_price=price,
            #                                 client=client,basecoin=basecoin,
            #                                 percent_of_price_buy= percent_of_price_buy, 
            #                                 percent_of_price_sell=percent_of_price_sell)
                    
            #     except KeyboardInterrupt:
            #         logger.info("Script stopped by user")
            #     except Exception as e:
            #         logger.error(f"Script error: {e}")
            #     finally:
            #         #ensure cleanup happens
            #         await scraper.cleanup()

            # # Run the asynchronous scraper
            #asyncio.run(scraper2())