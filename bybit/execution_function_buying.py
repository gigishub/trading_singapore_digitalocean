from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from datetime import datetime , timedelta
import time
import re
import os 
import json 
import pybit
import traceback
from apscheduler.triggers.date import DateTrigger
from apscheduler.schedulers.background import BackgroundScheduler

#with scheduler


def main():
    # directories to choose from according to server local mashine and what kind of data is used 

    #directory = '/root/test_data'
    directory = '/root/new_pair_data'
    #directory = '/Users/macbookair/Documents/Python/TradingBotq/new_pair_data'

    

    # Initialize the scheduler
    scheduler = BackgroundScheduler()

    # Loop through announced pairs 
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            with open(os.path.join(directory, filename)) as f:
                new_pair_dict = json.load(f)

        #create URL to scrape price data
        basecoin = new_pair_dict['pair'].split('USDT')[0]
        url_to_scrape = f"https://www.bybit.com/en/trade/spot/{basecoin}/USDT"

        # get the time remaining to listing in secodns 
        # !!!can be combined to just input dicted and output remaining seconds!!!
        date_time_dict = parse_date_time_string(new_pair_dict['date_time_string'])
        datetime_to_listing_seconds = time_until_listing_seconds(date_time_dict)

        #check for buying qty
        if not 'buying_qty' in new_pair_dict:
            print(f'No buying qty for {new_pair_dict["pair"]}')

        # Check if listing is close to start
        if 0 < datetime_to_listing_seconds < 1200 and 'buying_qty' in new_pair_dict:
            print()
            print(f"Time to listing: {datetime_to_listing_seconds} seconds")
            print('setting arguments for webdriver')


            chrome_options = Options()
            # chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36")
            chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration
            chrome_options.add_argument("--no-sandbox")  # Disable sandboxing
            chrome_options.add_argument("--disable-dev-shm-usage")  # Disable shared memory usage
            chrome_options.add_argument("--disable-extensions")  # Disable extensions


            # Initialize the Chrome driver with Service class and options
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)


            try:
                # update time to listing
                time_webdriver_start    = time_until_listing_seconds(date_time_dict)
                print(f'Start WebDriver for {new_pair_dict["pair"]} with {time_webdriver_start} reamaining to listing')
                driver.get(url_to_scrape)

                # Execute JavaScript to block pop-ups
                driver.execute_script("window.alert = function() {}; window.confirm = function() {}; window.prompt = function() {};")
                
                # Wait for the element to be presented and be able to removed for better performance 
                print('waiting for webpage to load to remove elements')
                time.sleep(12)

                # Remove elements by XPath
                # list of elements to remove 
                xpaths_to_remove = [
                    "//ul[contains(@class,'handicap-list is-sell')]",
                    "//ul[contains(@class,'handicap-list is-buy')]",
                    "//*[@id='klineContainer']/div[1]",
                    "//*[@id='root']/el-config-provider/div/div[1]/div[2]/div",
                    "//*[@id='root']/el-config-provider/div/div[1]/div[1]/div/div",
                    "//*[@id='root']/el-config-provider/div/div[2]",
                    "//*[@id='uniFrameHeader']",
                    "//*[@id='klineContainer']/div[2]/div/div[2]/div/div[2]/div[4]/div[2]/div[1]",
                    "//*[@id='root']/el-config-provider/div/div[3]/div[1]/div/div[2]",
                    "//*[@id='root']/el-config-provider/div/div[1]/div[4]/div/div[1]",
                ]
                
                # java scriped code to remove elements
                for xpath in xpaths_to_remove:
                    script = f"""
                        var elements = document.evaluate("{xpath}", document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
                        console.log('Found elements:', elements.snapshotLength);
                        for (var i = 0; i < elements.snapshotLength; i++) {{
                            elements.snapshotItem(i).remove();
                            console.log('Removed element:', elements.snapshotItem(i));
                        }}
                    """
                    driver.execute_script(script)


                target_datetime_dict = parse_date_time_string(new_pair_dict['date_time_string'])
                target_time_offset  =  offset_target_time(target_datetime_dict['formatted_string'],seconds_to_subtract=1)
                print('target time:',target_time_offset)

                # Define time left until listing 
                time_waiting_until_listing = time_until_listing_seconds(date_time_dict)
                
                if time_waiting_until_listing > 0:
                    session = create_session_bybit(test=False)
                    
                    # Schedule the task
                    scheduler.add_job(check_price_from_webpage, DateTrigger(run_date=target_time_offset), args=[driver,new_pair_dict,directory,session])
                    # Start the scheduler
                    scheduler.start()
                    print('started scheduler', datetime.now())
                
                try:
                    seconds_check = False
                    while True:
                        time.sleep(1)
                        time_until_listing_loop = time_until_listing_seconds(date_time_dict)
                        if 30 > time_until_listing_loop > 0 and not seconds_check:
                            print(f'less than 30 seconds to listing{datetime.now()}')
                            seconds_check = True
                        if time_until_listing_loop < -50:
                            print(f'break while loop to keep scheduler running {datetime.now()}')
                            break
                finally:
                    # Shutdown driver and sceduler
                    driver.quit()
                    scheduler.shutdown()   
                    print('driver/scheuler quit: ',datetime.now())  
                   
 


            except Exception as e:
                print("Error code:", e)


    print('loop finished sleeping 10 seconds before exit', datetime.now())
    time.sleep(10)


def check_price_from_webpage(driver,new_pair_dict,directory,session,timeout=3):
    # offset loop to start a fraction before listing time
    loop_offset = 0.8
    print(f'offset loop start by: {loop_offset} seconds')
    
    # initilise values
    price_from_webpage = None
    time.sleep(loop_offset)
    start_time = datetime.now()
    print(f'starting loop at: {datetime.now()}')
    while True:
        try:
            price_from_webpage = driver.find_element(By.XPATH, "//*[contains(@class,'price-area flex-shrink-0')]")
            if price_from_webpage and len(price_from_webpage.text.strip()) > 2:
                print("Price found:", price_from_webpage.text.strip())
                break
        except Exception as e:
            print("Any other error:", e)
        if datetime.now() - start_time > timedelta(seconds=timeout):
            break
        print(f'Value not found at: {datetime.now()}')

    try:
        if price_from_webpage:
            timenow = datetime.now().strftime("%H:%M:%S.%f")
            datenow = datetime.now().strftime("%Y-%m-%d")
            print('-----------------------------------------')
            print('value retrieved at: --->',timenow +' <---')
            print('-----------------------------------------')
            
            # initilazing size 
            dezimal_to_round = 0
            # Convert the extracted text to a float
            price_from_webpage_float = float(price_from_webpage.text)
            # Define the number of decimal points to round to based on the price value
            if price_from_webpage_float < 0.00009:
                dezimal_to_round = 8
            elif price_from_webpage_float < 0.0009:
                dezimal_to_round = 7
            elif price_from_webpage_float < 0.009:
                dezimal_to_round = 6
            elif price_from_webpage_float < 0.09:
                dezimal_to_round = 5
            elif price_from_webpage_float < 0.9:
                dezimal_to_round = 4
            elif price_from_webpage_float < 9:
                dezimal_to_round = 2


            # exeute trade with error catching in tradeexecution function 
            trade_execution(session=session,
                            pair=new_pair_dict['pair'],
                            category = 'spot', 
                            #adding 2 time 
                            initial_buying_price = 3 * float(price_from_webpage.text), 
                            buying_qty = new_pair_dict['buying_qty'],
                            decimal_points=dezimal_to_round, 
                            selling_price_adjust_multiplier=2, 
                            selling_qty_multiplier=0.96, 
                            adjust_decrease_buying_percent=0.98, 
                            adjust_increase_selling_percent=1.02, 
                            adjust_wait_time_for_sell_order=1)


            # Assuming element_with_value.text and timenow are defined
            price_time_release = {"price_first_second": {'price': price_from_webpage.text,"value_accesed_at_time": timenow, "date": datenow}}
            new_pair_dict.update(price_time_release)
            
            #safe final dictonary with timesspamps 
            path_to_final_dict = os.path.join(directory, 'final_'+ new_pair_dict['pair'] + '.json')
            with open(path_to_final_dict, 'w') as f:
                json.dump(new_pair_dict, f)
            
            print('safe updated dict', datetime.now())

        else:
            print("Element found, but it contains no text.")


    except TimeoutException:
        print("Timed out waiting for element to appear.")
    except Exception as e:
        print("An error occurred:", e)


            

def trade_execution(session,
                    pair, 
                    category, 
                    initial_buying_price, 
                    buying_qty,
                    decimal_points=4, 
                    selling_price_adjust_multiplier=0.1, 
                    selling_qty_multiplier=0.99, 
                    adjust_decrease_buying_percent=0.98, 
                    adjust_increase_selling_percent=1.02, 
                    adjust_wait_time_for_sell_order=3):
    """
    Executes an automated trading strategy by placing buy and sell limit orders.
    The strategy adjusts the buying and selling prices based on predefined parameters to attempt to make a profit.

    Arguments:
    - pair: trading pair.
    - category: The category eg. "spot"
    - initial_buying_price: The initial price to place the buy limit order.
    - buying_qty: The quantity to buy.
    - decimal_points: The number of decimal points for the buying and selling prices.
    - test_net: Boolean indicating if the Bybit testnet should be used.
    - selling_price_adjust_multiplier: starting price for sell limt order eg.  1 = entry price and    0.1 = 10% of entry price 
    - selling_qty_multiplier: qty in percent of buying qty because eg. buying 100 token gets only 99 (avoiding insuffient balce error )
    - adjust_decrease_buying_percent: 0.98 = 2% decrease the buying price on each iteration to buy limit order to execute.
    - adjust_increase_selling_percent: 1.02 = 2% increase the selling price on each iteration to get sell limit order to execute..
    - adjust_wait_time_for_sell_order: Time to wait before placing a sell order after a buy order is executed.

    continues to adjust and place orders based on market conditions and the specified parameters until a sell order is successfully executed.
    """


    # Initialize trading variables
    decimal_points = decimal_points
    orderqty_decimal_points = 1
    buying_price = initial_buying_price
    entry_price = 0
    order_status_BUY = None
    order_status_SELL = None
    in_trade = False
    error_log = []
    data_log = []
    error_count = 0
    max_error = 700
    selling_qty = float(buying_qty) * selling_qty_multiplier
    buying_qty = float(buying_qty)

    try:
        while True:
            
            try:


                # Update in_trade status
                in_trade = order_status_BUY is not None

                # Place buy order if not in trade
                if not in_trade:
                    buying_price = round(buying_price, decimal_points)
                    print('Buying at: ', buying_price)
                    print('time before buy order: ', datetime.now())
                    order_status_BUY = BUY_limit_order(symbol=pair, category=category, session=session, price=str(round(buying_price,decimal_points)), qty=round(buying_qty,orderqty_decimal_points))
                    order_executed_at = datetime.now()
                    print('---------------------------------------')
                    print('order executed at: ',order_executed_at)
                    print('---------------------------------------')
                    entry_price = buying_price
                    selling_price = round(entry_price * selling_price_adjust_multiplier, decimal_points)


                    #maybe redundant 
                    time.sleep(adjust_wait_time_for_sell_order)

                # Wait before placing a sell order
                if in_trade:
                    order_status_SELL = SELL_limit_order(symbol=pair, category=category, session=session, price=str(round(selling_price,decimal_points)), qty=round(selling_qty,orderqty_decimal_points))
                        # Log iteration data
                
                data_log_dict= { error_count: {
                    "buying_price": buying_price,
                    "selling_price": selling_price,
                    "order_status_BUY": order_status_BUY,
                    "order_status_SELL": order_status_SELL,
                    "in_trade": in_trade,
                    'order_executed_at': order_executed_at.isoformat(),
                    "timestamp": datetime.now().strftime('%H-%M-%S')
                    }}
                
                data_log.append(data_log_dict)
                # Break loop if sell order is executed
                if order_status_SELL is not None:
                    print('Sell limit order successful at: ', selling_price)
                    print(order_status_SELL['retMsg'])
                    time.sleep(2)
                    #session.cancel_all_orders(category=category)
                    break




            except pybit.exceptions.InvalidRequestError as e:
                print(f"\nError message:\n{e}")
                # Buy order price cannot be higher than ..... (ErrCode: 170193) 
                if not in_trade and '170193' in str(e):
                    pattern = r'\d+\.\d+'
                    match = re.search(pattern, str(e))
                    if match:
                        buying_price = float(match.group())* 0.99
                        print(f"Adjusting buying from info error messsage to: {buying_price}")
                    else:
                        print(f"failed to BUY at {buying_price}")
                        buying_price = float(buying_price)
                        buying_price = round(buying_price * adjust_decrease_buying_percent, decimal_points)
                        print(f"Adjusting buying price to {buying_price}")
                        rate_limit_requests(iterations_per_second=37)

                # Sell order price cannot be lower than ..... (ErrCode: 170194)
                if in_trade and '170194' in str(e):
                    print(f"failed to SELL at {selling_price}")
                    pattern = r'\d+\.\d+'
                    match = re.search(pattern, str(e))
                    if match:
                        selling_price = float(match.group())* 1.01
                        print(f"Adjusting selling from info error messsage to: {selling_price}")
                    else:
                        selling_price = float(selling_price)
                        selling_price = round(selling_price * adjust_increase_selling_percent, decimal_points)
                        print(f"New SELLING price: {selling_price}")
                    rate_limit_requests(iterations_per_second=37)
                
                # Order price has too many decimals. (ErrCode: 170134) 
                if '170134' in str(e):
                    decimal_points -= 1
                    print(f"Decreasing decimal points to {decimal_points}")

                # UID 70437586 is not available to this feature (ErrCode: 170219)
                if '170219' in str(e):
                    print(f"UID 70437586 is not available to this feature")
                    rate_limit_requests(10)
                error_log.append(str(e))
                error_count +=1

                
                if error_count > max_error:
                    print(f"Max error count reached: {max_error}")
                    break
            


            except Exception as e1:
                print(f"ERROR from Exception NOT InvalidRequestError : {e1}")
                traceback.print_exc()
                error_log.append(str(e1))
                rate_limit_requests(4)
                error_count +=1
                if error_count > max_error:
                    print(f"Max error count reached: {max_error}")
                    break
    finally:
        save_trade_data(time.time(), data_log, error_log, pair)


def save_trade_data(startTime, entryTradeData, entryTradeErrorList, pair):
    import os
    from datetime import datetime
    import json

    try:
        parent_save_dir = 'data_collection'
        os.makedirs(parent_save_dir, exist_ok=True)
        
        save_dir = 'trade_execution_data'
        parentpath = os.path.join(parent_save_dir, save_dir)
        os.makedirs(parentpath, exist_ok=True)
        
        save_dir_pair_time = f'intrade_{pair}_{datetime.fromtimestamp(startTime).strftime("%Y.%m.%d_%H-%M")}'
        file_path = os.path.join(parentpath, save_dir_pair_time)
        os.makedirs(file_path, exist_ok=True)

        # Write the error messages to a separate JSON file
        with open(os.path.join(file_path, 'errors.json'), 'w') as f:
            json.dump(entryTradeErrorList, f)

        # After the loop, save entry data to a JSON file
        with open(os.path.join(file_path, 'intrade_data.json'), 'w') as f:
            json.dump(entryTradeData, f)
    except Exception as e:
        print(f"An error occurred: {e}")

def BUY_limit_order(symbol, category, session, price, qty):
    """
    Places a limit buy order for a given asset.
    Args:
        symbol (str): The symbol of the asset to place the order for.
        category (str): The category of the order.
        session (obj): The session object used to place the order.
        price (float): The price at which to place the limit order.
        qty (float): qty always in base coin eg. "BTCUSDT" qty in BTC.

    Returns:
        str: The order dict if the order is successfully placed, None otherwise.
    """

    order = session.place_order(category=category,
                                symbol=symbol,
                                side='buy',
                                price=price,
                                orderType='Limit',
                                qty=qty)
    
    if order['retMsg'] == 'OK':
        #print(f"Order: {order['retMsg']}   orderID: {order['result']['orderId']}")
        return order
    else:
        return None

def SELL_limit_order(symbol, category, session, price, qty):
    """
    Places a limit sell order for a given asset.
    Args:
        symbol (str): The symbol of the asset to place the order for.
        category (str): The category of the order.
        session (obj): The session object used to place the order.
        price (float): The price at which to place the limit order.
        qty (float): qty always in base coin eg. "BTCUSDT" qty in BTC.

    Returns:
        str: The order dict if the order is successfully placed, None otherwise.
    """

    order = session.place_order(category=category,
                                symbol=symbol,
                                side='sell',
                                price=price,
                                orderType='Limit',
                                qty=qty)
    
    if order['retMsg'] == 'OK':
        #print(f"Order: {order['retMsg']}   orderID: {order['result']['orderId']}")
        return order
    else:
        return None


def rate_limit_requests(iterations_per_second=40):
    import time
    """
    if called pauses the code for as long as the rate limit devided by second need to be 
    Parameters:
    - iterations_per_second: The number of requests allowed per second. Defaults to 40.

    retruns : the amount in seconds that function makes code sleep  
    """

    sleep_time = 1.0 / iterations_per_second  # Calculate sleep time in seconds
    time.sleep(sleep_time)  # Sleep to limit the rate of requests
    return sleep_time

def create_session_bybit(max_retries=100, test=True):
    """
    Creates a session with Bybit's API.

    Parameters:
    max_retries (int, optional): The maximum number of times to retry creating the session if a ReadTimeout occurs. Defaults to 5.
    test (bool, optional): Whether to create a testnet session or a live session. Defaults to True (testnet).

    Returns:
    HTTP: The created session, or None if the session could not be created after max_retries attempts.
    """
    from pybit.unified_trading import HTTP
    from requests.exceptions import ReadTimeout

    # If test is True, create a testnet session
    if test:
        for _ in range(max_retries):
            try:
                # Attempt to create the session
                session = HTTP(testnet=True,
                               api_key="YDpyCNtMWiNfKzvRtf",
                               api_secret="Tan1dxTFiTxTOIKtiHJu0Rub3q0n0JxlsQJ5")
                # If the session is created successfully, break the loop and return the session
                return session
            except ReadTimeout:
                # If a ReadTimeout occurs, print a message and retry
                print("ReadTimeout occurred, retrying...")
        else:
            # If the session could not be created after max_retries attempts, print a message and return None
            print(f"Failed to create session after {max_retries} retries")
            return None
    # If test is False, create a live session
    else:
        for _ in range(max_retries):
            try:
                # Attempt to create the session
                session = HTTP(testnet=False,
                               api_key="lZ9fMZoU1oTR5fWBNP",
                               api_secret="kcI4mAoQhd32gjXMPkEA2cjuyNC0rR0WHrpe")
                # If the session is created successfully, break the loop and return the session
                return session
            except ReadTimeout:
                # If a ReadTimeout occurs, print a message and retry
                print("ReadTimeout occurred, retrying...")
        else:
            # If the session could not be created after max_retries attempts, print a message and return None
            print(f"Failed to create session after {max_retries} retries")
            return None


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


def offset_target_time(datetime_str,seconds_to_subtract=1):
    """
    Subtracts one second from the given datetime string.

    Args:
    datetime_str (str): The datetime string in the format 'YYYY-MM-DD HH:MM:SS'.

    Returns:
    str: The updated datetime string with one second subtracted.
    """
    # Parse the string into a datetime object
    target_datetime = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')

    # Subtract one second
    target_datetime_minus_one_second = target_datetime - timedelta(seconds=seconds_to_subtract)

    # Convert back to string
    return target_datetime_minus_one_second.strftime('%Y-%m-%d %H:%M:%S')



def parse_date_time_string_old(date_time_string):
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



if __name__ == "__main__":
    main()