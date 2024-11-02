import json 
from datetime import datetime
import os
import time



#17/07/24 4pm

def main():
    # Define trading parameters
    initial_buying_qty = '0.000001'
    buyingPrice =   1.223
    min_buying_qty_multiplier = 9
    testNetActive = False
    session = create_session_bybit(test=testNetActive)
    
    #directory_path = '/root/test_data'
    directory_path = '/root/new_pair_data'
    #directory_path = 'new_pair_data'
    json_files = [f for f in os.listdir(directory_path) if f.endswith('.json')]

    # Process each JSON file
    for json_file in json_files:
        json_file_path = os.path.join(directory_path, json_file)
        with open(json_file_path, 'r') as f:
            json_data = json.load(f)

    
        # Calculate the time remaining to listing
        time_to_listing = get_hours_to_listing(json_file_path)
        try:
            if 'buying_qty' not in json_data and time_to_listing > 0:
                min_buying_qty = determine_lowest_buying_qty(symbol=json_data['pair'],
                                            category='spot',
                                            session=session,
                                            price=buyingPrice,
                                            qty=initial_buying_qty)
                
                total_buying_qty = min_buying_qty * min_buying_qty_multiplier

                json_data.update({'min_buying_qty': min_buying_qty})
                json_data.update({'buying_qty': total_buying_qty})
                print('_____________________________________________________________')
                print(f"Added min_buying_qty {json_data['min_buying_qty']} for {json_data['pair']}")
                print(f"Added buying_qty {json_data['buying_qty']} for {json_data['pair']}")
                print('_____________________________________________________________')


                with open(json_file_path, 'w') as f:
                    json.dump(json_data, f, indent=4)
                

            elif 'buying_qty' in json_data and time_to_listing > 0:
                print(f"buying_qty for {json_data['pair']} is {json_data['buying_qty']}")
            
        except TypeError as e:
            print(f"review error Checking from determine_lowest_buying_qty() function")
            print(e)



                                        
def determine_lowest_buying_qty(symbol,category,session,price,qty):
    low = qty
    high =None
    decimal_points = 7
    qty = round(float(qty),decimal_points)
    start_time = time.time()
    time_limit = 25 * 60  # 9.5 minutes in seconds

    while True:
        try:
            # Check if the time limit has been exceeded
            elapsed_time = time.time() - start_time
            print(f'timlilimit for function to run: {time_limit:.0f} seconds')
            print(f'time running: {elapsed_time:.0f} seconds')
            if elapsed_time > time_limit:
                print("Time limit exceeded. Exiting the loop.")
                break


            print('high:', high)
            print('low:', low)
            print('new_qty:', round(qty,decimal_points))
            BUY_limit_order(symbol,category,session,price,round(qty,decimal_points))
            # If we reach this point without an exception, it means the order was successful

        except Exception as e:
            print()
            print(str(e))
            print()

            # Order quantity exceeded lower limit. (ErrCode: 170136) (ErrTime: 14:49:57).
            if '170136' in str(e):
                low = qty
                if high is None:
                    qty = float(qty) * 10 
                    print(f"Adjusting buying qty to {qty}")
                if high is not None:
                    qty = (low + high)
                    high = qty
                    print(f"Adjusting buying qty to {qty}")

            
            # UID 70437586 is not available to this feature (ErrCode: 170219)
            if '170219' in str(e):
                if high is None:
                    high = qty
                    print(f"Setting high to {high}")
                if high is not None:
                    #qty = high
                    qty = (low + high) / 2
                    high  = qty
                    print(f"high is not None and quatity set to {qty}")
                if high and (high-low)<1 : 
                    if qty > 1:
                        return round(qty,None)
                    if qty < 1:
                        return round(qty,decimal_points)
                
            # Order quantity has too many decimals. (ErrCode: 170137)
            if '170137' in str(e):
                decimal_points -= 1
                print(f"Decimlal Points to {decimal_points} and qty to {round(qty,decimal_points)}")
            # Invalid symbol. (ErrCode: 170121)
            if '170121' in str(e):
                print(e)
                break
            
            time.sleep(3)



def save_trade_data(startTime, entryTradeData, entryTradeErrorList, pair):
    import json
    import os
    from datetime import datetime 
    try:
        save_dir_parent = 'saved_trade_data'
        os.makedirs(save_dir_parent, exist_ok=True)

        save_dir_pair_time = f'intrade_{pair}_{datetime.fromtimestamp(startTime).strftime("%Y.%m.%d_%H-%M")}'
        file_path = os.path.join(save_dir_parent, save_dir_pair_time)
        os.makedirs(file_path, exist_ok=True)

        # Write the error messages to a separate JSON file
        with open(os.path.join(file_path, 'errors.json'), 'w') as f:
            json.dump(entryTradeErrorList, f)

        # After the loop, save entry data to a JSON file
        with open(os.path.join(file_path, 'intrade_data.json'), 'w') as f:
            json.dump(entryTradeData, f)
    except Exception as e:
        print(f"An error occurred during file operations: {e}")

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
        print(f"Order: {order['retMsg']}   orderID: {order['result']['orderId']}")
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
        print(f"Order: {order['retMsg']}   orderID: {order['result']['orderId']}")
        return order
    else:
        return None


def get_hours_to_listing (json_file_path):
    """
    Calculates the number of hours from the current time to a listing time specified in a JSON file.

    The function reads a JSON file specified by `json_file_path`, extracts a datetime string under the key
    'date_time_string', and converts it into a datetime object. It supports datetime strings with and without minutes.
    The function then calculates the difference between the current time and the listing time, returning the
    difference in hours.

    Parameters:
    - json_file_path (str): The file path to the JSON file containing the listing time information.

    Returns:
    - float: The number of hours from the current time to the listing time. This value can be negative if the
      listing time is in the past.

    Raises:
    - ValueError: If the datetime string in the JSON file does not match the expected format.
    """

    import json
    from datetime import datetime
    # Load the JSON data from the file
    with open(json_file_path, 'r') as f:
        new_pair_data = json.load(f)

    # Extract the time to execute from the JSON data
    time_to_execute_str = new_pair_data['date_time_string']

    # Convert the string to a datetime object
    try:
        time_to_execute = datetime.strptime(time_to_execute_str, "%b %d, %Y, %I:%M%p")
    except ValueError:
        # If it fails, try parsing without minutes
        time_to_execute = datetime.strptime(time_to_execute_str, "%b %d, %Y, %I%p")

    # Calculate the future time by adding hours to the current time
    time_to_listing = time_to_execute - datetime.now() 
    
    return time_to_listing.total_seconds() / 3600

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

if __name__ == "__main__":
    main()