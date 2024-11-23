import aiohttp
import asyncio
import time
import logging
import orjson
from datetime import datetime
from typing import Optional
from datetime import timedelta
import traceback

# Configure logging with microsecond precision and function names
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) 

class market_level2_channel:
    def __init__(self):
        """Initialize the WebSocket scraper with basic configuration"""
        self.api_url = "https://api.kucoin.com"
        self.price_found = asyncio.Event()
        self.final_price = None
        self.ws_connection = None
        self.ws_session = None
        self.ping_task = None
        self.subscription_confirmed = asyncio.Event()

    async def get_token(self) -> Optional[str]:
        """
        Get WebSocket token from KuCoin API
        Returns:
            Optional[str]: Authentication token or None if request fails
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.api_url}/api/v1/bullet-public") as response:
                    if response.status == 200:
                        data = await response.json(loads=orjson.loads)
                        return data['data']['token']
        except Exception as e:
            logger.error(f"Failed to get WebSocket token: {e}")
            return None

    async def get_websocket_url(self) -> Optional[str]:
        """
        Construct WebSocket URL with authentication token
        Returns:
            Optional[str]: Complete WebSocket URL or None if token retrieval fails
        """
        token = await self.get_token()
        return f"wss://ws-api-spot.kucoin.com/?token={token}" if token else None

    async def websocket_ping(self, ws):
        """
        Maintain WebSocket connection with periodic ping messages
        Args:
            ws: WebSocket connection object
        """
        try:
            while True:
                ping_data = orjson.dumps({
                    "id": int(time.time() * 1000),
                    "type": "ping"
                }).decode('utf-8')
                await ws.send_str(ping_data)
                await asyncio.sleep(20)
        except Exception as e:
            logger.error(f"Ping error: {e}")
            if not ws.closed:
                await ws.close()

    async def wait_for_subscription(self, symbol: str) -> bool:
        """
        Wait for subscription confirmation
        Args:
            symbol: Trading pair symbol
        Returns:
            bool: True if subscription confirmed, False otherwise
        """
        try:
            async for msg in self.ws_connection:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = orjson.loads(msg.data)
                    if data.get('type') == 'ack' or (data.get('type') == 'message' and 'data' in data):
                        logger.info(f"Successfully subscribed to {symbol}-USDT")
                        self.subscription_confirmed.set()
                        return True
        except Exception as e:
            logger.error(f"Error waiting for subscription: {e}")
            return False
        return False

    async def initialize_websocket(self, symbol: str) -> bool:
        """
        Initialize WebSocket connection and subscribe to market ticker
        Args:
            symbol: Trading pair symbol (e.g., 'BTC')
        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            ws_url = await self.get_websocket_url()
            if not ws_url:
                return False

            self.ws_session = aiohttp.ClientSession()
            self.ws_connection = await self.ws_session.ws_connect(ws_url)
            
            # Start ping task to keep connection alive
            self.ping_task = asyncio.create_task(self.websocket_ping(self.ws_connection))
            
            subscribe_data = orjson.dumps({
                "id": int(time.time() * 1000),
                "type": "subscribe",
                "topic": f"/market/level2:{symbol}-USDT",
                "privateChannel": False,
                "response": True
            }).decode('utf-8')
            
            await self.ws_connection.send_str(subscribe_data)
            logger.info(f"Subscription request sent for {symbol}-USDT")

            # Wait for subscription confirmation with timeout
            try:
                subscription_confirmed = await asyncio.wait_for(
                    self.wait_for_subscription(symbol),
                    timeout=5.0
                )
                return subscription_confirmed
            except asyncio.TimeoutError:
                logger.error("Subscription confirmation timeout")
                return False

        except Exception as e:
            logger.error(f"WebSocket initialization error: {e}")
            await self.cleanup()
            return False

    async def wait_until_listing(self, release_time: datetime):
        """
        Wait until near the token listing time
        Args:
            release_time: DateTime object for when to start monitoring price
        """
        initiation_time_print_flag = False
        while True:
            time_waiting_in_seconds = (release_time - datetime.now()).total_seconds()

            if time_waiting_in_seconds > 25:
                if not initiation_time_print_flag:
                    initiation_time_print_flag = True
                    logger.info(f'Waiting {time_waiting_in_seconds:.2f} seconds until token release time')
                await asyncio.sleep(5)
            elif 0 < time_waiting_in_seconds <= 25:
                logger.info('25 seconds left until token release')
                break

    async def get_price_websocket(self, symbol: str, max_wait_time: int = 2, release_time: datetime = None) -> Optional[float]:
        """
        Get price through WebSocket connection
        Args:
            symbol: Trading pair symbol
            max_wait_time: Maximum time to wait for price in seconds
            release_time: DateTime object for when to start monitoring price
        Returns:
            Optional[float]: Price if found, None otherwise
        """
        logger.info('Release time and date: ' + release_time.strftime('%d-%m-%Y %H:%M:%S.%f')[:-3])
        
        # Initialize WebSocket and wait for confirmation
        success = await self.initialize_websocket(symbol)
        if not success:
            logger.error("Failed to initialize WebSocket")
            return None

        # Wait until near the token listing time
        end_time = release_time + timedelta(seconds=max_wait_time)
        await self.wait_until_listing(release_time)

        # Precise waiting for release time
        while datetime.now() < release_time:
            await asyncio.sleep(0.0001)

        try:
            async for msg in self.ws_connection:
                if datetime.now() > end_time:
                    logger.info("Reached max wait time")
                    break
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = orjson.loads(msg.data)
                    logger.debug(f"Received data: {data}")
                    if data.get('type') == 'message' and 'data' in data:
                        try:
                            # # ask price received from the socket
                            # price = float(data['data']['changes']['asks'][0][0])
                            data_from_socket = data['data']['changes']
                            if data_from_socket:
                                logger.info(f"data from WebSocket: {data_from_socket} ")
                                logger.debug(f"Data from socket: {data_from_socket}")
                                self.final_price = data_from_socket
                                self.price_found.set()
                                return data_from_socket
                                # if price > 0:
                                # logger.info(f"Price found via WebSocket: {price} at ")
                                # logger.debug(f"Data from socket: {data_from_socket}")
                                # self.final_price = price
                                # self.price_found.set()
                                # return price
                            else:
                                logger.debug(f"retrived: {data_from_socket}")
                        except (KeyError, ValueError) as e:
                            logger.error(f"Failed to parse price: {e}")
                            continue

        except Exception as e:
            logger.error(f"Error in retrieving price loop: {e}")
            traceback.print_exc()

        return None

    async def cleanup(self):
        """Clean up WebSocket resources and connections"""
        if self.ping_task and not self.ping_task.done():
            self.ping_task.cancel()
            try:
                await self.ping_task
            except asyncio.CancelledError:
                pass
            
        if self.ws_connection and not self.ws_connection.closed:
            await self.ws_connection.close()
            
        if self.ws_session and not self.ws_session.closed:
            await self.ws_session.close()


async def main():
    import os
    import json
    from datetime import datetime
    import traceback
    
    # if is_running():
    #     logger.info("Script is running. Exiting.")
    #     sys.exit(0)

    # create_lock()

    #try:

    directory = '/root/trading_systems/kucoin_dir/new_pair_data_kucoin'
    testing = True


    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            with open(os.path.join(directory, filename)) as f:
                new_pair_dict = json.load(f)

        try:
            #create URL to scrape price data
            symbol = new_pair_dict['pair'].split('USDT')[0]

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
        if 0 < datetime_to_listing_seconds < 1200:
            try:
                if datetime.now() > release_date_time:
                    logger.info('release time has passed')
                    break
            except Exception as e:
                logger.error(f"checking release time:\n {e}")

            logger.info(f'detected new pair {new_pair_dict["pair"]} at {new_pair_dict["date_time_string"]}')
        
            scraper = market_level2_channel()

            try:
                price = await scraper.get_price_websocket(symbol, max_wait_time=2, release_time=release_date_time)
                if price:
                    print(f"Successfully retrieved {symbol} price: {price}")
                else:
                    print("No price found after all retry attempts")
            finally:
                await scraper.cleanup()
    
    if testing:
        symbol = 'SWELL'
        scraper = market_level2_channel()
        #release_date_time = datetime(2024, 11, 8, 11, 35, 0)  # Example fixed datetime
        release_date_time = datetime.now() + timedelta(seconds=2)  # Example release time

        try:
            price = await scraper.get_price_websocket(symbol, max_wait_time=2, release_time=release_date_time)
            if price:
                print(f"Successfully retrieved {symbol} price: {price}")
            else:
                print("No price found after all retry attempts")
        finally:
            await scraper.cleanup()



    print('done')




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



if __name__ == "__main__":
    asyncio.run(main())



