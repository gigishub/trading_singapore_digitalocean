import aiohttp
import asyncio
import time
import logging
import orjson
from datetime import datetime
from typing import Optional
from datetime import timedelta
import traceback
import base64
import hmac
import hashlib

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class Bitget_websocket_collection:
    def __init__(self, api_key: str = None, api_secret: str = None, passphrase: str = None):
        """
        Initialize the WebSocket scraper with basic configuration
        Args:
            api_key: Optional API key for private channels
            api_secret: Optional API secret for private channels
            passphrase: Optional API passphrase for private channels
        """
        self.public_ws_url = "wss://ws.bitget.com/v2/ws/public"
        self.private_ws_url = "wss://ws.bitget.com/v2/ws/private"
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.price_found = asyncio.Event()
        self.final_price = None
        self.ws_connection = None
        self.ws_session = None
        self.ping_task = None

    def _generate_signature(self, timestamp: str) -> str:
        """
        Generate signature for authentication
        Args:
            timestamp: Current timestamp in seconds
        Returns:
            str: Base64 encoded signature
        """
        message = timestamp + 'GET' + '/user/verify'
        mac = hmac.new(
            bytes(self.api_secret, encoding='utf8'),
            msg=bytes(message, encoding='utf-8'),
            digestmod='sha256'
        )
        d = mac.digest()
        return base64.b64encode(d).decode()

    async def _login(self) -> bool:
        """
        Authenticate with the WebSocket server
        Returns:
            bool: True if login successful, False otherwise
        """
        if not all([self.api_key, self.api_secret, self.passphrase]):
            return True  # No authentication needed for public channels

        timestamp = str(int(time.time()))
        sign = self._generate_signature(timestamp)

        login_data = {
            "op": "login",
            "args": [{
                "apiKey": self.api_key,
                "passphrase": self.passphrase,
                "timestamp": timestamp,
                "sign": sign
            }]
        }

        await self.ws_connection.send_str(orjson.dumps(login_data).decode('utf-8'))
        
        async for msg in self.ws_connection:
            if msg.type == aiohttp.WSMsgType.TEXT:
                data = orjson.loads(msg.data)
                if data.get('event') == 'login' and data.get('code') == '0':
                    logger.info("Successfully logged in to WebSocket")
                    return True
                elif data.get('event') == 'error':
                    logger.error(f"Login failed: {data.get('msg')}")
                    return False
        return False


    async def initialize_websocket(self, symbol: str, use_private: bool = False, use_ticker_channel: bool =None ,use_trade_channel:bool =None,use_depth_channel:bool =None) -> bool:
        """
        Initialize WebSocket connection and subscribe to market ticker
        Args:
            symbol: Trading pair symbol (e.g., 'BTC')
            use_private: Whether to use private WebSocket connection
        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            self.ws_session = aiohttp.ClientSession()
            ws_url = self.private_ws_url if use_private else self.public_ws_url
            self.ws_connection = await self.ws_session.ws_connect(ws_url)
            
            if use_private and not await self._login():
                await self.cleanup()
                return False

            # Start ping task to keep connection alive
            self.ping_task = asyncio.create_task(self.websocket_ping(self.ws_connection))
            
            # Format symbol correctly for Bitget
            formatted_symbol = f"{symbol}USDT"
            subscribe_data = {}
            # Subscribe to ticker
            if use_ticker_channel:
                subscribe_data = {
                "op": "subscribe",
                "args": [{
                    "instType": "SPOT",
                    "channel": "ticker",
                    "instId": formatted_symbol
                }]
            }

            if use_trade_channel:
                subscribe_data = {
                    "op": "subscribe",
                    "args": [
                        {
                            "instType": "SPOT",
                            "channel": "trade",
                            "instId": formatted_symbol
                        }
                    ]
                }

            if use_depth_channel:
                subscribe_data = {
                    "op": "subscribe",
                    "args": [
                        {
                            "instType": "SPOT",
                            "channel": "books5",
                            "instId": formatted_symbol
                        }
                    ]
                }
            
            await self.ws_connection.send_str(orjson.dumps(subscribe_data).decode('utf-8'))
            logger.info(f"Subscription request sent for {formatted_symbol}")
            
            # Wait for subscription confirmation
            async for msg in self.ws_connection:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = orjson.loads(msg.data)
                    if data.get('event') == 'subscribe':
                        logger.info(f"Successfully subscribed to {formatted_symbol}")
                        return True
                    elif 'data' in data:
                        logger.info(f"Receiving data for {formatted_symbol}")
                        return True
            
            return False
        except Exception as e:
            logger.error(f"WebSocket initialization error: {str(e)}")
            await self.cleanup()
            return False


    async def websocket_ping(self, ws):
        """
        Maintain WebSocket connection with periodic ping messages
        Args:
            ws: WebSocket connection object
        """
        try:
            while True:
                await ws.send_str('ping')
                await asyncio.sleep(20)  # Send ping every 20 seconds
        except Exception as e:
            logger.error(f"Ping error: {str(e)}")
            if not ws.closed:
                try:
                    await ws.close()
                except:
                    pass

    async def wait_until_listing(self, release_time: datetime):
        initiation_time_print_flag = False
        while True:
            time_waiting_in_seconds = (release_time - datetime.now()).total_seconds()

            if 25 < time_waiting_in_seconds > 0:
                if not initiation_time_print_flag:
                    initiation_time_print_flag = True
                    logger.info(f'Waiting {time_waiting_in_seconds:.2f} seconds until token release time')
                await asyncio.sleep(5)

            if 25 > time_waiting_in_seconds > 0:
                logger.info('25 seconds left until token release')
                break


    async def get_price_by_release_time_ticker(self, symbol: str, max_wait_time: int = 1, release_time: datetime = None) -> Optional[float]:
        """
        Get price through WebSocket connection at specific release time
        Args:
            symbol: Trading pair symbol
            max_wait_time: Maximum time to wait for price in seconds
            release_time: DateTime object for when to start monitoring price
        Returns:
            Optional[float]: Price if found, None otherwise
        """
        logger.info('Release time and date: ' + release_time.strftime('%d-%m-%Y %H:%M:%S.%f')[:-3])
        success = await self.initialize_websocket(symbol, use_ticker_channel=True)
        
        if not success:
            logger.error("Failed to initialize WebSocket")
            return None

        end_time = release_time + timedelta(seconds=max_wait_time)
        await self.wait_until_listing(release_time)

        while datetime.now() < release_time:
            await asyncio.sleep(0.0001)

        try:
            async for msg in self.ws_connection:
                if datetime.now() > end_time:
                    logger.info("Reached max wait time")
                    break

                if msg.type == aiohttp.WSMsgType.TEXT:
                    # Handle ping/pong messages
                    if msg.data in ['ping', 'pong']:
                        logger.debug(f"Received {msg.data} message")
                        continue
                        
                    try:
                        data = orjson.loads(msg.data)
                        logger.info(f'message orjson data: {str(data)}')
                        
                        # Skip pong messages in JSON format
                        if isinstance(data, dict) and data.get('op') == 'pong':
                            continue
                            
                        if 'data' in data:
                            price_data = data['data']
                            price_data = price_data[0]
                            self.final_price = price_data
                            self.price_found.set()
                            logger.info(f"bid price: {price_data.get('bidPr')}, ask price: {price_data.get('askPr')}")
                            return price_data
                    except orjson.JSONDecodeError as e:
                        logger.debug(f"Skipping non-JSON message: {msg.data[:100]}")  # Log first 100 chars of message
                    except (KeyError, ValueError, IndexError) as e:
                        logger.error(f"Failed to parse price: {str(e)}")

                await asyncio.sleep(0.0001)

            logger.debug("No price found after all retry attempts")
            return None
        except Exception as e:
            logger.error(f"Error in retrieving price loop: {str(e)}")
            traceback.print_exc()
            return None


    async def get_price_by_release_time_trade(self, symbol: str, max_wait_time: int = 2, release_time: datetime = None) -> Optional[float]:
        """
        Get price through WebSocket connection at specific release time
        Args:
            symbol: Trading pair symbol
            max_wait_time: Maximum time to wait for price in seconds
            release_time: DateTime object for when to start monitoring price
        Returns:
            Optional[float]: Price if found, None otherwise
        """
        logger.info('Release time and date: ' + release_time.strftime('%d-%m-%Y %H:%M:%S.%f')[:-3])
        success = await self.initialize_websocket(symbol, use_trade_channel=True)
        
        if not success:
            logger.error("Failed to initialize WebSocket")
            return None

        end_time = release_time + timedelta(seconds=max_wait_time)
        await self.wait_until_listing(release_time)

        while datetime.now() < release_time:
            await asyncio.sleep(0.0001)

        try:
            async for msg in self.ws_connection:
                if datetime.now() > end_time:
                    logger.info("Reached max wait time")
                    break

                if msg.type == aiohttp.WSMsgType.TEXT:
                    # Handle ping/pong messages
                    if msg.data in ['ping', 'pong']:
                        logger.debug(f"Received {msg.data} message")
                        continue
                        
                    try:
                        data = orjson.loads(msg.data)
                        debug_data = data['action'], data['arg'], data['data'][:10]
                        logger.debug(f'message orjson data: {str(debug_data)}')
                        
                        # Skip pong messages in JSON format
                        if isinstance(data, dict) and data.get('op') == 'pong':
                            logger.debug("Received pong message")
                            continue
                            

                        price_data = data
                        logger.info(f"Price found via WebSocket:")
                        self.final_price = price_data
                        self.price_found.set()
                        return 'price_data'
                    except orjson.JSONDecodeError as e:
                        logger.debug(f"Skipping non-JSON message: {msg.data[:100]}")  # Log first 100 chars of message
                    except (KeyError, ValueError, IndexError) as e:
                        logger.error(f"Failed to parse price: {str(e)}")

                await asyncio.sleep(0.0001)

            logger.debug("No price found after all retry attempts")
            return None
        except Exception as e:
            logger.error(f"Error in retrieving price loop: {str(e)}")
            traceback.print_exc()
            return None
        

    async def get_price_by_release_time_depth(self, symbol: str, max_wait_time: int = 2, release_time: datetime = None) -> Optional[float]:
        """
        Get price through WebSocket connection at specific release time
        Args:
            symbol: Trading pair symbol
            max_wait_time: Maximum time to wait for price in seconds
            release_time: DateTime object for when to start monitoring price
        Returns:
            Optional[float]: Price if found, None otherwise
        """
        logger.info('Release time and date: ' + release_time.strftime('%d-%m-%Y %H:%M:%S.%f')[:-3])
        success = await self.initialize_websocket(symbol, use_depth_channel=True)
        
        if not success:
            logger.error("Failed to initialize WebSocket")
            return None

        end_time = release_time + timedelta(seconds=max_wait_time)
        await self.wait_until_listing(release_time)

        while datetime.now() < release_time:
            await asyncio.sleep(0.0001)

        try:
            async for msg in self.ws_connection:
                if datetime.now() > end_time:
                    logger.info("Reached max wait time")
                    break

                if msg.type == aiohttp.WSMsgType.TEXT:
                    # Handle ping/pong messages
                    if msg.data in ['ping', 'pong']:
                        logger.debug(f"Received {msg.data} message")
                        continue
                        
                    try:
                        data = orjson.loads(msg.data)
                        #logger.debug(f'message orjson data: {str(data)}')
                        
                        # Skip pong messages in JSON format
                        if isinstance(data, dict) and data.get('op') == 'pong':
                            logger.debug("Received pong message")
                            continue

                        price_data = data
                        asks_prices = price_data.get('data')[0]['asks']
                        logger.info(f"Price found via WebSocket (asks prices):{asks_prices}")
                        self.final_price = price_data
                        self.price_found.set()
                        return price_data

                    except orjson.JSONDecodeError as e:
                        logger.debug(f"Skipping non-JSON message: {msg.data[:100]}")  # Log first 100 chars of message
                    except (KeyError, ValueError, IndexError) as e:
                        logger.error(f"Failed to parse price: {str(e)}")

                await asyncio.sleep(0.0001)

            logger.debug("No price found after all retry attempts")
            return None
        except Exception as e:
            logger.error(f"Error in retrieving price loop: {str(e)}")
            traceback.print_exc()
            return None



    async def cleanup(self):
        """Clean up WebSocket resources and connections"""
        # Cancel ping task
        if self.ping_task and not self.ping_task.done():
            self.ping_task.cancel()
            try:
                await self.ping_task
            except asyncio.CancelledError:
                pass

        # Close WebSocket connection
        if self.ws_connection and not self.ws_connection.closed:
            await self.ws_connection.close()
            self.ws_connection = None

        # Close session
        if self.ws_session and not self.ws_session.closed:
            await self.ws_session.close()
            self.ws_session = None
        




                
async def main():
    # Example usage
    symbol = "BTC"
    scraper_trade = Bitget_websocket_collection()
    scraper_depth = Bitget_websocket_collection()    
    scraper_ticker = Bitget_websocket_collection()

    try:
        # Set the release time
        release_time = datetime.now() + timedelta(seconds=5)
        
        # Create tasks for different methods
        task_price = asyncio.create_task(
            scraper_trade.get_price_by_release_time_trade(symbol, max_wait_time=2, release_time=release_time)
        )
        task_depth = asyncio.create_task(
            scraper_depth.get_price_by_release_time_ticker(symbol, max_wait_time=2, release_time=release_time)
        )
        task_ticker = asyncio.create_task(
            scraper_ticker.get_price_by_release_time_depth(symbol, max_wait_time=2, release_time=release_time)
        )
        
        # Run all tasks concurrently
        results = await asyncio.gather(task_price, task_depth,task_ticker)
        
        # Process the results
        price = results[0]
        depth = results[1]
        ticker = results[2]
        print(f"\n---------\nPrice:{price} \n---------\nDepth: {depth}\n---------\nTicker: {ticker}")
        
    finally:
        await scraper_trade.cleanup()
        await scraper_depth.cleanup()
        await scraper_ticker.cleanup()
        await asyncio.sleep(1)



    # try:

    #     # Test release time price
    #     release_time = datetime.now() + timedelta(seconds=5)
    #     price = await scraper.get_price_by_release_time_depth(symbol, max_wait_time=2, release_time=release_time)
        
    #     if price:
    #         print(f"Successfully retrieved {symbol} price: {price}")
    #     else:
    #         print("No price found after all retry attempts")
            
    # finally:
    #     await scraper.cleanup()
    #     await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())