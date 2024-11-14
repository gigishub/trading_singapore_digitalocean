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

class KucoinWebSocketScraper:
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
                "topic": f"/market/ticker:{symbol}-USDT",
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
                            price = float(data['data']['price'])
                            if price > 0:
                                logger.info(f"Price found via WebSocket: {price} at ")
                                self.final_price = price
                                self.price_found.set()
                                return price
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
    from datetime import timedelta

    symbol = "BTC"  # Example symbol
    scraper = KucoinWebSocketScraper()
    release_time = datetime(2024, 11, 4, 8, 58, 0)  # Example fixed datetime
    release_time = datetime.now() + timedelta(seconds=10)  # Example release time

    try:
        price = await scraper.get_price_websocket(symbol, max_wait_time=2, release_time=release_time)
        if price:
            print(f"Successfully retrieved {symbol} price: {price}")
        else:
            print("No price found after all retry attempts")
    finally:
        await scraper.cleanup()


if __name__ == "__main__":
    asyncio.run(main())






##############################################################################################################
#### more testing code
##############################################################################################################

# async def main():
#     from datetime import timedelta
#     import sys

#     # Test mode parameters
#     test_mode = len(sys.argv) > 1 and sys.argv[1] == "--test"
#     if test_mode:
#         logger.info("Running in test mode")
#         test_scenarios = [
#             {
#                 "symbol": "BTC",
#                 "max_wait_time": 2,
#                 "release_time": datetime.now() + timedelta(seconds=10),
#                 "description": "Normal scenario (10 second delay)"
#             },
#             {
#                 "symbol": "INVALID_SYMBOL",  # This should trigger max wait time
#                 "max_wait_time": 1,
#                 "release_time": datetime.now() + timedelta(seconds=20),
#                 "description": "Timeout scenario (20 second delay, 1s max wait)"
#             }
#         ]

#         for scenario in test_scenarios:
#             logger.info(f"\nTesting scenario: {scenario['description']}")
#             scraper = KucoinWebSocketScraper()
            
#             try:
#                 start_time = None
#                 price = await scraper.get_price_websocket(
#                     symbol=scenario['symbol'],
#                     max_wait_time=scenario['max_wait_time'],
#                     release_time=scenario['release_time']
#                 )
                
#                 if price:
#                     logger.info(f"SUCCESS: Price found: {price}")
#                 else:
#                     logger.info(f"EXPECTED TIMEOUT: No price found within {scenario['max_wait_time']} seconds")
                
#             except Exception as e:
#                 logger.error(f"Test scenario failed: {str(e)}")
#                 traceback.print_exc()
#             finally:
#                 await scraper.cleanup()
#                 logger.info(f"Scenario completed: {scenario['description']}\n")
#                 await asyncio.sleep(1)  # Brief pause between scenarios

#     else:
#         # Normal execution mode
#         symbol = "BTC"
#         scraper = KucoinWebSocketScraper()
#         release_time = datetime.now() + timedelta(seconds=30)  # 30 seconds from now
        
#         try:
#             price = await scraper.get_price_websocket(
#                 symbol=symbol,
#                 max_wait_time=2,
#                 release_time=release_time
#             )
            
#             if price:
#                 logger.info(f"Successfully retrieved {symbol} price: {price}")
#             else:
#                 logger.info(f"No price found after waiting {max_wait_time} seconds")
#         finally:
#             await scraper.cleanup()


# if __name__ == "__main__":
#     asyncio.run(main())