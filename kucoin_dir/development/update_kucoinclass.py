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

class KucoinWebSocketScraper:
    def __init__(self):
        """Initialize the WebSocket scraper with basic configuration"""
        self.api_url = "https://api.kucoin.com"
        self.price_found = asyncio.Event()
        self.final_price = None
        self.ws_connection = None
        self.ws_session = None
        self.ping_task = None

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
                # Convert orjson bytes to str for WebSocket
                ping_data = orjson.dumps({"id": int(time.time() * 1000), "type": "ping"}).decode('utf-8')
                await ws.send_str(ping_data)
                await asyncio.sleep(20)
        except Exception as e:
            logger.error(f"Ping error: {e}")

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
            
            # Convert orjson bytes to str for WebSocket
            subscribe_data = orjson.dumps({
                "id": int(time.time() * 1000),
                "type": "subscribe",
                "topic": f"/market/ticker:{symbol}-USDT",
                "privateChannel": False,
                "response": True
            }).decode('utf-8')
            
            await self.ws_connection.send_str(subscribe_data)
            
            # Log WebSocket initialization success
            logger.info(f"WebSocket initialized and subscribed to {symbol}-USDT ticker")
            return True
        except Exception as e:
            logger.error(f"WebSocket initialization error: {e}")
            await self.cleanup()
            return False


    async def wait_until_listing(self,release_time: datetime):

        initiation_time_print_flag = False
        while True:
            time_waiting_in_seconds = (release_time - datetime.now()).total_seconds()

            # keep adjusting the time to listing for accuracy and safeing resources
            if 25 < time_waiting_in_seconds > 0:
                if not initiation_time_print_flag:
                    initiation_time_print_flag = True
                    logger.info(f' Waiting {time_waiting_in_seconds:.2f} seconds until token release time')
                await asyncio.sleep(5)

            # 15 seconds left until token release
            if 25 > time_waiting_in_seconds > 0:
                logger.info(f'25 seconds left until token release')
                break


    async def get_price_websocket(self, symbol: str, max_wait_time: int = 2, release_time: datetime = None) -> Optional[float]:
        """
        Get price through WebSocket connection with retry mechanism
        Args:
            symbol: Trading pair symbol
            max_wait_time: Maximum time to wait for price in seconds
            max_retries: Maximum number of retry attempts
        Returns:
            Optional[float]: Price if found, None otherwise
        """
        logger.info('Release time and date: ' + release_time.strftime('%d-%m-%Y %H:%M:%S.%f')[:-3])
        await self.initialize_websocket(symbol)

        if not self.ws_connection:
            logger.error("WebSocket not initialized")
            return None

        end_time = release_time + timedelta(seconds=max_wait_time)
        
        await self.wait_until_listing(release_time)

        while datetime.now() < release_time:
            await asyncio.sleep(0.0001)

        try:

            async for msg in self.ws_connection:
                if datetime.now() > end_time:
                    break
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = orjson.loads(msg.data)
                    if data.get('type') == 'message' and 'data' in data:
                        try:
                            price = float(data['data']['price'])
                            if price > 0:
                                logger.info(
                                    f"Price found via WebSocket: {price} at "
                                    f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]}"
                                )
                                self.final_price = price
                                self.price_found.set()
                                return price
                        except (KeyError, ValueError) as e:
                            logger.debug(f"Failed to parse price: {e}")
                            continue
                    await asyncio.sleep(0.0001)
            
        except Exception as e:
            logger.error(f"Error in retriving price loop {e}")
            traceback.print_exc()

        return None


    async def cleanup(self):
        """Clean up WebSocket resources and connections"""
        if self.ping_task:
            self.ping_task.cancel()
            try:
                await self.ping_task
            except asyncio.CancelledError:
                pass
            
            
        if self.ws_connection:
            await self.ws_connection.close()
            
        if self.ws_session:
            await self.ws_session.close()



# # Example usage
# async def main():

#     from datetime import timedelta

#     symbol = "BTC"  # Example symbol
#     scraper = KucoinWebSocketScraper()
#     #release_time = datetime(2024, 11, 1, 13, 30, 0)  # Example fixed datetime

#     release_time = datetime.now() + timedelta(seconds=10)  # Example release time


#     try:
#         price = await scraper.get_price_websocket(symbol, max_wait_time=2, release_time =release_time)
#         if price:
#             print(f"Successfully retrieved {symbol} price: {price}")
#         else:
#             print("No price found after all retry attempts")
#     finally:
#         await scraper.cleanup()


# if __name__ == "__main__":
#     asyncio.run(main())