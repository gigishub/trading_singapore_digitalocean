import aiohttp
import asyncio
import time
import logging
import orjson
from datetime import datetime
from typing import Optional
from datetime import timedelta
import traceback

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

class BitgetWebSocketScraper:
    def __init__(self):
        """Initialize the WebSocket scraper with basic configuration"""
        self.ws_url = "wss://ws.bitget.com/spot/v1/stream"
        self.price_found = asyncio.Event()
        self.final_price = None
        self.ws_connection = None
        self.ws_session = None
        self.ping_task = None


    async def initialize_websocket(self, symbol: str) -> bool:
        """
        Initialize WebSocket connection and subscribe to market ticker
        Args:
            symbol: Trading pair symbol (e.g., 'BTC')
        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            self.ws_session = aiohttp.ClientSession()
            self.ws_connection = await self.ws_session.ws_connect(self.ws_url)
            
            # Start ping task to keep connection alive
            self.ping_task = asyncio.create_task(self.websocket_ping(self.ws_connection))
            
            # Format symbol correctly for Bitget
            formatted_symbol = f"{symbol}USDT"
            
            # Subscribe to ticker
            subscribe_data = orjson.dumps({
                "op": "subscribe",
                "args": [{
                    "instType": "sp",
                    "channel": "ticker",
                    "instId": formatted_symbol
                }]
            }).decode('utf-8')
            
            await self.ws_connection.send_str(subscribe_data)
            logger.info(f"Subscription request sent for {formatted_symbol}")
            
            # Wait for subscription confirmation
            async for msg in self.ws_connection:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = orjson.loads(msg.data)
                    if 'event' in data and data['event'] == 'subscribe':
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
                # Just send ping without waiting for pong
                ping_data = orjson.dumps({"op": "ping"}).decode('utf-8')
                await ws.send_str(ping_data)
                await asyncio.sleep(20)
        except Exception as e:
            logger.error(f"Ping error: {str(e)}")
            # Don't raise the exception, just log it
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
                    logger.info(f' Waiting {time_waiting_in_seconds:.2f} seconds until token release time')
                await asyncio.sleep(5)

            if 25 > time_waiting_in_seconds > 0:
                logger.info(f'25 seconds left until token release')
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
        success = await self.initialize_websocket(symbol)
        
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
                    data = orjson.loads(msg.data)
                    
                    # Extract price from the message
                    if 'data' in data:
                        try:
                            price_data = data['data'][0]
                            if 'last' in price_data:
                                price = float(price_data['last'])
                                if price > 0:
                                    logger.info(
                                        f"Price found via WebSocket: {price} at "
                                        f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]}"
                                    )
                                    self.final_price = price
                                    self.price_found.set()
                                    return price
                        except (KeyError, ValueError, IndexError) as e:
                            logger.debug(f"Failed to parse price: {str(e)}")
                            continue

                await asyncio.sleep(0.0001)
            
        except Exception as e:
            logger.error(f"Error in retrieving price loop: {str(e)}")
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

    symbol = "ARCA"  # Example symbol
    scraper = BitgetWebSocketScraper()
    release_time = datetime(2024, 11, 4, 12, 37, 0)  # Example fixed datetime
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