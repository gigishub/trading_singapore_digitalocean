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

class mexc_websocket_collection:
    def __init__(self):
        """Initialize the WebSocket scraper with basic configuration"""
        self.ws_url = "wss://wbs.mexc.com/ws"
        self.price_found = asyncio.Event()
        self.final_price = None
        self.ws_connection = None
        self.ws_session = None
        self.ping_task = None
        self.subscription_confirmed = asyncio.Event()

    async def websocket_ping(self, ws):
        """
        Maintain WebSocket connection with periodic ping messages
        Args:
            ws: WebSocket connection object
        """
        try:
            while True:
                ping_data = orjson.dumps({
                    "method": "PING"
                }).decode('utf-8')
                await ws.send_str(ping_data)
                await asyncio.sleep(20)  # Send ping every 20 seconds
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
                    if data.get('code') == 0 and data.get('msg'):
                        logger.info(f"Successfully subscribed to {symbol}")
                        self.subscription_confirmed.set()
                        return True
        except Exception as e:
            logger.error(f"Error waiting for subscription: {e}")
            return False
        return False

    async def initialize_websocket(self, symbol: str, stream_type: str = None) -> bool:
        """
        Initialize WebSocket connection and subscribe to specified stream
        Args:
            symbol: Trading pair symbol (e.g., 'BTC')
            stream_type: Type of stream to subscribe to
        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            self.ws_session = aiohttp.ClientSession()
            self.ws_connection = await self.ws_session.ws_connect(self.ws_url)
            
            # Start ping task to keep connection alive
            self.ping_task = asyncio.create_task(self.websocket_ping(self.ws_connection))

            # Format symbol
            formatted_symbol = f"{symbol}USDT"
            
            # Prepare subscription data based on stream type
            if stream_type == "trades":
                subscribe_data = {
                    "method": "SUBSCRIPTION",
                    "params": [f"spot@public.deals.v3.api@{formatted_symbol}"]
                }
            elif stream_type == "bookticker":
                subscribe_data = {
                    "method": "SUBSCRIPTION",
                    "params": [f"spot@public.bookTicker.v3.api@{formatted_symbol}"]
                }
            elif stream_type == "depth":
                subscribe_data = {
                    "method": "SUBSCRIPTION",
                    "params": [f"spot@public.limit.depth.v3.api@{formatted_symbol}@5"]
                }

            await self.ws_connection.send_str(orjson.dumps(subscribe_data).decode('utf-8'))
            logger.info(f"Subscription request sent for {formatted_symbol}")

            # Wait for subscription confirmation with timeout
            try:
                subscription_confirmed = await asyncio.wait_for(
                    self.wait_for_subscription(formatted_symbol),
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

    async def get_price_websocket_trades(self, symbol: str, max_wait_time: int = 2, release_time: datetime = None) -> Optional[float]:
        """
        Get price through WebSocket trades channel
        Args:
            symbol: Trading pair symbol
            max_wait_time: Maximum time to wait for price in seconds
            release_time: DateTime object for when to start monitoring price
        Returns:
            Optional[float]: Price if found, None otherwise
        """
        logger.info('Release time and date: ' + release_time.strftime('%d-%m-%Y %H:%M:%S.%f')[:-3])

        success = await self.initialize_websocket(symbol, "trades")
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
                    logger.debug(f"Received message: {msg.data}")
                    if 'd' in data and 'deals' in data['d']:
                        try:
                            for deal in data['d']['deals']:
                                price = float(deal['p'])
                                if price > 0:
                                    logger.info(f"Price found via trades: {price}")
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

    async def get_price_websocket_bookticker(self, symbol: str, max_wait_time: int = 2, release_time: datetime = None) -> Optional[dict]:
        """
        Get price through WebSocket book ticker channel
        Args:
            symbol: Trading pair symbol
            max_wait_time: Maximum time to wait for price in seconds
            release_time: DateTime object for when to start monitoring price
        Returns:
            Optional[dict]: Best bid/ask prices and quantities if found, None otherwise
        """
        logger.info('Release time and date: ' + release_time.strftime('%d-%m-%Y %H:%M:%S.%f')[:-3])

        success = await self.initialize_websocket(symbol, "bookticker")
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
                    logger.debug(f"Received message: {msg.data}")
                    data = orjson.loads(msg.data)
                    if 'd' in data:
                        try:
                            ticker_data = {
                                'best_ask': float(data['d']['a']),
                                'best_bid': float(data['d']['b']),
                                'best_ask_qty': float(data['d']['A']),
                                'best_bid_qty': float(data['d']['B'])
                            }
                            logger.info(f"Book ticker data found: {ticker_data}")
                            self.final_price = ticker_data
                            self.price_found.set()
                            return ticker_data
                        except (KeyError, ValueError) as e:
                            logger.error(f"Failed to parse book ticker: {e}")
                            continue
        except Exception as e:
            logger.error(f"Error in retrieving book ticker: {e}")
            traceback.print_exc()

        return None

    async def get_price_websocket_depth(self, symbol: str, max_wait_time: int = 2, release_time: datetime = None) -> Optional[dict]:
        """
        Get price through WebSocket depth channel
        Args:
            symbol: Trading pair symbol
            max_wait_time: Maximum time to wait for price in seconds
            release_time: DateTime object for when to start monitoring price
        Returns:
            Optional[dict]: Depth data if found, None otherwise
        """
        logger.info('Release time and date: ' + release_time.strftime('%d-%m-%Y %H:%M:%S.%f')[:-3])

        success = await self.initialize_websocket(symbol, "depth")
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
                    logger.debug(f"Received message: {msg.data}")
                    data = orjson.loads(msg.data)
                    if 'd' in data and 'asks' in data['d']:
                        try:
                            depth_data = {
                                'asks': [(float(ask['p']), float(ask['v'])) for ask in data['d']['asks']],
                                'version': data['d']['r']
                            }
                            logger.info(f"Depth data found: {depth_data}")
                            self.final_price = depth_data
                            self.price_found.set()
                            return depth_data
                        except (KeyError, ValueError) as e:
                            logger.error(f"Failed to parse depth data: {e}")
                            continue
        except Exception as e:
            logger.error(f"Error in retrieving depth data: {e}")
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

# Example usage in main
async def main():
    symbol = 'FUSE'
    scraper = mexc_websocket_collection()
    release_date_time = datetime.now() + timedelta(seconds=60)

    try:
    #     # Example using trades stream
    #     trade_data = await scraper.get_price_websocket_trades(symbol, max_wait_time=10, release_time=release_date_time)
    #     if trade_data:
    #         print(f"Successfully retrieved {symbol} trade data: {trade_data}")

    #     # Example using book ticker stream
    #     book_data = await scraper.get_price_websocket_bookticker(symbol, max_wait_time=10, release_time=release_date_time)
    #     if book_data:
    #         print(f"Successfully retrieved {symbol} book ticker data: {book_data}")

        # Example using depth stream
        depth_data = await scraper.get_price_websocket_depth(symbol, max_wait_time=10, release_time=release_date_time)
        if depth_data:
            print(f"Successfully retrieved {symbol} depth data: {depth_data}")

    finally:
        await scraper.cleanup()

if __name__ == "__main__":
    asyncio.run(main())