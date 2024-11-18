import aiohttp
import asyncio
import time
import logging
import orjson
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

# Create a dedicated logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

class KucoinWebsocketCollection:
    # Define channel types as simple class variables
    CHANNEL_TICKER = "ticker"          # Basic price updates
    CHANNEL_LEVEL2 = "level2"          # Order book updates
    CHANNEL_MATCH = "match"            # Trade matches
    CHANNEL_DEPTH5 = "level2Depth5"    # Top 5 bids and asks

    def __init__(self):
        self.api_url = "https://api.kucoin.com"
        self.price_found = asyncio.Event()
        self.final_price = None
        self.ws_connection = None
        self.ws_session = None
        self.ping_task = None
        self.subscription_confirmed = asyncio.Event()

    async def get_token(self) -> Optional[str]:
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
        token = await self.get_token()
        return f"wss://ws-api-spot.kucoin.com/?token={token}" if token else None

    async def websocket_ping(self, ws):
        try:
            while True:
                if ws.closed:
                    break
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

    def get_subscription_data(self, symbol: str, channel: str) -> dict:
        # Use level2Depth5 channel differently as it needs a different topic prefix
        if channel == self.CHANNEL_DEPTH5:
            topic = f"/spotMarket/{channel}:{symbol}-USDT"
        else:
            topic = f"/market/{channel}:{symbol}-USDT"
        
        return {
            "id": int(time.time() * 1000),
            "type": "subscribe",
            "topic": topic,
            "privateChannel": False,
            "response": True
        }

    async def initialize_websocket(self, symbol: str, channel: str) -> bool:
        try:
            # Get WebSocket URL and establish connection
            ws_url = await self.get_websocket_url()
            if not ws_url:
                return False

            self.ws_session = aiohttp.ClientSession()
            self.ws_connection = await self.ws_session.ws_connect(
                ws_url,
                heartbeat=20,
                receive_timeout=0.1
            )

            # Start ping task
            self.ping_task = asyncio.create_task(self.websocket_ping(self.ws_connection))

            # Subscribe to channel
            subscribe_data = orjson.dumps(
                self.get_subscription_data(symbol, channel)
            ).decode('utf-8')
            await self.ws_connection.send_str(subscribe_data)

            # Wait for subscription confirmation
            try:
                subscription_confirmed = await asyncio.wait_for(
                    self._wait_for_subscription(),
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

    async def _wait_for_subscription(self) -> bool:
        try:
            async for msg in self.ws_connection:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = orjson.loads(msg.data)
                    if data.get('type') == 'ack' or (data.get('type') == 'message' and 'data' in data):
                        self.subscription_confirmed.set()
                        return True
        except Exception as e:
            logger.error(f"Error waiting for subscription: {e}")
        return False

    def _extract_price_data(self, data: Dict[str, Any], channel: str) -> Optional[Any]:
        """Extract price data based on channel type"""
        try:
            if channel == self.CHANNEL_TICKER:
                return data['data']
            elif channel == self.CHANNEL_LEVEL2:
                return data['data']
            elif channel == self.CHANNEL_MATCH:
                return data['data']
            elif channel == self.CHANNEL_DEPTH5:
                return data['data']
        except (KeyError, ValueError) as e:
            logger.error(f"Failed to extract price data: {e}")
            return None

    async def get_price_at_release(
        self,
        symbol: str,
        release_time: datetime,
        channel: str = "ticker",  # Default to ticker channel
        max_wait_time: int = 2,
        start_monitoring_adjustment: int = 1
    ) -> Optional[Any]:
        """
        Get price data at token release time.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTC' for BTC-USDT)
            release_time: Exact datetime when token will be released
            channel: Channel to listen to. Options are:
                    - "ticker" (basic price updates)
                    - "level2" (order book updates)
                    - "match" (trade matches)
                    - "level2Depth5" (top 5 bids and asks)
            max_wait_time: Maximum seconds to wait for price after release
        """
        logger.info(f'Release time: {release_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')
        
        # Initialize WebSocket connection early
        success = await self.initialize_websocket(symbol, channel)
        if not success:
            return None

        # Start monitoring 1 second before release
        start_monitoring = release_time - timedelta(seconds=start_monitoring_adjustment)
        end_time = release_time + timedelta(seconds=max_wait_time)

        # Wait until close to release time
        current_time = datetime.now()
        if current_time < start_monitoring:
            await asyncio.sleep((start_monitoring - current_time).total_seconds())

        try:
            while True:
                if datetime.now() > end_time:
                    logger.info(f"{channel} reached max wait time")
                    break

                try:
                    msg = await asyncio.wait_for(
                        self.ws_connection.receive(),
                        timeout=0.001  # 1ms timeout for high-frequency checking
                    )

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = orjson.loads(msg.data)
                        
                        if data.get('type') == 'pong':
                            continue

                        if data.get('type') == 'message' and 'data' in data:
                            result = self._extract_price_data(data, channel)
                            if result is not None:
                                logger.debug(f"Data for {channel} channel received at {datetime.now().strftime('%H:%M:%S.%f')}: {result}")
                                self.final_price = result
                                self.price_found.set()
                                return result

                except asyncio.TimeoutError:
                    # If not yet release time, continue with minimal delay
                    if datetime.now() < release_time:
                        await asyncio.sleep(0.0001)
                    continue

        except Exception as e:
            logger.error(f"Error in price monitoring loop: {e}")
            return None
        finally:
            await self.cleanup()



    async def cleanup(self):
        """Clean up WebSocket resources"""
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





import asyncio
from datetime import datetime, timedelta
import json

# Example usage showing how to use different channels
async def main():
    symbol = "SWELL"
    release_time = datetime.now() + timedelta(seconds=3)
    release_time = release_time.replace(microsecond=0) 

    # Channels to use
    channel_names = ['ticker', 'level2', 'match', 'depth5']
    scrapers = []
    tasks = []

    # Create a new scraper object for each channel
    for channel_name in channel_names:
        scraper = KucoinWebsocketCollection()
        scrapers.append(scraper)
        channel = getattr(scraper, f'CHANNEL_{channel_name.upper()}')
        tasks.append(
            scraper.get_price_at_release(
                symbol=symbol,
                release_time=release_time,
                channel=channel
            )
        )

    try:
        # Run all tasks concurrently
        results = await asyncio.gather(*tasks)

        # Print results with visual separation
        for channel_name, result in zip(channel_names, results):
            print("=" * 65)
            print(f"Results from channel: {channel_name}")
            if result:
                json_output = json.dumps(result, indent=2)
                print(json_output)
            else:
                print("No data retrieved.")
        print("=" * 65)

    finally:
        # Clean up each scraper individually
        await asyncio.gather(*(scraper.cleanup() for scraper in scrapers))

if __name__ == "__main__":
    asyncio.run(main())




    
# #Example usage showing how to use different channels
# async def main():
#     symbol = "BTC"
#     release_time = datetime.now() + timedelta(seconds=3)
#     release_time = release_time.replace(microsecond=0) 
    
#     scraper = KucoinWebsocketOptimized()
#     try:
#         # # Example using ticker channel (basic price updates)
#         # result = await scraper.get_price_at_release(
#         #     symbol=symbol,
#         #     release_time=release_time,
#         #     channel=scraper.CHANNEL_TICKER  # or just use "ticker"
#         # )
        
#         # Example using level2 channel (order book updates)
#         # result = await scraper.get_price_at_release(
#         #     symbol=symbol,
#         #     release_time=release_time,
#         #     channel=scraper.CHANNEL_LEVEL2  # or just use "level2"
#         # )
        
#         # Example using match channel (trade matches)
#         # result = await scraper.get_price_at_release(
#         #     symbol=symbol,
#         #     release_time=release_time,
#         #     channel=scraper.CHANNEL_MATCH,
#         #     max_wait_time=10  # or just use "match"
#         # )
        
#         # #Example using depth5 channel (top 5 bids and asks)
#         # result = await scraper.get_price_at_release(
#         #     symbol=symbol,
#         #     release_time=release_time,
#         #     channel=scraper.CHANNEL_DEPTH5  # or just use "level2Depth5"
#         # )
        

            
#     finally:
#         await scraper.cleanup()

# if __name__ == "__main__":
#     asyncio.run(main())