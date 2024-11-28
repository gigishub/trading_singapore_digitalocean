import asyncio
import aiohttp
import orjson
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import time

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Can be changed to INFO for production
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

class KucoinWebsocketListen:
    # Channel constants
    CHANNEL_TICKER = "ticker"
    CHANNEL_LEVEL2 = "level2"
    CHANNEL_MATCH = "match"
    CHANNEL_DEPTH5 = "level2Depth5"
    CHANNEL_SNAPSHOT = "snapshot"
    CHANNEL_LEVEL1 = "level1"
    
    def __init__(self, symbol: str, channel: str = "ticker"):
        # Basic configuration
        self.symbol = symbol
        self.channel = channel
        self.api_url = "https://api.kucoin.com"
        
        # Performance optimized state management
        self.queue = asyncio.Queue(maxsize=100000)
        self._last_heartbeat = time.monotonic()
        self._connection_ready = asyncio.Event()
        self._warm_up_complete = asyncio.Event()

        
        # WebSocket state
        self.ws_connection = None
        self.ws_session = None
        self.is_running = False
        
        # Performance metrics (debug mode only)
        self._start_time = None
        self._latency_samples = []
        self._connection_quality = 100.0

        # Subscription data (pre-computed)
        self._subscription_data = self._prepare_subscription_data()

    def _prepare_subscription_data(self) -> dict:
        """Pre-compute subscription data for faster connection setup"""
        if self.channel == self.CHANNEL_DEPTH5:
            topic = f"/spotMarket/{self.channel}:{self.symbol}-USDT"
        elif self.channel == self.CHANNEL_SNAPSHOT:
            topic = f"/market/{self.channel}:{self.symbol}-USDT"
        elif self.channel == self.CHANNEL_LEVEL1:
            topic = f"/spotMarket/{self.channel}:{self.symbol}-USDT"
        else:
            topic = f"/market/{self.channel}:{self.symbol}-USDT"
        
        return {
            "type": "subscribe",
            "topic": topic,
            "privateChannel": False,
            "response": True
        }

    async def get_token(self) -> Optional[str]:
        """Optimized token retrieval with error handling"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.api_url}/api/v1/bullet-public",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    if response.status == 200:
                        data = await response.json(loads=orjson.loads)
                        return data['data']['token']
                    logger.error(f"Token retrieval failed with status: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Token retrieval error: {e}")
            return None

    async def warm_up(self):
        """Pre-launch connection warm-up and validation"""
        if logger.level <= logging.DEBUG:
            logger.debug("Starting warm-up sequence")
        
        try:
            # Test token retrieval
            token = await self.get_token()
            if not token:
                raise Exception("Failed to obtain valid token during warm-up")

            # Test connection establishment
            ws_url = f"wss://ws-api-spot.kucoin.com/?token={token}"
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ws_url) as ws:
                    # Measure baseline latency
                    start_time = time.monotonic()
                    await ws.ping()
                    await ws.pong()
                    latency = (time.monotonic() - start_time) * 1000

                    if logger.level <= logging.DEBUG:
                        logger.debug(f"Warm-up latency: {latency:.2f}ms")
                        self._latency_samples.append(latency)

            self._warm_up_complete.set()
            if logger.level <= logging.DEBUG:
                logger.debug("Warm-up completed successfully")
                
        except Exception as e:
            logger.error(f"Warm-up failed: {e}")
            raise

    async def direct_message_receive(self):
        try:
            msg = await self.ws_connection.receive()
            if msg.type == aiohttp.WSMsgType.TEXT:
                return orjson.loads(msg.data)
            return None
        except Exception as e:
            logger.error(f"Error in direct_message_receive: {e}")
            return None

    async def _process_message(self, msg_data: dict) -> None:
        """Optimized message processing with microsecond precision timing"""
        try:
            precise_time = datetime.now()
            
            # Fast path for data messages
            if msg_data.get('type') == 'message' and 'data' in msg_data:
                processed_data = msg_data['data']
                processed_data['time_received'] = precise_time.strftime('%H:%M:%S.%f')[:-3]
                try:
                    self.queue.put_nowait(processed_data)
                except asyncio.QueueFull:
                    logger.warning("Queue is full, dropping message")
                
                

            elif msg_data.get('type') == 'pong':
                self._last_heartbeat = time.monotonic()
                
        except Exception as e:
            logger.error(f"Message processing error: {e}")

    async def start(self):
        """Main entry point with warm-up and optimization"""
        try:
            # Perform warm-up if not done
            if not self._warm_up_complete.is_set():
                await self.warm_up()

            self._start_time = time.monotonic()
            await self._start_websocket()
            
        except Exception as e:
            logger.error(f"Start error: {e}")
            raise

    async def _start_websocket(self):
        """Optimized WebSocket connection handling"""
        try:
            token = await self.get_token()
            if not token:
                raise Exception("Failed to obtain WebSocket token")

            self.ws_session = aiohttp.ClientSession()
            self.ws_connection = await self.ws_session.ws_connect(
                f"wss://ws-api-spot.kucoin.com/?token={token}",
                heartbeat=20,
                receive_timeout=30
            )

            # Start background tasks
            self.is_running = True
            asyncio.create_task(self._keep_alive())
            
            # Send subscription
            await self.ws_connection.send_str(orjson.dumps(self._subscription_data).decode('utf-8'))
            self._connection_ready.set()
            
            if logger.level <= logging.DEBUG:
                logger.debug(f"WebSocket connected and subscribed to {self.channel}")
            
            await self._message_loop()

        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
            await self.cleanup()
            raise

    async def _message_loop(self):
        """Optimized message processing loop"""
        while self.is_running:
            try:
                msg = await self.ws_connection.receive(timeout=0.1)
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._process_message(orjson.loads(msg.data))
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    logger.warning(f"WebSocket state changed: {msg.type}")
                    break
                    
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Message loop error: {e}")
                break

    async def _keep_alive(self):
        """Optimized connection maintenance"""
        while self.is_running:
            try:
                if time.monotonic() - self._last_heartbeat > 15:
                    await self.ws_connection.ping()
                await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"Keep-alive error: {e}")
                break


    async def cleanup(self):
        """Resource cleanup"""
        self.is_running = False
        
        if self.ws_connection and not self.ws_connection.closed:
            await self.ws_connection.close()
        
        if self.ws_session and not self.ws_session.closed:
            await self.ws_session.close()
        
        self._log_performance_metrics()

    async def get_data(self) -> Dict[str, Any]:
        """Get latest message from the queue"""
        try:
            return self.queue.get_nowait()
        except asyncio.QueueEmpty:
            return None


    def _log_performance_metrics(self):
        """Log performance metrics in debug mode"""
        if self._start_time:
            avg_latency = sum(self._latency_samples) / len(self._latency_samples) if self._latency_samples else 0
            
            logger.info(f"Performance Metrics:")
            logger.info(f"Average latency: {avg_latency:.2f}ms")
            logger.info(f"Connection quality: {self._connection_quality:.1f}%")


async def main():
    import json
    ws = KucoinWebsocketListen(symbol="BTC", channel=KucoinWebsocketListen.CHANNEL_DEPTH5
                               )
    
    try:
        asyncio.create_task( ws.start())
        
        # Monitor for 2 minutes
        end_time = datetime.now() + timedelta(minutes=2)
        logger.debug(f'{end_time}')
        while datetime.now() < end_time:
            get_data = await ws.get_data()
            print(json.dumps(get_data,indent=4))
            
            await asyncio.sleep(1)


            
    finally:
        await ws.cleanup()
if __name__ == "__main__":
    asyncio.run(main())