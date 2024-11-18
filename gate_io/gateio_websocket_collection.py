import aiohttp
import asyncio
import time
import logging
import orjson
from datetime import datetime, timedelta
from typing import Optional, Dict
import traceback
import base64
import hmac
import hashlib
import ssl

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class GateIO_websocket_collection:
    def __init__(self, api_key: str = None, api_secret: str = None):
        """
        Initialize the WebSocket scraper with basic configuration
        Args:
            api_key: Optional API key for private channels
            api_secret: Optional API secret for private channels
        """
        self.public_ws_url = "wss://ws.gateio.io/v4/"
        self.api_key = api_key
        self.api_secret = api_secret
        self.price_found = asyncio.Event()
        self.final_price = None
        self.ws_connection = None
        self.ws_session = None
        self.ping_task = None
        self.request_id = 1

    async def initialize_websocket(self, symbol: str, use_private: bool = False, 
                                 use_ticker: bool = False, use_trades: bool = False, 
                                 use_depth: bool = False) -> bool:
        """
        Initialize WebSocket connection and subscribe to specified channels
        """
        try:
            # Create SSL context that doesn't verify certificates
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            # Create connector with the SSL context
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            
            # Create session with the connector
            self.ws_session = aiohttp.ClientSession(connector=connector)
            
            # Connect to WebSocket with SSL context
            self.ws_connection = await self.ws_session.ws_connect(
                self.public_ws_url,
                ssl=ssl_context
            )
            
            if use_private and not await self._login():
                await self.cleanup()
                return False

            self.ping_task = asyncio.create_task(self.websocket_ping())
            
            subscriptions = []
            if use_ticker:
                subscriptions.append({
                    "id": self._get_next_id(),
                    "method": "ticker.subscribe",
                    "params": [symbol]
                })
            
            if use_trades:
                subscriptions.append({
                    "id": self._get_next_id(),
                    "method": "trades.subscribe",
                    "params": [symbol]
                })
            
            if use_depth:
                subscriptions.append({
                    "id": self._get_next_id(),
                    "method": "depth.subscribe",
                    "params": [symbol, 5, "0.0001"]
                })

            for sub in subscriptions:
                await self.ws_connection.send_str(orjson.dumps(sub).decode('utf-8'))
                async for msg in self.ws_connection:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = orjson.loads(msg.data)
                        if data.get('error'):
                            logger.error(f"Subscription failed: {data.get('error')}")
                            return False
                        elif data.get('result', {}).get('status') == 'success':
                            break

            return True

        except Exception as e:
            logger.error(f"WebSocket initialization error: {str(e)}")
            await self.cleanup()
            return False

    async def cleanup(self):
        """Clean up WebSocket resources and connections"""
        try:
            if self.ping_task and not self.ping_task.done():
                self.ping_task.cancel()
                try:
                    await self.ping_task
                except asyncio.CancelledError:
                    pass

            if self.ws_connection and not self.ws_connection.closed:
                await self.ws_connection.close()
                self.ws_connection = None

            if self.ws_session and not self.ws_session.closed:
                await self.ws_session.close()
                self.ws_session = None
                
        except Exception as e:
            logger.error(f"Error in cleanup: {str(e)}")

    def _get_next_id(self) -> int:
        current_id = self.request_id
        self.request_id += 1
        return current_id

    async def websocket_ping(self):
        """Maintain WebSocket connection with periodic ping messages"""
        try:
            while True:
                if self.ws_connection and not self.ws_connection.closed:
                    ping_data = {
                        "id": self._get_next_id(),
                        "method": "server.ping",
                        "params": []
                    }
                    await self.ws_connection.send_str(orjson.dumps(ping_data).decode('utf-8'))
                await asyncio.sleep(15)
        except Exception as e:
            logger.error(f"Ping error: {str(e)}")
            if self.ws_connection and not self.ws_connection.closed:
                await self.ws_connection.close()

    async def get_market_data(self, symbol: str, channel_type: str, 
                            max_wait_time: int = 2, release_time: datetime = None) -> Optional[Dict]:
        """
        Get market data through WebSocket connection
        """
        logger.info(f'Release time and date: {release_time.strftime("%d-%m-%Y %H:%M:%S.%f")[:-3]}')
        
        try:
            use_ticker = channel_type == 'ticker'
            use_trades = channel_type == 'trades'
            use_depth = channel_type == 'depth'
            
            success = await self.initialize_websocket(
                symbol, 
                use_ticker=use_ticker,
                use_trades=use_trades,
                use_depth=use_depth
            )
            
            if not success:
                logger.error("Failed to initialize WebSocket")
                return None

            end_time = release_time + timedelta(seconds=max_wait_time)
            await self.wait_until_listing(release_time)

            while datetime.now() < release_time:
                await asyncio.sleep(0.0001)

            async for msg in self.ws_connection:
                if datetime.now() > end_time:
                    logger.info("Reached max wait time")
                    break

                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = orjson.loads(msg.data)
                        logger.debug(f"Received data: {str(data)}")                            
                        if 'method' in data:
                            if data['method'] == f'{channel_type}.update':
                                logger.info(f"Received {channel_type} update: {str(data['params'])}")
                                self.final_price = data['params']
                                self.price_found.set()
                                return data['params']

                    except orjson.JSONDecodeError as e:
                        logger.error(f"Failed to decode JSON: {str(e)}")
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")

                await asyncio.sleep(0.0001)

            logger.debug("No data found after all retry attempts")
            return None
            
        except Exception as e:
            logger.error(f"Error in retrieving data loop: {str(e)}")
            traceback.print_exc()
            return None


    async def wait_until_listing(self, release_time: datetime):
        """Wait until the specified release time"""
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

async def main():
    """Example usage"""
    with open('/root/trading_systems/gate_io/api_creds.json', 'r') as f:
        creds = orjson.loads(f.read())
    api_key = creds['api_key']
    api_secret = creds['api_secret']
    symbol = "BTC_USDT"
    
    websocket = GateIO_websocket_collection(api_key, api_secret)
    
    try:
        release_time = datetime.now() + timedelta(seconds=5)
        

        result = await websocket.get_market_data(symbol, "ticker", max_wait_time=2, release_time=release_time)
        print(f"\nTicker Data: {result}")

        # result = await websocket.get_market_data(symbol, "trades", max_wait_time=2, release_time=release_time)
        # print(f"\nTrades Data: {result}")

        # result = await websocket.get_market_data(symbol, "depth", max_wait_time=2, release_time=release_time)
        # print(f"\nDepth Data: {result}")
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
    finally:
        await websocket.cleanup()
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
