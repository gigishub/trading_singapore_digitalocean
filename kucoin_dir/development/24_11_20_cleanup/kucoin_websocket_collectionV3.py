import asyncio
import aiohttp
import orjson
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s', 
                    datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

class KucoinWebsocketCollection:
    CHANNEL_TICKER = "ticker"
    CHANNEL_LEVEL2 = "level2"
    CHANNEL_MATCH = "match"
    CHANNEL_DEPTH5 = "level2Depth5"

    def __init__(self, symbol: str, channel: str):
        self.symbol = symbol
        self.channel = channel
        self.api_url = "https://api.kucoin.com"
        self.ws_connection = None
        self.ws_session = None
        self.ping_task = None
        self.received_data: List[Dict[Any, Any]] = []
        self.data_queue = asyncio.Queue()
        self.is_running = False

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

    def get_subscription_data(self) -> dict:
        topic = f"/market/{self.channel}:{self.symbol}-USDT"
        if self.channel == self.CHANNEL_DEPTH5:
            topic = f"/spotMarket/{self.channel}:{self.symbol}-USDT"
        
        return {
            "id": int(datetime.now().timestamp() * 1000),
            "type": "subscribe",
            "topic": topic,
            "privateChannel": False,
            "response": True
        }

    async def initialize_websocket(self) -> bool:
        try:
            ws_url = await self.get_websocket_url()
            if not ws_url:
                return False

            self.ws_session = aiohttp.ClientSession()
            self.ws_connection = await self.ws_session.ws_connect(
                ws_url, 
                heartbeat=20, 
                receive_timeout=0.1
            )

            # Subscribe to channel
            subscribe_data = orjson.dumps(
                self.get_subscription_data()
            ).decode('utf-8')
            await self.ws_connection.send_str(subscribe_data)

            return True

        except Exception as e:
            logger.error(f"WebSocket initialization error: {e}")
            await self.cleanup()
            return False

    async def listen(self):
        """
        Listen to WebSocket updates and store data
        """
        if not await self.initialize_websocket():
            return

        self.is_running = True
        try:
            while self.is_running:
                try:
                    msg = await asyncio.wait_for(
                        self.ws_connection.receive(),
                        timeout=0.001
                    )

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = orjson.loads(msg.data)
                        
                        if data.get('type') == 'pong':
                            continue

                        if data.get('type') == 'message' and 'data' in data:
                            processed_data = data['data']
                            current_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                            processed_data['time_received'] = current_time
                            
                            #self.received_data.append(processed_data)
                            await self.data_queue.put(processed_data)

                except asyncio.TimeoutError:
                    continue

        except Exception as e:
            logger.error(f"Error in WebSocket listening: {e}")
        finally:
            await self.cleanup()

    async def process_data(self):
        """
        Process data from the queue
        """
        while self.is_running:
            try:
                data = await asyncio.wait_for(
                    self.data_queue.get(), 
                    timeout=1.0
                )
                
                current_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                data['time_processed'] = current_time
                
                # Example processing - just print or log
                logger.info(f"{self.channel} data: {json.dumps(data, indent=2)}")
                
                self.data_queue.task_done()
            except asyncio.TimeoutError:
                continue

    async def cleanup(self):
        """Clean up WebSocket resources"""
        self.is_running = False
        
        if self.ws_connection and not self.ws_connection.closed:
            await self.ws_connection.close()
        
        if self.ws_session and not self.ws_session.closed:
            await self.ws_session.close()

        # Optional: save received data to file
        if self.received_data:
            filename = f"{self.symbol}_{self.channel}_data.json"
            with open(filename, "w") as f:
                json.dump(self.received_data, f, indent=2)
            logger.info(f"Saved {len(self.received_data)} records to {filename}")




async def process_ticker_data(ticker_ws, shared_data):
    while True:
        data = await ticker_ws.data_queue.get()
        price = float(data.get('price', 0))
        shared_data['ticker_price'] = price
        print(f"Ticker price updated: {price}")
        ticker_ws.data_queue.task_done()

async def process_match_data(match_ws, shared_data):
    while True:
        data = await match_ws.data_queue.get()
        price = float(data.get('price', 0))
        shared_data['match_price'] = price
        print(f"Match price updated: {price}")
        match_ws.data_queue.task_done()

async def compare_prices(shared_data):
    while True:
        await asyncio.sleep(0.1)  # Check periodically
        ticker_price = shared_data.get('ticker_price')
        match_price = shared_data.get('match_price')

        if ticker_price is not None and match_price is not None:
            if ticker_price > match_price:
                print(f"Ticker price {ticker_price} is higher than match price {match_price}")
            elif ticker_price < match_price:
                print(f"Match price {match_price} is higher than ticker price {ticker_price}")
            else:
                print(f"Prices are equal: {ticker_price}")



async def main():
    symbol = "BTC"
    shared_data = {}

    ticker_ws = KucoinWebsocketCollection(symbol, KucoinWebsocketCollection.CHANNEL_TICKER)
    match_ws = KucoinWebsocketCollection(symbol, KucoinWebsocketCollection.CHANNEL_MATCH)

    # Start listening to the WebSockets
    await asyncio.gather(
        ticker_ws.listen(),
        match_ws.listen(),
    )

    # Start processing data
    await asyncio.gather(
        process_ticker_data(ticker_ws, shared_data),
        process_match_data(match_ws, shared_data),
        compare_prices(shared_data),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutting down...")