import asyncio
import aiohttp
import orjson
import logging
import os
import json
from datetime import datetime
from typing import Dict, Any, Optional

# Configure logging (similar to KuCoin example)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

class BitgetWebsocketListen:
    # Define channel constants similar to KuCoin
    CHANNEL_TICKER = "ticker"
    CHANNEL_TRADE = "trade"
    CHANNEL_DEPTH = "books5"

    def __init__(self, symbol: str, channel: str = "ticker", api_key: str = None, api_secret: str = None, passphrase: str = None):
        self.symbol = symbol
        self.channel = channel
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        
        # Public WebSocket URL
        self.ws_url = "wss://ws.bitget.com/v2/ws/public"
        
        # WebSocket connection attributes
        self.ws_connection = None
        self.ws_session = None
        self.is_running = False
        self.queue = asyncio.Queue()
        self.stored_data = []

    async def start_websocket(self):
        """Start the WebSocket connection and begin processing messages"""
        try:
            # Create WebSocket session
            self.ws_session = aiohttp.ClientSession()
            self.ws_connection = await self.ws_session.ws_connect(
                self.ws_url, 
                heartbeat=20, 
                receive_timeout=0.1
            )

            # Prepare subscription data
            subscribe_data = self._get_subscription_data()
            await self.ws_connection.send_str(orjson.dumps(subscribe_data).decode('utf-8'))

            self.is_running = True
            await self._process_messages()
        except Exception as e:
            logger.error(f"Error starting WebSocket: {e}")

    def _get_subscription_data(self) -> dict:
        """Generate subscription data based on channel and symbol"""
        formatted_symbol = f"{self.symbol}USDT"
        
        # Channel mapping similar to KuCoin's approach
        channel_map = {
            self.CHANNEL_TICKER: "ticker",
            self.CHANNEL_TRADE: "trade",
            self.CHANNEL_DEPTH: "books5"
        }
        
        return {
            "op": "subscribe",
            "args": [{
                "instType": "SPOT",
                "channel": channel_map.get(self.channel, self.channel),
                "instId": formatted_symbol
            }]
        }

    async def _process_messages(self):
        """Process incoming WebSocket messages with optimized speed"""
        while self.is_running:
            try:
                # Use a very short timeout to minimize blocking
                msg = await asyncio.wait_for(
                    self.ws_connection.receive(),
                    timeout=0.001  # Extremely short timeout
                )

                if msg.type == aiohttp.WSMsgType.TEXT:
                    # Use orjson for fastest JSON parsing
                    data = orjson.loads(msg.data)
                    
                    # Skip pong messages quickly
                    if isinstance(data, dict) and data.get('op') == 'pong':
                        continue

                    # Check for actual message data
                    if 'data' in data:
                        try:
                            # Record precise timing
                            precise_time = datetime.now()
                            processed_data = data['data']
                            #print(f'{json.dumps(processed_data,indent= 4)}')
                            # Add timestamp similar to KuCoin implementation
                            processed_data[0]['time_data_received'] = precise_time.strftime('%H:%M:%S.%f')[:-2]

                            # Process the data and put it in the queue
                            self.queue.put_nowait(processed_data)
                        except asyncio.QueueFull:
                            logger.warning("Queue is full, message may be dropped")

            except asyncio.TimeoutError:
                # Immediately continue to next iteration
                continue
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                break

    async def save_data(self, saving_path: str, release_time: datetime = datetime.now()):
        """Save the stored data to a file"""
        saving_dir = f"{release_time.strftime('%Y-%m-%d_%H-%M')}_{self.symbol}"
        full_path = os.path.join(saving_path, saving_dir)
        os.makedirs(full_path, exist_ok=True)

        filename = os.path.join(full_path, f"{self.symbol}_{self.channel}_data.json")
        with open(filename, "w") as f:
            json.dump(self.stored_data, f, indent=2)
        logger.info(f"Data saved to directory: {full_path}")

    async def process_for_saving(self):
        """Process and save data to a list"""
        try:    
            while True:
                try:
                    data = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                    logger.debug(f"Received data: {data}")
                    self.stored_data.append(data)
                except asyncio.TimeoutError:
                    # Check if connection is still running
                    if not self.is_running:
                        break
        except Exception as e:
            logger.error(f"Error processing data: {e}")

    async def get_data(self) -> Dict[str, Any]:
        """Get the next data item from the queue"""
        return await self.queue.get()

    async def cleanup(self):
        """Clean up WebSocket resources"""
        self.is_running = False
        
        if self.ws_connection and not self.ws_connection.closed:
            await self.ws_connection.close()
        
        if self.ws_session and not self.ws_session.closed:
            await self.ws_session.close()

async def procestest():
    try:    
        ws_test = BitgetWebsocketListen(symbol="BTC", channel="books5")
        
        # Start the websocket connection
        websocket_task = asyncio.create_task(ws_test.start_websocket())
        
        # Process and save data
        data_task = asyncio.create_task(ws_test.process_for_saving())
        
        # Run for a specific duration
        await asyncio.sleep(5)  # Listen for 5 seconds
        
        # Cancel tasks and cleanup
        data_task.cancel()
        websocket_task.cancel()
        await ws_test.cleanup()
        
        # Save collected data
        await ws_test.save_data("/root/trading_systems/bit_channel_data/bitget")
        
    except Exception as e:
        logger.error(f"Error processing data: {e}")

if __name__ == "__main__":
    asyncio.run(procestest())