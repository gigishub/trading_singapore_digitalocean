import asyncio
import aiohttp
import orjson
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import os
import json

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

class KucoinWebsocketlisten:
    CHANNEL_TICKER = "ticker"
    CHANNEL_LEVEL2 = "level2"
    CHANNEL_MATCH = "match"
    CHANNEL_DEPTH5 = "level2Depth5"
    CHANNEL_SNAPSHOT = "snapshot"
    CHANNEL_LEVEL1 = "level1"

    def __init__(self, symbol: str, channel: str = "ticker"):
        self.symbol = symbol
        self.channel = channel
        self.api_url = "https://api.kucoin.com"
        self.ws_connection = None
        self.ws_session = None
        self.is_running = False
        self.queue = asyncio.Queue()
        self.stored_data = []

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
        if self.channel == self.CHANNEL_DEPTH5:
            topic = f"/spotMarket/{self.channel}:{self.symbol}-USDT"
        elif self.channel == self.CHANNEL_SNAPSHOT:
            topic = f"/market/{self.channel}:{self.symbol}-USDT"
        elif self.channel == self.CHANNEL_LEVEL1:
            topic = f"/spotMarket/{self.channel}:{self.symbol}-USDT"
        else:
            topic = f"/market/{self.channel}:{self.symbol}-USDT"
        
        return {
            "id": int(datetime.now().timestamp() * 1000),
            "type": "subscribe",
            "topic": topic,
            "privateChannel": False,
            "response": True
        }

    async def start_websocket(self):
        """Start the WebSocket connection and begin processing messages"""
        try:
            ws_url = await self.get_websocket_url()
            if not ws_url:
                logger.error("Could not obtain WebSocket URL")
                return

            self.ws_session = aiohttp.ClientSession()
            self.ws_connection = await self.ws_session.ws_connect(
                ws_url, 
                heartbeat=20, 
                receive_timeout=0.1
            )

            subscribe_data = orjson.dumps(
                self.get_subscription_data()
            ).decode('utf-8')
            await self.ws_connection.send_str(subscribe_data)

            self.is_running = True
            await self._process_messages()
        except Exception as e:
            logger.error(f"Error starting WebSocket: {e}")


    async def _process_messages(self):
        """Process incoming WebSocket messages with optimized speed and precise timing"""
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
                    if data.get('type') == 'pong':
                        continue

                    # Check for actual message data
                    if data.get('type') == 'message' and 'data' in data:

                        try:
                            #record timing
                            precise_time = datetime.now()
                            processed_data = data['data']
                            processed_data['time_data_received'] = precise_time.strftime('%H:%M:%S.%f')[:-2]

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




    async def _process_messages_21_11_24(self):
        """Process incoming WebSocket messages and put them in the queue"""
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
                        processed_data['time_received'] = datetime.now().strftime('%H:%M:%S.%f')[:-5]
                        
                        await self.queue.put(processed_data)

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                break


    async def save_data(self, saving_path: str, release_time: datetime = datetime.now()):
        """Save the stored data to a file"""
        saving_dir = f"{release_time.strftime('%Y-%m-%d_%H-%M')}_{self.symbol}"
        full_path = os.path.join(saving_path, saving_dir)
        os.makedirs(full_path, exist_ok=True)

        # for data in self.stored_data:
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
                    print(data)
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
        ws_test = KucoinWebsocketlisten(symbol="BTC", channel="match")
        
        # Start the websocket connection
        websocket_task = asyncio.create_task(ws_test.start_websocket())
        
        # Process and save data
        data_task = asyncio.create_task(ws_test.process_for_saving())
        
        # Run for a specific duration
        await asyncio.sleep(5)  # Listen for 30 seconds
        
        # Cancel tasks and cleanup
        data_task.cancel()
        websocket_task.cancel()
        await ws_test.cleanup()
        
        # Save collected data
        await ws_test.save_data("test1234")
        
    except Exception as e:
        logger.error(f"Error processing data: {e}")

if __name__ == "__main__":
    asyncio.run(procestest())


















async def main():
    symbol = "BTC"
    # Create WebSocket instances for different channels
    match_ws = KucoinWebsocketlisten(symbol="BTC", channel="match")
    snapshot_ws = KucoinWebsocketlisten(symbol="BTC", channel="snapshot")
    level1_ws = KucoinWebsocketlisten(symbol="BTC", channel="level1")
    path_to_save = '/root/trading_systems/kucoin_dir/test_data22222'

    try:
        # Start WebSocket connections
        websocket_tasks = [
            asyncio.create_task(match_ws.start_websocket()),
            asyncio.create_task(snapshot_ws.start_websocket()),
            asyncio.create_task(level1_ws.start_websocket())
        ]
    

        # Start data processing tasks
        processing_tasks = [
            asyncio.create_task(match_ws.process_for_saving()),
            asyncio.create_task(snapshot_ws.process_for_saving()),
            asyncio.create_task(level1_ws.process_for_saving()),
        ]

            # Example of how to run for a specific duration
        await asyncio.sleep(3)  # Run for 30 seconds
        
        # Cancel all tasks
        for task in processing_tasks + websocket_tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*processing_tasks, *websocket_tasks, return_exceptions=True)

    except asyncio.CancelledError:
        logger.info("Main task cancelled")
    finally:
        # Cleanup
        await asyncio.gather(match_ws.save_data(path_to_save),
                             snapshot_ws.save_data(path_to_save),
                             level1_ws.save_data(path_to_save))

        await asyncio.gather(
            match_ws.cleanup(),
            snapshot_ws.cleanup(),
            level1_ws.cleanup()
        )





async def process_match_data(websocket: KucoinWebsocketlisten):
    """Example processor for match data"""
    try:
        while True:
            data = await websocket.queue.get()

            
            
    except Exception as e:
        logger.error(f"Error processing match data: {e}")   

async def process_snapshot_data(websocket: KucoinWebsocketlisten):
    """Example processor for snapshot data"""
    try:
        while True:
            data = await websocket.get_data()
            logger.info(f"Snapshot data: {data}")
            await asyncio.sleep(1)
            
    except Exception as e:
        logger.error(f"Error processing snapshot data: {e}")

async def process_level1_data(websocket: KucoinWebsocketlisten):
    """Example processor for level1 (BBO) data"""
    try:
        while True:
            data = await websocket.get_data()
            logger.info(f"Level1 data: {data}")
            await asyncio.sleep(4)
            
            
    except Exception as e:
        logger.error(f"Error processing level1 data: {e}")


async def main1():
    """
    Example showing different ways to use the WebSocket data streams
    """
    # Create WebSocket instances for different channels
    match_ws = KucoinWebsocketlisten(symbol="BTC", channel="match")
    snapshot_ws = KucoinWebsocketlisten(symbol="BTC", channel="snapshot")
    level1_ws = KucoinWebsocketlisten(symbol="BTC", channel="level1")



    try:
        # Start WebSocket connections
        websocket_tasks = [
            asyncio.create_task(match_ws.start_websocket()),
            asyncio.create_task(snapshot_ws.start_websocket()),
            asyncio.create_task(level1_ws.start_websocket())
        ]

        # Start data processing tasks
        processing_tasks = [
            asyncio.create_task(process_match_data(match_ws)),
            asyncio.create_task(process_snapshot_data(snapshot_ws)),
            asyncio.create_task(process_level1_data(level1_ws))
        ]

        # Example of how to run for a specific duration
        await asyncio.sleep(30)  # Run for 30 seconds
        
        # Cancel all tasks
        for task in processing_tasks + websocket_tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*processing_tasks, *websocket_tasks, return_exceptions=True)

    except asyncio.CancelledError:
        logger.info("Main task cancelled")
    finally:
        # Cleanup
        await asyncio.gather(
            match_ws.cleanup(),
            snapshot_ws.cleanup(),
            level1_ws.cleanup()
        )





# Alternative example: Simple single channel usage
async def simple_example():
    """Simple example using just one channel"""
    match_ws = KucoinWebsocketlisten(symbol="BTC", channel="match")
    
    try:
        # Start WebSocket in background
        websocket_task = asyncio.create_task(match_ws.start_websocket())
        while match_ws.is_running:
            data = await match_ws.queue.get()
            print(data)
        
            
        # # Process 5 trades then exit
        # for _ in range(5):
        #     data = await match_ws.queue.get()
        #     print(f"Trade: {data.get('price')} {data.get('size')}")
        #     await asyncio.sleep(1)
            
    finally:
        websocket_task.cancel()
        await match_ws.cleanup()

# if __name__ == "__main__":
# #     ## Run the main example
#      asyncio.run(main())
    
#       asyncio.run(simple_example())



