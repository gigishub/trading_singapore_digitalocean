import asyncio
import json
from typing import List, Any
from kucoin_dir.kucoin_websocket_collectionV2 import KucoinWebsocketCollection
import orjson
import aiohttp
class KucoinWebsocketDataManager:
    def __init__(self):
        self.data_queue = asyncio.Queue()
        self.received_data: List[Any] = []

    async def listen_to_updates(self, symbol: str, channel: str):
        """
        Listen to WebSocket updates and store data
        """
        scraper = KucoinWebsocketCollection()
        
        try:
            # Initialize WebSocket connection
            success = await scraper.initialize_websocket(symbol, channel)
            if not success:
                return

            while True:
                try:
                    msg = await asyncio.wait_for(
                        scraper.ws_connection.receive(),
                        timeout=0.001
                    )

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = orjson.loads(msg.data)
                        print(msg.data)
                        
                        if data.get('type') == 'pong':
                            continue

                        if data.get('type') == 'message' and 'data' in data:
                            # Store data in queue and list
                            processed_data = data['data']
                            await self.data_queue.put(processed_data)
                            self.received_data.append(processed_data)

                except asyncio.TimeoutError:
                    continue

        except Exception as e:
            print(f"Error in WebSocket listening: {e}")
        finally:
            await scraper.cleanup()

    async def process_data(self):
        """
        Process data from the queue
        """
        while True:
            # Wait for and get data from queue
            data = await self.data_queue.get()
            
            # Process or print the data
            print(f"Received data: {json.dumps(data, indent=2)}")
            
            # Optional: You can do more complex processing here
            self.data_queue.task_done()


async def main():
    try:
        # Create the data manager
        data_manager = KucoinWebsocketDataManager()
        channel ="level2"
        # Create tasks for listening and processing
        listen_task = asyncio.create_task(
            data_manager.listen_to_updates(symbol="URO", channel=channel)
        )
        process_task = asyncio.create_task(
            data_manager.process_data()
        )

        # Wait for tasks to complete (or run indefinitely)
        await asyncio.gather(listen_task, process_task)
    finally:
        with open(f"data_{channel}.json", "w") as f:
            json.dump(data_manager.received_data, f)
        if KeyboardInterrupt:
            print("Shutting down...")


if __name__ == "__main__":
    asyncio.run(main())