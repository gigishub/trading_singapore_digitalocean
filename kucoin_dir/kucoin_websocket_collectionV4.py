import asyncio
import aiohttp
import orjson
import logging
from datetime import datetime
from typing import AsyncGenerator, Dict, Any, Optional

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s', 
                    datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

class KucoinWebsocketlisten:
    CHANNEL_TICKER = "ticker"
    CHANNEL_LEVEL2 = "level2"
    CHANNEL_MATCH = "match"
    CHANNEL_DEPTH5 = "level2Depth5"

    def __init__(self, symbol: str, channel: str = "ticker"):
        self.symbol = symbol
        self.channel = channel
        self.api_url = "https://api.kucoin.com"
        self.ws_connection = None
        self.ws_session = None
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

    async def continuous_data_stream(self) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Continuously yield ticker data for the specified symbol
        
        Usage:
            async for data in websocket.continuous_data_stream():
                process_data(data)
        """
        try:
            # Get WebSocket URL and establish connection
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

            # Subscribe to channel
            subscribe_data = orjson.dumps(
                self.get_subscription_data()
            ).decode('utf-8')
            await self.ws_connection.send_str(subscribe_data)

            self.is_running = True
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
                            processed_data['time_received'] = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                            
                            yield processed_data

                except asyncio.TimeoutError:
                    continue

        except Exception as e:
            logger.error(f"Error in WebSocket stream: {e}")
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Clean up WebSocket resources"""
        self.is_running = False
        
        if self.ws_connection and not self.ws_connection.closed:
            await self.ws_connection.close()
        
        if self.ws_session and not self.ws_session.closed:
            await self.ws_session.close()

async def main():
   
    #     await websocket.cleanup()

    # Example usage
    websocket_match = KucoinWebsocketlisten(symbol="BTC", channel="match")
    task_match = asyncio.create_task(process_websocket_data(websocket_match))
    await task_match
    
async def process_websocket_data(websocket):
    try:
        counter = 0
        async for data in websocket.continuous_data_stream():
            if float(data.get('price')):
                print(data)
                print(f"Price is {data.get('price')}")
                print(f"Time received: {data.get('time_received')}")
                await asyncio.sleep(3)
                print(f'{datetime.now().strftime("%H:%M:%S")}: Waiting for next data{websocket.channel}')
                counter += 1
                print(f"Counter: {counter}")
                if counter == 5:
                    print(f'break {websocket.channel}')
                    break 
    except KeyboardInterrupt:
        print("Stopped by user")
    finally:
        await websocket.cleanup()


if __name__ == "__main__":
    asyncio.run(main())




    ## access more then one socket 
    # websocket_match = KucoinWebsocketlisten(symbol="BTC", channel="match")
    # websocket_ticker = KucoinWebsocketlisten(symbol="BTC", channel="ticker")

    # task_match = asyncio.create_task(process_websocket_data(websocket_match))
    # task_ticker = asyncio.create_task(process_websocket_data(websocket_ticker))

    # task_match, task_ticker = await asyncio.gather(task_match, task_ticker)