import asyncio
import aiohttp
import orjson
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import os
import json

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s', datefmt='%H:%M:%S')
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

    def __init__(self, 
                 symbol: str, 
                 channel: str = "ticker" ,):
        
        # Basic configuration
        self.symbol = symbol
        self.channel = channel
        self.api_url = "https://api.kucoin.com"
        
        # WebSocket state
        self.ws_connection = None
        self.ws_session = None
        self.is_running = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        
        # Data management
        self.queue = asyncio.Queue()
        self.stored_data = []
        self.messages_processed = 0
        # self.saving_path = saving_path
        
        # # Timing control
        # self.release_time = release_time
        # self.start_time = release_time - timedelta(seconds=pre_release_seconds)
        # self.end_time = release_time + timedelta(minutes=duration_minutes)
        self.collection_started = False
        self.collection_ended = False

        # logger.info(f"Initialized collector for {symbol} - Start: {self.get_formatted_time(self.start_time)}, "
        #            f"Release: {self.get_formatted_time(release_time)}, "
        #            f"End: {self.get_formatted_time(self.end_time)}")


    @staticmethod
    def get_formatted_time(dt: datetime = None) -> str:
        """Return formatted time string with microseconds"""
        if dt is None:
            dt = datetime.now()
        return dt.strftime('%H:%M:%S.%f')[:-2]
    
    ###########################################
    # establishing and maintaining websocket connection

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
        """Start the WebSocket connection with timing control"""
        try:
            ws_url = await self.get_websocket_url()
            if not ws_url:
                logger.error("Could not obtain WebSocket URL")
                return

            self.ws_session = aiohttp.ClientSession()
            self.ws_connection = await self.ws_session.ws_connect(
                ws_url,
                heartbeat=20,
                autoping=True,
                receive_timeout=30
            )

            # Add ping message handling
            asyncio.create_task(self._keep_alive())
            

            subscribe_data = orjson.dumps(self.get_subscription_data()).decode('utf-8')
            await self.ws_connection.send_str(subscribe_data)
            
            self.is_running = True
            self.collection_started = True
            logger.info("WebSocket connection established")
            
            await self._process_messages()

        except Exception as e:
            logger.error(f"Error in WebSocket connection: {e}")
            if self.reconnect_attempts < self.max_reconnect_attempts:
                self.reconnect_attempts += 1
                logger.info(f"Attempting reconnection ({self.reconnect_attempts}/{self.max_reconnect_attempts})")
                await asyncio.sleep(2 ** self.reconnect_attempts)  # Exponential backoff
                await self.start_websocket()
            else:
                logger.error("Max reconnection attempts reached")
    
    async def _keep_alive(self):
        """Maintain connection with explicit ping/pong"""
        while self.is_running:
            try:
                await self.ws_connection.ping()
                await asyncio.sleep(15)  # Send ping every 15 seconds
            except Exception as e:
                logger.error(f"Error in keep_alive: {e}")
                break


# process initial messsages from websocket 
    async def _process_messages(self):
        """Process incoming WebSocket messages with optimized speed and timing"""

        while self.is_running:
            try:
                msg = await self.ws_connection.receive(timeout=0.1)
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = orjson.loads(msg.data)
                    
                    # Update heartbeat time for any valid message
                    last_heartbeat = datetime.now()
                    
                    if data.get('type') == 'pong':
                        continue


                    if data.get('type') == 'message' and 'data' in data:
                        precise_time = datetime.now()
                        processed_data = data['data']
                        processed_data['time_received'] = self.get_formatted_time(precise_time)
                        
                        await self.queue.put(processed_data)

                        


                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    logger.warning("WebSocket connection closed")
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error("WebSocket error")
                    break

                # Check heartbeat
                if (datetime.now() - last_heartbeat).total_seconds() > 30:
                    logger.warning("No heartbeat received, reconnecting...")
                    await self.cleanup()
                    await self.start_websocket()
                    break

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                if datetime.now() <= self.end_time:
                    await self.cleanup()
                    await self.start_websocket()
                break

        self.collection_ended = True
        logger.info("Message processing completed")

# end of websocket connect and maintain code 
#############################################









    # async def get_final_snapshot(self,symbol: str):
    #     api_url = f"https://api.kucoin.com/api/v1/market/stats?symbol={symbol}-USDT"
        
    #     def convert_timestamp_final_snapshot(timestamp: int) -> str:
    #         timestamp_in_seconds = timestamp / 1000
    #         dt_object = datetime.fromtimestamp(timestamp_in_seconds)
    #         human_readable_time = dt_object.strftime('%H:%M:%S.%f')[:-2]
    #         return human_readable_time

    #     async with aiohttp.ClientSession() as session:
    #         async with session.get(api_url) as response:
    #             if response.status == 200:
    #                 data_raw = await response.json()
    #                 data = data_raw['data']
    #                 final_snapshot ={
    #                     'time': convert_timestamp_final_snapshot(data['time']),
    #                     'symbol': data['symbol'],
    #                     'price_high': data['high'],
    #                     'price_last': data['last'],
    #                     'price_low': data['low'],
    #                 }
    #                 return final_snapshot
                    
    #             else:
    #                 logger.error(f"Failed to get snapshot for {symbol}: {response.status}")
    #                 return None


    async def cleanup(self):
        """Clean up resources"""
        self.is_running = False
        
        if self.ws_connection and not self.ws_connection.closed:
            await self.ws_connection.close()
        
        if self.ws_session and not self.ws_session.closed:
            await self.ws_session.close()
        
        logger.info("Cleanup completed")







################################
# testing code



async def main():

    symbol = "BTC"  # Replace with the desired symbol
    ws = KucoinWebsocketlisten(symbol=symbol, channel=KucoinWebsocketlisten.CHANNEL_TICKER)
    asyncio.create_task(ws.start_websocket())
    end_time_test = datetime.now() + timedelta(seconds=120)
    
    print('end_time_test:',end_time_test)
    while datetime.now() < end_time_test:
        data = await ws.queue.get()
        await asyncio.sleep(2)
        logger.info(data)

    await ws.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
