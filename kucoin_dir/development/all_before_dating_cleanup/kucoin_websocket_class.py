#!/usr/bin/python3

import aiohttp
import asyncio
from datetime import datetime, timedelta
import orjson
import logging
from typing import Optional, Tuple
import websockets.exceptions

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

class FastKucoinWebSocket:
    def __init__(self, max_wait_time: int = 2):
        self.api_url = "https://api.kucoin.com"
        self.price_found = asyncio.Event()
        self.final_price = None
        self.session = None
        self.ws_conn = None
        self.max_wait_time = max_wait_time

    async def initialize(self):
        """Initialize single session"""
        self.session = aiohttp.ClientSession(json_serialize=lambda x: orjson.dumps(x).decode())
        
    async def get_ws_token(self) -> Optional[str]:
        """Get WebSocket token"""
        try:
            async with self.session.post(f"{self.api_url}/api/v1/bullet-public") as response:
                if response.status == 200:
                    data = await response.json(loads=orjson.loads)
                    return data['data']['token']
        except Exception as e:
            logger.error(f"Token error: {e}")
            return None

    async def connect_websocket(self, symbol: str) -> bool:
        """Establish WebSocket connection"""
        try:
            token = await self.get_ws_token()
            if not token:
                return False

            self.ws_conn = await self.session.ws_connect(
                f"wss://ws-api-spot.kucoin.com/?token={token}",
                heartbeat=20,
                autoclose=False
            )

            await self.ws_conn.send_json({
                "id": int(datetime.now().timestamp() * 1000),
                "type": "subscribe",
                "topic": f"/market/ticker:{symbol}-USDT",
                "privateChannel": False,
                "response": True
            })

            # Wait for subscription confirmation
            async for msg in self.ws_conn:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = orjson.loads(msg.data)
                    if data.get('type') == 'ack':
                        logger.info("WebSocket connection established")
                        return True

            return False
            
        except Exception as e:
            logger.error(f"WS connection error: {e}")
            return False


    async def ping_loop_and_reconnect(self, symbol: str):
        """Continuously ping the WebSocket connection to ensure it's alive."""
        while True:
            try:
                await self.ws_conn.ping()
                logger.info("Ping successful.")
            except aiohttp.ClientConnectionError:
                logger.info("WebSocket connection closed, stopping ping loop.")
                break
            except Exception as e:
                logger.info(f"Ping failed, reconnecting: {e}")
                await self.connect_websocket(symbol)
            await asyncio.sleep(2.5)  # Ping interval (adjust as needed)


    async def wait_until_listing(self,release_time: datetime, symbol: str):

        try:
            initiation_time_print_flag = False
            while True:
                time_waiting_in_seconds = (release_time - datetime.now()).total_seconds()

                # keep adjusting the time to listing for accuracy and safeing resources
                if 15 < time_waiting_in_seconds > 0:
                    if not initiation_time_print_flag:
                        initiation_time_print_flag = True
                        logger.info(f' Waiting {time_waiting_in_seconds:.2f} seconds until token release time')
                    await asyncio.sleep(5)

                # 15 seconds left until token release
                if 15 > time_waiting_in_seconds > 0:
                    logger.info(f'15 seconds left until token release')
                    break


        except Exception as e:
            logger.error(f"Error during waiting to realse time: {e}")





    async def get_price(self, symbol: str, release_time: datetime) -> Tuple[Optional[float], str]:
        """Get initial price as fast as possible"""
        try:
            await self.initialize()
            logger.info(f"Initializing WebSocket for {symbol}-USDT")
            
            # Wait until listing is close buy to not waste resources
            await self.wait_until_listing(release_time, symbol)

            #establish connection to websocket
            await self.connect_websocket(symbol)

            # pinging websocked to keep it alive and ensure connection
            await self.ping_loop_and_reconnect(symbol)

            while release_time > datetime.now():
                await asyncio.sleep(0.001)

            logger.info("Starting price capture")
            end_time = release_time + timedelta(seconds=self.max_wait_time)
            loopcount = 0

            while datetime.now() < end_time:
                loopcount += 1
                logger.info(f"Loopcount: {loopcount} ")
                try:
                    try:
                        msg = await self.ws_conn.receive_json(loads=orjson.loads)
                    except Exception as e:
                        print(f"Unexpected error: {e}")
                        # await self.connect_websocket(symbol)
                        continue
                                            
                    if msg.get('type') == 'message' and 'data' in msg:
                        try:
                            price = float(msg['data']['price'])
                            if price > 0:
                                capture_time = datetime.now()
                                logger.info(f"Price found: {price} at {capture_time.strftime('%H:%M:%S.%f')[:-3]}")
                                logger.info(f"Latency from target: {(capture_time - release_time).total_seconds():.3f}s")
                                return price, "Success"
                        except (KeyError, ValueError) as e:
                            logger.error(f"No price yet(KeyError or ValueError) at {datetime.now().strftime('%H:%M:%S.%f')[:-3]} Error message:{e}")
                            continue

                except Exception as e:
                    logger.error(f"No price yet at {datetime.now().strftime('%H:%M:%S.%f')[:-3]} Error message:{e} ")
                    await asyncio.sleep(0.1)
                    continue 

            return None, "Timeout reached"

        except Exception as e:
            logger.error(f"Error: {e}")
            return None, str(e)
    
    async def cleanup_websocket(self):
        """
        Clean up WebSocket resources
        """
        if self.ping_task:
            self.ping_task.cancel()
            try:
                await self.ping_task
            except asyncio.CancelledError:
                pass
            
        if self.ws_connection:
            await self.ws_connection.close()
            
        if self.ws_session:
            await self.ws_session.close()

    async def cleanup(self):
        try:
            if self.ws_conn:
                await self.ws_conn.close()
            if self.session:
                await self.session.close()
            logger.info("Cleaned up")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
        

############################################################################################################
# EXAMPLE USAGE
############################################################################################################


# async def main():
#     try:
#         # Get current time and set release time 10 seconds in the future for testing
#         now = datetime.now()
#         release_time = now + timedelta(seconds=10)
        
#         logger.info(f"Current time: {now.strftime('%H:%M:%S.%f')[:-3]}")
#         logger.info(f"Target time: {release_time.strftime('%H:%M:%S.%f')[:-3]}")
        
#         fetcher = FastKucoinWebSocket()
#         price, status = await fetcher.get_price("BTC", release_time)
#         logger.info(f"Final result - Price: {price}, Status: {status}")
            
#     except KeyboardInterrupt:
#         logger.info("Script stopped by user")
#     except Exception as e:
#         logger.error(f"Script error: {e}")
#     finally:
#         await fetcher.cleanup()


# if __name__ == "__main__":
#     asyncio.run(main())

