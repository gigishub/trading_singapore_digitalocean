import aiohttp
import asyncio
from playwright.async_api import async_playwright
from datetime import datetime, timedelta
import time
from typing import Optional, Tuple
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class KucoinPriceScraper:
    def __init__(self, xpath_to_find: str, enable_browser: bool = True, enable_api: bool = True, enable_websocket: bool = True,max_run_time_no_price_found: int = 2):
        self.ws_url = "wss://push1-v2.kucoin.com/endpoint"
        self.api_url = "https://api.kucoin.com"
        self.price_found = asyncio.Event()
        self.final_price = None
        self.browser = None
        self.page = None
        self.ws_connection = None
        self.ws_session = None
        self.ping_task = None
        self.xpath_to_find = xpath_to_find  
        self.enable_browser = enable_browser
        self.enable_api = enable_api
        self.enable_websocket = enable_websocket
        self.max_run_time_no_price_found = max_run_time_no_price_found
        

        
    async def get_token(self) -> Optional[str]:
        """
        Get WebSocket token from KuCoin API
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.api_url}/api/v1/bullet-public") as response:
                    if response.status == 200:
                        data = await response.json()
                        return data['data']['token']
        except Exception as e:
            logger.error(f"Failed to get WebSocket token: {e}")
            return None

    async def get_websocket_url(self) -> Optional[str]:
        """
        Get WebSocket connection URL with token
        """
        token = await self.get_token()
        if not token:
            return None
        return f"wss://ws-api-spot.kucoin.com/?token={token}"

    async def websocket_ping(self, ws):
        """
        Keep WebSocket connection alive with ping messages
        """
        try:
            while True:
                await ws.send_json({
                    "id": int(time.time() * 1000),
                    "type": "ping"
                })
                await asyncio.sleep(20)
        except Exception as e:
            logger.debug(f"Ping failed: {e}")

    async def initialize_websocket(self, symbol: str):
        """
        Pre-initialize WebSocket connection
        """
        try:
            ws_url = await self.get_websocket_url()
            if not ws_url:
                logger.error("Failed to get WebSocket URL during initialization")
                return False

            self.ws_session = aiohttp.ClientSession()
            self.ws_connection = await self.ws_session.ws_connect(ws_url)
            
            # Start ping task
            self.ping_task = asyncio.create_task(self.websocket_ping(self.ws_connection))
            
            # Subscribe to market ticker
            await self.ws_connection.send_json({
                "id": int(time.time() * 1000),
                "type": "subscribe",
                "topic": f"/market/ticker:{symbol}-USDT",
                "privateChannel": False,
                "response": True
            })
            
            # Wait for subscription confirmation
            async for msg in self.ws_connection:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if data.get('type') == 'ack':
                        logger.info("WebSocket pre-initialized successfully")
                        return True
                    
            return False
        except Exception as e:
            logger.error(f"WebSocket initialization error: {e}")
            await self.cleanup_websocket()
            return False

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

    async def initialize_browser(self, symbol: str):
        """
        Pre-initialize browser before release time
        """
        try:
            playwright = await async_playwright().start()
            self.browser = await playwright.chromium.launch()
            self.page = await self.browser.new_page()
            
            await self.page.route("**/*", lambda route: route.abort() 
                if route.request.resource_type in ['image', 'font', 'media']
                else route.continue_())
            
            await self.page.goto(f"https://www.kucoin.com/trade/{symbol}-USDT", wait_until='domcontentloaded')
            logger.info("Browser pre-initialized successfully")
        except Exception as e:
            logger.error(f"Browser initialization error: {e}")
            if self.browser:
                await self.browser.close()
    

    async def get_price_websocket(self, symbol: str, end_time: datetime) -> Optional[float]:
        """
        WebSocket price fetching using pre-initialized connection
        """
        if not self.ws_connection:
            logger.error("WebSocket not initialized")
            return None
            
        try:
            loopcount = 0
            while datetime.now() < end_time and not self.price_found.is_set():
                async for msg in self.ws_connection:
                    if datetime.now() >= end_time or self.price_found.is_set():
                        break
                    loopcount += 1
                    logger.info(f'loop Websocket {loopcount} at {datetime.now().strftime("%H:%M:%S:%f")[:-3]}')
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if data.get('type') == 'message' and 'data' in data:
                            try:
                                price = float(data['data']['price'])
                                if price > 0:
                                    logger.info(f"Price found via WebSocket: {price}")
                                    logger.info(f"Time price found: {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
                                    self.final_price = price
                                    self.price_found.set()
                                    return price
                            except (KeyError, ValueError) as e:
                                logger.debug(f"Failed to parse price from message: {e}")
                                
        except Exception as e:
            logger.error(f"WebSocket error during price fetch: {e}")
        return None

    # [Previous methods remain the same: get_price_api, get_price_browser]

    async def cleanup(self):
        """
        Clean up all resources
        """
        await self.cleanup_websocket()
        if self.browser:
            await self.browser.close()
    
    async def get_price_api(self, symbol: str, end_time: datetime) -> Optional[float]:
        """
        REST API price fetching with timeout
        """
        loopcount = 0
        while datetime.now() < end_time and not self.price_found.is_set():
            try:
                loopcount += 1
                logger.info(f'loop API {loopcount} at {datetime.now().strftime("%H:%M:%S:%f")[:-3]}')
                
                async with aiohttp.ClientSession() as session:
                    url = f"{self.api_url}/api/v1/market/orderbook/level1?symbol={symbol}-USDT"
                    async with session.get(url) as response:
                        if response.status == 200:
                            data = await response.json()
                            price = float(data['data']['price'])
                            logger.info(f"Price found via API: {price}")
                            logger.info(f"Time price found: {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
                            self.final_price = price
                            self.price_found.set()
                            return price
            except Exception as e:
                logger.debug(f"API attempt failed: {e}")
            # await asyncio.sleep(0.1)
        return None

    async def get_price_browser(self, symbol: str, end_time: datetime) -> Optional[float]:
        """
        Browser price fetching with timeout
        """
        loopcount = 0
        while datetime.now() < end_time and not self.price_found.is_set():
            try:
                loopcount += 1
                logger.info(f'loop browser {loopcount} at {datetime.now().strftime("%H:%M:%S:%f")[:-3]}')
                if not self.page:
                    logger.error("Browser not initialized")
                    return None
                    
                price_element = await self.page.wait_for_selector(
                    self.xpath_to_find,
                    timeout=100
                )
                
                if price_element:
                    price_text = await price_element.text_content()
                    try:
                        price = float(price_text.strip())
                        if price > 0:  # Validate price
                            logger.info(f"Price found via Browser: {price}")
                            logger.info(f"Time price found: {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
                            self.final_price = price
                            self.price_found.set()
                            return price
                    except ValueError:
                        pass
            except Exception as e:
                logger.debug(f"Browser attempt failed: {e}")
        return None

    async def wait_until_listing(self,release_time: datetime):
        """
        Waiting and updating when time is close to mitigate time lagging.
        
        This function continuously checks the remaining time until the listing and sleeps for different intervals
        based on the remaining time. It prints messages when the time to listing is less than certain thresholds.
        
        Args:
            time_waiting_in_seconds (float): The time in seconds until the listing.
        """
        try:
            two_seconds_mark = False
            initiation_time_print_flag = False
            while True:
                time_waiting_in_seconds = (release_time - datetime.now()).total_seconds()
                if 0.3 > time_waiting_in_seconds > 0:
                    logger.info(f'start searching for price at {datetime.now().strftime("%H:%M:%S:%f")}')
                    break
                elif 2 > time_waiting_in_seconds > 0:
                    if not two_seconds_mark:
                        two_seconds_mark = True
                        logger.info('time to listing is less than 2 seconds')
                        
                    await asyncio.sleep(0.1)
                elif 15 > time_waiting_in_seconds > 0:
                    await asyncio.sleep(1)
                elif 15 < time_waiting_in_seconds > 0:
                    if not initiation_time_print_flag:
                        initiation_time_print_flag = True
                        logger.info(f' Waiting {time_waiting_in_seconds:.2f} seconds until token release time')
                    await asyncio.sleep(10)
                else:
                    print('time to listing is less than 0 seconds')
                    break
        except Exception as e:
            logger.error(f"Error during waiting to realse time: {e}")



    async def get_release_price(self, symbol: str, release_time: datetime) -> Tuple[Optional[float], str]:
        """
        Main method to get price at release time using all methods
        """
        # Pre-initialize both browser and WebSocket
        await asyncio.gather(
            self.initialize_browser(symbol),
            self.initialize_websocket(symbol)
        )
        
        # Wait for release time
        await self.wait_until_listing(release_time)
        

        
        # Set end time to 5 seconds after release
        end_time = release_time + timedelta(seconds=self.max_run_time_no_price_found)
        
        try:

            tasks = []
            
            if self.enable_browser:
                tasks.append(asyncio.create_task(self.get_price_browser(symbol, end_time)))
            if self.enable_api:
                tasks.append(asyncio.create_task(self.get_price_api(symbol, end_time)))
            if self.enable_websocket:
                tasks.append(asyncio.create_task(self.get_price_websocket(symbol, end_time)))
            

            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED
            )
            
            for task in pending:
                task.cancel()
            
            await asyncio.gather(*pending, return_exceptions=True)
            
            if self.final_price:
                return self.final_price, "Price found successfully"
            return None, f"No price found within timeout at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}"
        
        except Exception as e:
            logger.error(f"Error during price retrieval: {e}")
        # finally:
        #     await self.cleanup()


# # Example usage:

# async def main():
#     release_time = datetime.now() + timedelta(seconds=5)
#     print('Release time:', release_time)
    
#     scraper = KucoinPriceScraper()
#     price, status = await scraper.get_release_price("ETH", release_time)
    
#     if price:
#         time_at_price_found = datetime.now()
#         print(f"Time at retrieval: {time_at_price_found.strftime('%H:%M:%S.%f')[:-3]}")
#         print(f"Found price: {price}")
        
#         relase_to_price_found_time = time_at_price_found - release_time
#         print(f'Time difference of release time and price found: {relase_to_price_found_time}')
#     else:
#         print(f"Status: {status}")


# if __name__ == "__main__":
#     asyncio.run(main())