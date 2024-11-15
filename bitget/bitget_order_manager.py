import asyncio
import aiohttp
import time
import uuid
import hmac
import base64
import hashlib
import json
from typing import Optional, Dict, Union, List
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor

# Create a dedicated logger for this module
module_logger = logging.getLogger(__name__)
module_logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
module_logger.addHandler(console_handler)
module_logger.propagate = False

class BitgetOrderManager:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str, debug: bool = False):
        """Initialize Bitget trading client with async support"""
        if not all([api_key, api_secret, api_passphrase]):
            raise ValueError("API key, secret, and passphrase are required")
            
        self.api_key = api_key
        self.api_secret = api_secret if isinstance(api_secret, str) else str(api_secret)
        self.api_passphrase = api_passphrase
        self.base_url = "https://api.bitget.com"
        self.debug = debug
        
        self._static_headers = {
            "Content-Type": "application/json",
            "locale": "en-US",
            "ACCESS-KEY": self.api_key
        }
        
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.session = None


    def _get_formatted_time(self) -> Dict[str, str]:
        """
        Returns current time in human readable format
        """
        now = datetime.now()
        return {
            "order_sent_time": now.strftime('%H:%M:%S.%f')[:-3],
        }

    async def _init_session(self):
        """Initialize aiohttp session"""
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=5)
            self.session = aiohttp.ClientSession(timeout=timeout)

    def _generate_signature(self, timestamp: str, method: str, endpoint: str, body: str = "") -> str:
        """Generate signature for Bitget API"""
        try:
            message = f'{timestamp}{method}{endpoint}{body}'
            secret_bytes = self.api_secret.encode('utf-8')
            
            signature = base64.b64encode(
                hmac.new(secret_bytes, 
                        message.encode('utf-8'), 
                        hashlib.sha256).digest()
            ).decode('utf-8')
            
            return signature
            
        except Exception as e:
            module_logger.error(f"Error generating signature: {str(e)}")
            raise

    async def _make_request(self, method: str, endpoint: str, data: Dict = None) -> Dict:
        """Make async API request"""
        await self._init_session()
        url = f"{self.base_url}{endpoint}"
        timestamp = str(int(time.time() * 1000))
        
        body = ""
        if data:
            body = json.dumps(data)
        
        try:
            signature = self._generate_signature(timestamp, method, endpoint, body)
            
            headers = self._static_headers.copy()
            headers.update({
                "ACCESS-SIGN": signature,
                "ACCESS-TIMESTAMP": timestamp,
                "ACCESS-PASSPHRASE": self.api_passphrase
            })

            async with getattr(self.session, method.lower())(url, headers=headers, json=data if data else None) as response:
                return await response.json()

        except Exception as e:
            module_logger.error(f"Request error: {str(e)}")
            raise

    def _format_request_time(self, ms):
        # Convert milliseconds to seconds
        seconds = ms / 1000.0
        # Create a datetime object from the seconds
        dt = datetime.utcfromtimestamp(seconds)
        # Format the datetime object to the desired format
        return dt.strftime('%H:%M:%S.%f')[:-3]

    async def place_limit_buy(self, symbol: str, price: str, size: str, 
                              time_in_force: str = "gtc") -> Dict:
        """Place an async limit buy order"""
        start_time = time.perf_counter()
        try:
            # Record order_sent_time immediately before sending the order
            time_info = self._get_formatted_time()

            order_data = {
                "symbol": symbol,
                "side": "buy",
                "orderType": "limit",
                "price": price,
                "size": size,
                "force": time_in_force.lower()
            }

            response = await self._make_request("POST", "/api/v2/spot/trade/place-order", order_data)
            execution_time = round(time.perf_counter() - start_time, 5)
            
            if response.get('code') == '00000':  # Bitget success code
                return {
                    "success": True,
                    "execution_time": execution_time,
                    "limit_buy_price": price,
                    "order_id": response.get('data', {}).get('orderId', 'unknown'),
                    "requestTime": self._format_request_time(response.get('requestTime', 0)),
                    "order_sent_time": time_info["order_sent_time"]
                }    
            else:
                return {
                    "success": False,
                    "message": response.get('msg', 'Unknown error'),
                    "code": response.get('code'),
                    "requestTime": self._format_request_time(response.get('requestTime', 0)),
                    "order_sent_time": time_info["order_sent_time"]
                }

        except Exception as e:
            return {
                "success": False,
                "message": str(e),
                "code": "ERROR",
                "requestTime": None,
                "order_sent_time": time_info["order_sent_time"]
            }

    async def place_limit_sell(self, symbol: str, price: str, size: str, 
                               time_in_force: str = "gtc") -> Dict:
        """Place an async limit sell order"""
        start_time = time.perf_counter()
        try:
            # Record order_sent_time immediately before sending the order
            time_info = self._get_formatted_time()

            order_data = {
                "symbol": symbol,
                "side": "sell",
                "orderType": "limit",
                "price": price,
                "size": size,
                "force": time_in_force.lower()
            }

            response = await self._make_request("POST", "/api/v2/spot/trade/place-order", order_data)
            execution_time = round(time.perf_counter() - start_time, 5)
            
            if response.get('code') == '00000':  # Bitget success code
                return {
                    "success": True,
                    "execution_time": execution_time,
                    "limit_sell_price": price,
                    "order_id": response.get('data', {}).get('orderId', 'unknown'),
                    "requestTime": self._format_request_time(response.get('requestTime', 0)),
                    "order_sent_time": time_info["order_sent_time"]
                }
            else:
                return {
                    "success": False,
                    "message": response.get('msg', 'Unknown error'),
                    "code": response.get('code'),
                    "requestTime": self._format_request_time(response.get('requestTime', 0)),
                    "order_sent_time": time_info["order_sent_time"]
                }

        except Exception as e:
            return {
                "success": False,
                "message": str(e),
                "code": "ERROR",
                "requestTime": None,
                "order_sent_time": time_info["order_sent_time"]
            }

    async def place_multiple_orders(self, orders: List[Dict]) -> List[Dict]:
        """Place multiple orders concurrently

        Args:
            orders: List of order dictionaries, each containing:
                {
                    "symbol": str,
                    "side": "buy" or "sell",
                    "price": str,
                    "size": str,
                    "time_in_force": str (optional)
                }
        """
        tasks = []
        for order in orders:
            if order["side"].lower() == "buy":
                task = self.place_limit_buy(
                    symbol=order["symbol"],
                    price=order["price"],
                    size=order["size"],
                    time_in_force=order.get("time_in_force", "gtc")
                )
            else:  # sell
                task = self.place_limit_sell(
                    symbol=order["symbol"],
                    price=order["price"],
                    size=order["size"],
                    time_in_force=order.get("time_in_force", "gtc")
                )
            tasks.append(task)
        
        return await asyncio.gather(*tasks)

    async def close(self):
        """Close the aiohttp session"""
        if self.session:
            await self.session.close()
        self.executor.shutdown(wait=False)


    async def multiple_buy_orders_percent_dif(self, symbol: str, base_price: float, size: str, num_orders: int, percentage_difference: float, time_in_force: str = "gtc") -> List[Dict]:
        """
        Place multiple limit buy orders concurrently with different prices based on a percentage difference.

        Args:
            symbol: Trading symbol (e.g., "BTCUSDT").
            base_price: The base price for the limit buy orders.
            size: Size of each order.
            num_orders: Number of orders to place.
            percentage_difference: Percentage difference between each order price.
            time_in_force: Time in force for the orders (default is "gtc").

        Returns:
            List of order results.
        """
        prices = [
            str(base_price * (1 + (i * percentage_difference / 100)))
            for i in range(num_orders)
        ]

        orders = [
            {
                "symbol": symbol,
                "side": "buy",
                "price": price,
                "size": size,
                "time_in_force": time_in_force
            }
            for price in prices
        ]
        multi_buy_order_resposne = await self.place_multiple_orders(orders)

        for order in multi_buy_order_resposne:
            if order["success"]:
                self.successful_buy_orders.append(order)


# Example usage
async def main():
    try:
        with open('/root/trading_systems/bitget/api_creds.json', 'r') as file:
            api_creds = json.load(file)

        trader = BitgetOrderManager(
            api_key=api_creds['api_key'],
            api_secret=api_creds['api_secret'],
            api_passphrase=api_creds['api_passphrase'],
            debug=False
        )

        # Example of placing multiple limit buy orders with different prices based on percentage difference
        base_price = 30000
        percentage_difference = 1  # 1%
        num_orders = 8
        results = await trader.multiple_buy_orders_percent_dif(
            symbol="BTCUSDT",
            base_price=base_price,
            size="0.0001",
            num_orders=num_orders,
            percentage_difference=percentage_difference
        )
        module_logger.info(f"Order Results:{json.dumps(results, indent=4)}")

        # Clean up
        await trader.close()

    except Exception as e:
        module_logger.error(f"Main execution error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())

# # Example usage
# async def main():
#     try:
#         with open('/root/trading_systems/bitget/api_creds.json', 'r') as file:
#             api_creds = json.load(file)

#         trader = BitgetOrderManager(
#             api_key=api_creds['api_key'],
#             api_secret=api_creds['api_secret'],
#             api_passphrase=api_creds['api_passphrase'],
#             debug=False
#         )

#         # Example of placing multiple orders concurrently
#         orders = [
#             {
#                 "symbol": "BTCUSDT",
#                 "side": "buy",
#                 "price": "30000",
#                 "size": "0.0001"
#             },
#             {
#                 "symbol": "BTCUSDT",
#                 "side": "buy",
#                 "price": "31000",
#                 "size": "0.0001"
#             }
#         ]

#         # Execute multiple orders concurrently
#         results = await trader.place_multiple_orders(orders)
#         module_logger.info(f"Order Results:{json.dumps(results, indent=4)}")

#         # Clean up
#         await trader.close()

#     except Exception as e:
#         module_logger.error(f"Main execution error: {str(e)}", exc_info=True)

# if __name__ == "__main__":
#     asyncio.run(main())