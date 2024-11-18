import asyncio
import aiohttp
import time
import hmac
import hashlib
import json
from typing import Optional, Dict, Union, List
from typing import Dict
from datetime import datetime
import logging
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor

# Create a dedicated logger for this module
module_logger = logging.getLogger(__name__)
module_logger.setLevel(logging.DEBUG)  # Set to DEBUG level for verbose logging
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
module_logger.addHandler(console_handler)
module_logger.propagate = False

class MexcOrderManager:
    def __init__(self, api_key: str, api_secret: str, debug: bool = False):
        """Initialize MEXC trading client with async support"""
        if not all([api_key, api_secret]):
            raise ValueError("API key and secret are required")
            
        self.api_key = api_key
        self.api_secret = api_secret  # Store as string
        self.base_url = "https://api.mexc.com"
        self.debug = debug
        
        self._static_headers = {
            'X-MEXC-APIKEY': self.api_key,
            'Content-Type': 'application/json'  # Set to URL-encoded form data
        }

        self.executor = ThreadPoolExecutor(max_workers=4)
        self.session = None

    async def _init_session(self):
        """Initialize aiohttp session"""
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=5)
            self.session = aiohttp.ClientSession(timeout=timeout)

    def _generate_signature(self, params: Dict) -> str:
        """Generate signature for MEXC API according to their specifications"""
        # Exclude 'signature' from the parameters used for signature calculation
        params_to_sign = [(k, v) for k, v in params.items() if k != 'signature']
        # Concatenate parameters in the order they appear
        param_str = '&'.join(f"{key}={value}" for key, value in params_to_sign)
        if self.debug:
            module_logger.debug(f"Parameter string before signing: {param_str}")
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            param_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature

    async def _make_request(self, method: str, endpoint: str, params: Dict) -> Dict:
        """Make async API request"""
        await self._init_session()
        
        # Add timestamp to parameters
        params['timestamp'] = str(int(time.time() * 1000))
        
        # Generate signature
        signature = self._generate_signature(params)
        params['signature'] = signature
        
        if method.upper() == 'GET':
            # For GET requests, parameters are sent in the query string
            query_string = urlencode(params)
            url = f"{self.base_url}{endpoint}?{query_string}"
            if self.debug:
                module_logger.debug(f"Final URL: {url}")
                module_logger.debug(f"Headers: {self._static_headers}")
            async with self.session.get(url, headers=self._static_headers) as response:
                return await response.json()
        else:
            # For POST requests, parameters are sent as URL-encoded form data
            url = f"{self.base_url}{endpoint}"
            body = urlencode(params)
            if self.debug:
                module_logger.debug(f"Final URL: {url}")
                module_logger.debug(f"Headers: {self._static_headers}")
                module_logger.debug(f"Request Body: {body}")
            async with self.session.post(url, headers=self._static_headers, data=body) as response:
                return await response.json()

    async def place_limit_buy(self, symbol: str, price: str, quantity: str) -> Dict:
        """Place a limit buy order"""
        time_info = self._get_formatted_time()
        start_time = time.perf_counter()
        
        try:
            # Prepare order parameters exactly as required by MEXC
            order_params = {
                'symbol': symbol.upper(),
                'side': 'SELL',
                'type': 'LIMIT',
                'quantity': quantity,
                'price': price,
                'timestamp': str(int(time.time() * 1000)),  # Add timestamp here
                'timeInForce': 'GTC'  # Good Till Cancel
            }
            
            response = await self._make_request('POST', '/api/v3/order', order_params)
            execution_time = round(time.perf_counter() - start_time, 5)
            
            if response.get('orderId'):
                return {
                    'success': True,
                    'execution_time': execution_time,
                    'limit_buy_price': price,
                    'order_id': response.get('orderId'),
                    'transactTime': self._format_request_time(response.get('transactTime', 0)),
                    'order_sent_time': time_info['order_sent_time'],
                    'order_size': quantity
                }
            else:
                return {
                    'success': False,
                    'message': response.get('msg', 'Unknown error'),
                    'code': response.get('code'),
                    'order_sent_time': time_info['order_sent_time'],
                    'price': price,
                    'order_size': quantity
                }
                
        except Exception as e:
            return {
                'success': False,
                'message': str(e),
                'code': 'ERROR',
                'order_sent_time': time_info['order_sent_time'],
                'price': price,
                'order_size': quantity
            }

    def _format_request_time(self, ms):
        seconds = ms / 1000.0
        dt = datetime.utcfromtimestamp(seconds)
        return dt.strftime('%H:%M:%S.%f')[:-3]

    def _get_formatted_time(self) -> Dict[str, str]:
        """Returns current time in human readable format"""
        now = datetime.now()
        return {
            "order_sent_time": now.strftime('%H:%M:%S.%f')[:-3],
        }

    async def close(self):
        """Close the aiohttp session"""
        if self.session:
            await self.session.close()

    async def place_limit_sell(self, symbol: str, price: str, quantity: str) -> Dict:
        """Place an async limit sell order"""
        start_time = time.perf_counter()
        try:
            time_info = self._get_formatted_time()

            order_data = {
                "symbol": symbol,
                "side": "SELL",
                "type": "LIMIT",
                "quantity": quantity,
                "price": price
            }

            response = await self._make_request("POST", "/api/v3/order", order_data)
            execution_time = round(time.perf_counter() - start_time, 5)
            
            if response.get('orderId'):  # Successful order
                return {
                    "success": True,
                    "execution_time": execution_time,
                    "limit_sell_price": price,
                    "order_id": response.get('orderId'),
                    "transactTime": self._format_request_time(response.get('transactTime', 0)),
                    "order_sent_time": time_info["order_sent_time"],
                    "order_size": quantity
                }
            else:
                return {
                    "success": False,
                    "message": response.get('msg', 'Unknown error'),
                    "code": response.get('code'),
                    "order_sent_time": time_info["order_sent_time"],
                    "price": price,
                    "order_size": quantity
                }

        except Exception as e:
            return {
                "success": False,
                "message": str(e),
                "code": "ERROR",
                "order_sent_time": time_info["order_sent_time"],
                "price": price,
                "order_size": quantity
            }

    async def place_multiple_orders(self, orders: List[Dict]) -> List[Dict]:
        """Place multiple orders concurrently

        Args:
            orders: List of order dictionaries, each containing:
                {
                    "symbol": str,
                    "side": "BUY" or "SELL",
                    "price": str,
                    "quantity": str
                }
        """
        tasks = []
        for order in orders:
            if order["side"].upper() == "BUY":
                task = self.place_limit_buy(
                    symbol=order["symbol"],
                    price=order["price"],
                    quantity=order["quantity"]
                )
            else:  # sell
                task = self.place_limit_sell(
                    symbol=order["symbol"],
                    price=order["price"],
                    quantity=order["quantity"]
                )
            tasks.append(task)
        
        return await asyncio.gather(*tasks)

    async def multiple_buy_orders_percent_dif(self, symbol: str, base_price: float, quantity: str, num_orders: int, percentage_difference: float) -> List[Dict]:
        """
        Place multiple limit buy orders concurrently with different prices based on a percentage difference.

        Args:
            symbol: Trading symbol (e.g., "BTCUSDT")
            base_price: The base price for the limit buy orders
            quantity: Size of each order
            num_orders: Number of orders to place
            percentage_difference: Percentage difference between each order price
        """
        prices = [
            str(base_price * (1 + (i * percentage_difference / 100)))
            for i in range(num_orders)
        ]

        orders = [
            {
                "symbol": symbol,
                "side": "BUY",
                "price": price,
                "quantity": quantity
            }
            for price in prices
        ]
        
        return await self.place_multiple_orders(orders)

    async def multiple_sell_orders_percent_dif(self, symbol: str, base_price: float, quantity: str, num_orders: int, percentage_difference: float) -> List[Dict]:
        """
        Place multiple limit sell orders concurrently with different prices based on a percentage difference.

        Args:
            symbol: Trading symbol (e.g., "BTCUSDT")
            base_price: The base price for the limit sell orders
            quantity: Size of each order
            num_orders: Number of orders to place
            percentage_difference: Percentage difference between each order price
        """
        prices = [
            str(base_price * (1 + (i * percentage_difference / 100)))
            for i in range(num_orders)
        ]

        orders = [
            {
                "symbol": symbol,
                "side": "SELL",
                "price": price,
                "quantity": quantity
            }
            for price in prices
        ]
        
        return await self.place_multiple_orders(orders)

    async def close(self):
        """Close the aiohttp session"""
        if self.session:
            await self.session.close()
        self.executor.shutdown(wait=False)

# # Example usage
# async def main():
#     try:
#         with open('/root/trading_systems/MEXC_dir/api_creds.json', 'r') as file:
#             api_creds = json.load(file)

#         trader = MexcOrderManager(
#             api_key=api_creds['api_key'],
#             api_secret=api_creds['api_secret'],
#             debug=False
#         )

#         # Example of placing multiple limit buy orders
#         base_price = 0.5
#         percentage_difference = 1  # 1%
#         num_orders = 2
#         results = await trader.multiple_buy_orders_percent_dif(
#             symbol="HONEYUSDT",
#             base_price=base_price,
#             quantity="10",
#             num_orders=num_orders,
#             percentage_difference=percentage_difference
#         )
#         module_logger.info(f"Order Results:{json.dumps(results, indent=4)}")

#         # Clean up
#         await trader.close()


#     except Exception as e:
#         module_logger.error(f"Main execution error: {str(e)}", exc_info=True)

# if __name__ == "__main__":
#     asyncio.run(main())
# Example usage
async def main():
    try:
        with open('/root/trading_systems/MEXC_dir/api_creds.json', 'r') as file:
            api_creds = json.load(file)

        trader = MexcOrderManager(
            api_key=api_creds['api_key'],
            api_secret=api_creds['api_secret'],
            debug=False
        )

        # Example of placing a limit buy order
        buy_limit_test = await trader.place_limit_buy(
            symbol="WQUILUSDT",
            price="0.09",  # Price as string
            quantity="100"
        )
        print(f"Buy Limit Test: {json.dumps(buy_limit_test, indent=4)}")

        # Clean up
        await trader.close()

    except Exception as e:
        module_logger.error(f"Main execution error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())

