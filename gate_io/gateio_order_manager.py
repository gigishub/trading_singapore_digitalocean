import asyncio
import aiohttp
import time
import hmac
import hashlib
import json
from typing import Dict, List
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

class GateioOrderManager:
    def __init__(self, api_key: str, api_secret: str, debug: bool = False):
        """Initialize Gate.io trading client with async support"""
        if not all([api_key, api_secret]):
            raise ValueError("API key and secret are required")
                
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.gateio.ws"
        self.debug = debug
        
        self._static_headers = {
            "Content-Type": "application/json",
        }
        
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.session = None

    def _get_formatted_time(self) -> Dict[str, str]:
        """Returns current time in human readable format"""
        now = datetime.now()
        return {
            "order_sent_time": now.strftime('%H:%M:%S.%f')[:-3],
        }

    def _convert_timestamp_to_readable(self, timestamp_ms: int) -> str:
        """Convert millisecond timestamp to readable time format"""
        dt = datetime.fromtimestamp(timestamp_ms / 1000)
        return dt.strftime('%H:%M:%S.%f')[:-3]

    async def _init_session(self):
        """Initialize aiohttp session"""
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=5)
            self.session = aiohttp.ClientSession(timeout=timeout)

    def _generate_signature(self, method: str, url_path: str, query_string: str = '', payload_string: str = '') -> Dict[str, str]:
        """Generate signature for Gate.io API"""
        t = str(int(time.time()))
        hashed_payload = hashlib.sha512((payload_string or "").encode('utf-8')).hexdigest()
        to_sign = '\n'.join([
            method.upper(),
            url_path,
            query_string,
            hashed_payload,
            t
        ])
        sign = hmac.new(self.api_secret.encode('utf-8'), to_sign.encode('utf-8'), hashlib.sha512).hexdigest()
        headers = {
            "KEY": self.api_key,
            "Timestamp": t,
            "SIGN": sign
        }
        return headers

    async def _make_request(self, method: str, endpoint: str, data: Dict = None, params: Dict = None) -> Dict:
        """Make async API request"""
        await self._init_session()
        url_path = endpoint
        url = f"{self.base_url}{endpoint}"
        query_string = ''
        if params:
            query_string = '&'.join([f"{key}={value}" for key, value in params.items()])
            url += f"?{query_string}"
        payload_string = ''
        if data:
            payload_string = json.dumps(data)
        
        try:
            headers = self._static_headers.copy()
            sig_headers = self._generate_signature(method, url_path, query_string, payload_string)
            headers.update(sig_headers)

            async with getattr(self.session, method.lower())(url, headers=headers, json=data if data else None) as response:
                return await response.json()
        except Exception as e:
            module_logger.error(f"Request error: {str(e)}")
            raise

    async def place_limit_buy(self, currency_pair: str, price: str, amount: str, time_in_force: str = "gtc") -> Dict:
        """Place an async limit buy order"""
        start_time = time.perf_counter()
        try:
            time_info = self._get_formatted_time()

            order_data = {
                "currency_pair": currency_pair,
                "type": "limit",
                "side": "buy",
                "price": price,
                "amount": amount,
                "time_in_force": time_in_force
            }

            response = await self._make_request("POST", "/api/v4/spot/orders", data=order_data)
            execution_time = round(time.perf_counter() - start_time, 5)
            module_logger.info(f"BUY Order response: {response}")

            if response.get('id'):
                create_time_ms = response.get('create_time_ms')
                return {
                    "success": True,
                    "execution_time": execution_time,
                    "limit_buy_price": price,
                    "order_id": response.get('id'),
                    "order_sent_time": time_info["order_sent_time"],
                    "order_create_time": self._convert_timestamp_to_readable(create_time_ms) if create_time_ms else None,
                    "order_received_time": datetime.now().strftime('%H:%M:%S.%f')[:-3],
                    "currency_pair": response.get('currency_pair', '').lower(),
                    "order_size": amount
                }    
            else:
                return {
                    "success": False,
                    "message": response.get('message', 'Unknown error'),
                    "code": response.get('label', ''),
                    "order_sent_time": time_info["order_sent_time"],
                    "order_received_time": datetime.now().strftime('%H:%M:%S.%f')[:-3],
                    "price": price,
                    "order_size": amount
                }

        except Exception as e:
            return {
                "success": False,
                "message": str(e),
                "code": "ERROR",
                "order_sent_time": time_info["order_sent_time"],
                "order_received_time": datetime.now().strftime('%H:%M:%S.%f')[:-3],
                "price": price,
                "order_size": amount
            }

    async def place_limit_sell(self, currency_pair: str, price: str, amount: str, time_in_force: str = "gtc") -> Dict:
        """Place an async limit sell order"""
        start_time = time.perf_counter()
        try:
            time_info = self._get_formatted_time()

            order_data = {
                "currency_pair": currency_pair,
                "type": "limit",
                "side": "sell",
                "price": price,
                "amount": amount,
                "time_in_force": time_in_force
            }

            response = await self._make_request("POST", "/api/v4/spot/orders", data=order_data)
            execution_time = round(time.perf_counter() - start_time, 5)
            module_logger.info(f"SELL Order response: {response}")

            if response.get('id'):
                create_time_ms = response.get('create_time_ms')
                return {
                    "success": True,
                    "execution_time": execution_time,
                    "limit_sell_price": price,
                    "order_id": response.get('id'),
                    "order_sent_time": time_info["order_sent_time"],
                    "order_create_time": self._convert_timestamp_to_readable(create_time_ms) if create_time_ms else None,
                    "order_received_time": datetime.now().strftime('%H:%M:%S.%f')[:-3],                    
                    "currency_pair": response.get('currency_pair', '').lower(),
                    "order_size": amount
                }    
            else:
                return {
                    "success": False,
                    "message": response.get('message', 'Unknown error'),
                    "code": response.get('label', ''),
                    "order_sent_time": time_info["order_sent_time"],
                    "order_received_time": datetime.now().strftime('%H:%M:%S.%f')[:-3],
                    "price": price,
                    "order_size": amount
                }

        except Exception as e:
            return {
                "success": False,
                "message": str(e),
                "code": "ERROR",
                "order_sent_time": time_info["order_sent_time"],
                "order_received_time": datetime.now().strftime('%H:%M:%S.%f')[:-3],
                "price": price,
                "order_size": amount
            }

    async def place_multiple_orders(self, orders: List[Dict]) -> List[Dict]:
        """Place multiple orders concurrently"""
        tasks = []
        for order in orders:
            if order["side"].lower() == "buy":
                task = self.place_limit_buy(
                    currency_pair=order["currency_pair"],
                    price=order["price"],
                    amount=order["amount"],
                    time_in_force=order.get("time_in_force", "gtc")
                )
            else:  # sell
                task = self.place_limit_sell(
                    currency_pair=order["currency_pair"],
                    price=order["price"],
                    amount=order["amount"],
                    time_in_force=order.get("time_in_force", "gtc")
                )
            tasks.append(task)
        
        return await asyncio.gather(*tasks)

    async def close(self):
        """Close the aiohttp session"""
        if self.session:
            await self.session.close()
        self.executor.shutdown(wait=False)

# Example usage
async def main():
    try:
        with open('/root/trading_systems/gate_io/api_creds.json', 'r') as file:
            api_creds = json.load(file)

        trader = GateioOrderManager(
            api_key=api_creds['api_key'],
            api_secret=api_creds['api_secret'],
            debug=False
        )

        # Example of placing multiple orders
        orders = [
            {
                "currency_pair": "BTC_USDT",  # Using lowercase
                "side": "buy",
                "price": "30300",
                "amount": "0.0001"
            },
            {
                "currency_pair": "BTC_USDT",  # Using lowercase
                "side": "buy",
                "price": "35000",
                "amount": "0.0001"
            }
        ]

        results = await trader.place_multiple_orders(orders)
        module_logger.info(f"Order Results:{json.dumps(results, indent=4)}")

        # Clean up
        await trader.close()

    except Exception as e:
        module_logger.error(f"Main execution error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())