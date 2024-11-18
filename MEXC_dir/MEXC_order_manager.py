import asyncio
import aiohttp
import time
import hmac
import hashlib
import json
from typing import Optional, Dict, Union, List
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlencode

# Create a dedicated logger for this module
module_logger = logging.getLogger(__name__)
module_logger.setLevel(logging.INFO)
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
        self.api_secret = api_secret
        self.base_url = "https://api.mexc.com"
        self.debug = debug
        
        self._static_headers = {
            "Content-Type": "application/json",
            "X-MEXC-APIKEY": self.api_key
        }
        
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.session = None

    def _get_formatted_time(self) -> Dict[str, str]:
        """Returns current time in human readable format"""
        now = datetime.now()
        return {
            "order_sent_time": now.strftime('%H:%M:%S.%f')[:-3],
        }

    async def _init_session(self):
        """Initialize aiohttp session"""
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=5)
            self.session = aiohttp.ClientSession(timeout=timeout)

    def _generate_signature(self, params: Dict) -> str:
        """Generate signature for MEXC API"""
        try:
            # Sort parameters alphabetically and create query string
            query_string = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
            
            # Generate signature using HMAC SHA256
            signature = hmac.new(
                self.api_secret.encode('utf-8'),
                query_string.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            return signature
            
        except Exception as e:
            module_logger.error(f"Error generating signature: {str(e)}")
            raise

    async def _make_request(self, method: str, endpoint: str, params: Dict = None) -> Dict:
        """Make async API request"""
        await self._init_session()
        
        params = params or {}
        
        # Add timestamp and recvWindow
        params['timestamp'] = str(int(time.time() * 1000))
        params['recvWindow'] = '5000'
        
        # Generate signature
        signature = self._generate_signature(params)
        params['signature'] = signature
        
        url = f"{self.base_url}{endpoint}"
        
        try:
            headers = self._static_headers.copy()
            
            if method.upper() == 'GET':
                query_string = urlencode(params)
                url = f"{url}?{query_string}"
                async with self.session.get(url, headers=headers) as response:
                    return await response.json()
            else:  # POST
                # For POST requests, send parameters in the request body
                form_data = aiohttp.FormData()
                for key, value in params.items():
                    form_data.add_field(key, str(value))
                
                headers["Content-Type"] = "application/x-www-form-urlencoded"
                async with self.session.post(url, headers=headers, data=form_data) as response:
                    return await response.json()

        except Exception as e:
            module_logger.error(f"Request error: {str(e)}")
            raise

    def _format_request_time(self, ms):
        seconds = ms / 1000.0
        dt = datetime.utcfromtimestamp(seconds)
        return dt.strftime('%H:%M:%S.%f')[:-3]

    async def place_limit_buy(self, symbol: str, price: str, quantity: str) -> Dict:
        """Place an async limit buy order"""
        start_time = time.perf_counter()
        try:
            time_info = self._get_formatted_time()

            order_data = {
                "symbol": symbol,
                "side": "BUY",
                "type": "LIMIT",
                "quantity": quantity,
                "price": str(price)  # Ensure price is string
            }

            response = await self._make_request("POST", "/api/v3/order", order_data)
            execution_time = round(time.perf_counter() - start_time, 5)
            
            if response.get('orderId'):  # Successful order
                return {
                    "success": True,
                    "execution_time": execution_time,
                    "limit_buy_price": price,
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

    # [Rest of the class methods remain the same...]

    async def close(self):
        """Close the aiohttp session"""
        if self.session:
            await self.session.close()
        self.executor.shutdown(wait=False)

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
            symbol="BTCUSDT",
            price="30000",  # Price as string
            quantity="0.0001"
        )
        print(f"Buy Limit Test: {json.dumps(buy_limit_test, indent=4)}")

        # Clean up
        await trader.close()

    except Exception as e:
        module_logger.error(f"Main execution error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())