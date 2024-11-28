import asyncio
import aiohttp
import time
import uuid
import hmac
import base64
import hashlib
from typing import Optional, Dict, Union, List
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.ERROR,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class kucoinHForderManager:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str, debug: bool = False):
        """Initialize KuCoin HF trading client with async support"""
        if not all([api_key, api_secret, api_passphrase]):
            raise ValueError("API key, secret, and passphrase are required")
            
        self.api_key = api_key
        self.api_secret = api_secret if isinstance(api_secret, str) else str(api_secret)
        self.api_passphrase = api_passphrase
        self.base_url = "https://api.kucoin.com"
        self.debug = debug
        
        self._static_headers = {
            "KC-API-KEY": self.api_key,
            "KC-API-KEY-VERSION": "2",
            "Content-Type": "application/json"
        }
        
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.session = None

        #order tracking
        self.placed_limit_buy = []
        self.placed_limit_sell = []


    async def _init_session(self):
        """Initialize aiohttp session"""
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=5)
            self.session = aiohttp.ClientSession(timeout=timeout)

    def _generate_signature(self, timestamp: str, method: str, endpoint: str, body: str = "") -> tuple:
        """Generate signature with minimal overhead"""
        try:
            signature_string = f"{timestamp}{method}{endpoint}{body}"
            secret_bytes = self.api_secret.encode('utf-8')
            
            signature_future = self.executor.submit(
                lambda: base64.b64encode(
                    hmac.new(secret_bytes, 
                            signature_string.encode('utf-8'), 
                            hashlib.sha256).digest()
                ).decode('utf-8')
            )
            
            passphrase_future = self.executor.submit(
                lambda: base64.b64encode(
                    hmac.new(secret_bytes,
                            self.api_passphrase.encode('utf-8'),
                            hashlib.sha256).digest()
                ).decode('utf-8')
            )
            
            return signature_future.result(), passphrase_future.result()
            
        except Exception as e:
            logger.error(f"Error generating signature: {str(e)}")
            raise

    async def get_order_status(self, order_id):
        """
        Retrieve the status of a specific order and save it to self.order_status_buy
        
        Args:
            order_id (str): The ID of the order to check
        
        Returns:
            dict: Order status details or error information
        """
        await self._init_session()
        url = f'{self.base_url}/api/v1/orders/{order_id}'
        
        # Generate signature for this specific request
        timestamp = str(int(time.time() * 1000))
        endpoint = f"/api/v1/orders/{order_id}"
        signature, passphrase = self._generate_signature(timestamp, "GET", endpoint)
        
        headers = self._static_headers.copy()
        headers.update({
            "KC-API-SIGN": signature,
            "KC-API-TIMESTAMP": timestamp,
            "KC-API-PASSPHRASE": passphrase
        })
    
        try:
            async with self.session.get(url, headers=headers) as response:
                if response.status == 200:
                    order_data = await response.json()
                    
                    # Save the order status to an attribute
                    if not hasattr(self, 'order_status_buy'):
                        self.order_status_buy = []
                    
                    self.order_status_buy.append(order_data)
                    
                    return order_data
                else:
                    error_data = await response.json()
                    print(f"Failed to retrieve order status: {response.status}")
                    print(f"Error details: {error_data}")
                    return error_data
        
        except Exception as e:
            logger.error(f"Error retrieving order status: {str(e)}")
            return {"error": str(e)}


    async def get_limit_fills(self):
            """
            Retrieve limit order fills with optional filtering
            
            Args:
                symbol (Optional[str]): Trading pair symbol (e.g., 'XRP-USDT')
                order_id (Optional[str]): Specific order ID to filter fills
                start_at (Optional[int]): Start timestamp in milliseconds
                end_at (Optional[int]): End timestamp in milliseconds
                page (int): Page number for pagination (default: 1)
                page_size (int): Number of results per page (default: 50)
            
            Returns:
                Dict: Order fills data from KuCoin API
            """
            await self._init_session()
            
            
            # Prepare request
            endpoint = "/api/v1/limit/fills"
            timestamp = str(int(time.time() * 1000))
            
            signature, passphrase = self._generate_signature(timestamp, "GET", f"{endpoint}")
            
            headers = self._static_headers.copy()
            headers.update({
                "KC-API-SIGN": signature,
                "KC-API-TIMESTAMP": timestamp,
                "KC-API-PASSPHRASE": passphrase
            })
            
            try:
                async with self.session.get(f"{self.base_url}{endpoint}", 
                                            headers=headers) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        error_data = await response.json()
                        logger.error(f"Failed to retrieve limit fills: {error_data}")
                        return error_data
            
            except Exception as e:
                logger.error(f"Error retrieving limit fills: {str(e)}")
                return {"error": str(e)}



    async def _make_request(self, method: str, endpoint: str, data: Dict = None) -> Dict:
        """Make async API request"""
        await self._init_session()
        url = f"{self.base_url}{endpoint}"
        timestamp = str(int(time.time() * 1000))
        
        body = ""
        if data:
            import json
            body = json.dumps(data)
        
        try:
            signature, passphrase = self._generate_signature(timestamp, method, endpoint, body)
            
            headers = self._static_headers.copy()
            headers.update({
                "KC-API-SIGN": signature,
                "KC-API-TIMESTAMP": timestamp,
                "KC-API-PASSPHRASE": passphrase
            })

            async with getattr(self.session, method.lower())(url, headers=headers, json=data if data else None) as response:
                return await response.json()

        except Exception as e:
            logger.error(f"Request error: {str(e)}")
            raise


    async def place_limit_buy(self, symbol: str, price: str, size: str, 
                            time_in_force: str = "GTC") -> Dict:
        """Place an async limit buy order"""
        start_time = time.perf_counter()
        order_sent_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        
        try:
            order_data = {
                "clientOid": str(uuid.uuid4()),
                "symbol": symbol,
                "type": "limit",
                "side": "buy",
                "price": price,
                "size": size,
                "timeInForce": time_in_force
            }

            response = await self._make_request("POST", "/api/v1/hf/orders", order_data)
            execution_time = time.perf_counter() - start_time
            order_received_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            
            if response.get('code') == '200000':
                successful_buy ={
                    "success": True,
                    "execution_time": round(execution_time,5),
                    "limit_buy_price": price,
                    "order_id": response.get('data', {}).get('orderId', 'unknown'),
                    "order_sent_time": order_sent_time,
                    "order_received_time": order_received_time,
                    "currency_pair": symbol,
                    "order_size": size
                }
                self.placed_limit_buy.append(successful_buy)
                
                return successful_buy
            
            else:
                return {
                    "success": False,
                    "message": response.get('msg', 'Unknown error'),
                    "code": response.get('code'),
                    "limit_buy_price": price,
                    "order_sent_time": order_sent_time,
                    "order_received_time": order_received_time,
                    "currency_pair": symbol,
                    "order_size": size
                }

        except Exception as e:
            order_received_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            return {
                "success": False,
                "message": str(e),
                "code": "ERROR",
                "limit_buy_price": price,
                "order_sent_time": order_sent_time,
                "order_received_time": order_received_time,
                "currency_pair": symbol,
                "order_size": size
            }

    async def place_limit_sell(self, symbol: str, price: str, size: str, 
                            time_in_force: str = "GTC") -> Dict:
        """Place an async limit sell order"""
        start_time = time.perf_counter()
        order_sent_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        
        try:
            order_data = {
                "clientOid": str(uuid.uuid4()),
                "symbol": symbol,
                "type": "limit",
                "side": "sell",
                "price": price,
                "size": size,
                "timeInForce": time_in_force
            }

            response = await self._make_request("POST", "/api/v1/hf/orders", order_data)
            execution_time = time.perf_counter() - start_time
            order_received_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            
            if response.get('code') == '200000':
                successful_sell = {
                    "success": True,
                    "execution_time": round(execution_time,5),
                    "limit_sell_price": price,
                    "order_id": response.get('data', {}).get('orderId', 'unknown'),
                    "order_sent_time": order_sent_time,
                    "order_received_time": order_received_time,
                    "currency_pair": symbol,
                    "order_size": size
                    }
                self.placed_limit_sell.append(successful_sell)
                
                return successful_sell
            
            else:
                return {
                    "success": False,
                    "message": response.get('msg', 'Unknown error'),
                    "code": response.get('code'),
                    "limit_sell_price": price,
                    "order_sent_time": order_sent_time,
                    "order_received_time": order_received_time,
                    "currency_pair": symbol,
                    "order_size": size
                }

        except Exception as e:
            order_received_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            return {
                "success": False,
                "message": str(e),
                "code": "ERROR",
                "limit_sell_price": price,
                "order_sent_time": order_sent_time,
                "order_received_time": order_received_time,
                "currency_pair": symbol,
                "order_size": size
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
            if order["side"] == "buy":
                task = self.place_limit_buy(
                    symbol=order["symbol"],
                    price=order["price"],
                    size=order["size"],
                    time_in_force=order.get("time_in_force", "GTC")
                )
            else:  # sell
                task = self.place_limit_sell(
                    symbol=order["symbol"],
                    price=order["price"],
                    size=order["size"],
                    time_in_force=order.get("time_in_force", "GTC")
                )
            tasks.append(task)
        
        return await asyncio.gather(*tasks)

    async def close(self):
        """Close the aiohttp session"""
        if self.session:
            await self.session.close()
        self.executor.shutdown(wait=False)






async def main():
    import json
    try:
        with open('/root/trading_systems/kucoin_dir/config_api.json', 'r') as file:
            api_creds = json.load(file)

        trader = kucoinHForderManager(
            api_key=api_creds['api_key'],
            api_secret=api_creds['api_secret'],
            api_passphrase=api_creds['api_passphrase'],
            debug=False
        )

        # Example of placing multiple orders concurrently
        orders = [
            {
                "symbol": "XRP-USDT",
                "side": "buy",
                "price": "2",
                "size": "11"
            },
            {
                "symbol": "XRP-USDT",
                "side": "sell",
                "price": "0.2",
                "size": "11"
            }
        ]

        # Execute multiple orders concurrently
        results = await trader.place_multiple_orders(orders)
        print(f"Order Results: {json.dumps(results,indent=4)} ")
        print(f"Placed Limit Buy Orders: {json.dumps(trader.placed_limit_buy,indent=4)} ")
        print(f"Placed Limit Sell Orders: {json.dumps(trader.placed_limit_sell,indent=4)} ")




        # # # Check the status of a specific order
        # order_id = "67485b4014a8920007fafa25"
        # order_status = await trader.get_order_status(order_id)
        # print(f"Order Status: {json.dumps(order_status,indent=4)} ")

        # # # Check limit order fills
        # fills = await trader.get_limit_fills()
        # print(f"Limit Order Fills: {json.dumps(fills,indent=4)} ")

        # # # Clean up
        await trader.close()

    except Exception as e:
        logger.error(f"Main execution error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())