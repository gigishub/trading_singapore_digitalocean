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
import json
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.ERROR,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KucoinHFOrderManager:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str, debug: bool = False):
        """Initialize KuCoin HF trading client with async support"""
        if not all([api_key, api_secret, api_passphrase]):
            raise ValueError("API key, secret, and passphrase are required")
            
        self.api_key = api_key
        self.api_secret = str(api_secret)
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
        

    def _prepare_order_data(self, symbol: str, side: str, price: str, size: str, 
                             time_in_force: str = "GTC") -> Dict:
        """Standardized order data preparation"""
        return {
            "clientOid": str(uuid.uuid4()),
            "symbol": symbol,
            "type": "limit",
            "side": side,
            "price": price,
            "size": size,
            "timeInForce": time_in_force
        }

    def _process_order_response(self, response: Dict, start_time: float, 
                                symbol: str, side: str, price: str, 
                                size: str, order_sent_time: str) -> Dict:
        """Process and standardize order response"""
        order_received_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        execution_time = time.perf_counter() - start_time
        
        if response.get('code') == '200000':
            order_result = {
                "success": True,
                "execution_time": round(execution_time, 5),
                f"limit_{side}_price": price,
                "order_id": response.get('data', {}).get('orderId', 'unknown'),
                "order_sent_time": order_sent_time,
                "order_received_time": order_received_time,
                "currency_pair": symbol,
                "order_size": size
            }
            if side == 'buy':
                self.placed_limit_buy.append(order_result)
            else:
                self.placed_limit_sell.append(order_result)
            return order_result
        
        return {
            "success": False,
            "message": response.get('msg', 'Unknown error'),
            "code": response.get('code', 'ERROR'),
            f"limit_{side}_price": price,
            "order_sent_time": order_sent_time,
            "order_received_time": order_received_time,
            "currency_pair": symbol,
            "order_size": size
        }

    async def _make_request(self, method: str, endpoint: str, data: Dict = None) -> Dict:
        """Unified async API request method"""
        await self._init_session()
        url = f"{self.base_url}{endpoint}"
        timestamp = str(int(time.time() * 1000))
        
        body = json.dumps(data) if data else ""
        
        try:
            signature, passphrase = self._generate_signature(timestamp, method, endpoint, body)
            
            headers = self._static_headers.copy()
            headers.update({
                "KC-API-SIGN": signature,
                "KC-API-TIMESTAMP": timestamp,
                "KC-API-PASSPHRASE": passphrase
            })

            async with getattr(self.session, method.lower())(url, headers=headers, json=data) as response:
                return await response.json()

        except Exception as e:
            logger.error(f"Request error: {str(e)}")
            raise

    async def place_limit_order(self, symbol: str, side: str, price: str, size: str, 
                                 time_in_force: str = "GTC") -> Dict:
        """Unified method for placing limit orders"""
        start_time = time.perf_counter()
        order_sent_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        
        try:
            order_data = self._prepare_order_data(symbol, side, price, size, time_in_force)
            response = await self._make_request("POST", "/api/v1/hf/orders", order_data)
            
            return self._process_order_response(
                response, start_time, symbol, side, price, size, order_sent_time
            )

        except Exception as e:
            order_received_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            return {
                "success": False,
                "message": str(e),
                "code": "ERROR",
                f"limit_{side}_price": price,
                "order_sent_time": order_sent_time,
                "order_received_time": order_received_time,
                "currency_pair": symbol,
                "order_size": size
            }



    async def cancel_order_by_id(self, order_id: str) -> Dict:
        """
        Cancel a specific order by its order ID.
        Args:
            order_id (str): The unique server-assigned order ID to cancel.
        Returns:
            Dict: Cancellation result containing details about the cancellation attempt.
        """
        await self._init_session()
        endpoint = f"/api/v1/orders/{order_id}"
        
        try:
            response = await self._make_request("DELETE", endpoint)
            

            # Remove cancelled order from tracking lists
            self.placed_limit_buy = [
                order for order in self.placed_limit_buy 
                if order['order_id'] != order_id
            ]
            self.placed_limit_sell = [
                order for order in self.placed_limit_sell 
                if order['order_id'] != order_id
            ]
            
            return response
        
        except Exception as e:
            return response
        

    async def cancel_all_orders(self, basecoin: Optional[str] = None, trade_type: str = "TRADE") -> Dict:
        """
        Cancel all open orders, optionally filtered by symbol and trade type.
        Args:
            symbol (Optional[str], optional): The trading pair to cancel orders for. 
                If None, cancels orders across all pairs. Defaults to None.
            trade_type (str, optional): Type of trading. 
                Options: 'TRADE', 'MARGIN_TRADE', 'MARGIN_ISOLATED_TRADE'. 
                Defaults to 'TRADE'.
        Returns:
            Dict: Cancellation result containing details about cancelled orders.
        """
        if basecoin:
            symbol = basecoin +"-USDT"
        await self._init_session()
        endpoint = "/api/v1/orders"
        if basecoin:
            endpoint += f"?symbol={symbol}&tradeType={trade_type}"
        
        try:
            # Use GET method with params for cancelling all orders
            response = await self._make_request("DELETE", endpoint)
            
            # Clear tracking lists
            self.placed_limit_buy.clear()
            self.placed_limit_sell.clear()
            
            return response
        
        except Exception as e:
            return response



    async def place_limit_buy(self, symbol: str, price: str, size: str, 
                               time_in_force: str = "GTC") -> Dict:
        """Convenience method for limit buy orders"""
        return await self.place_limit_order(symbol, "buy", price, size, time_in_force)

    async def place_limit_sell(self, symbol: str, price: str, size: str, 
                                time_in_force: str = "GTC") -> Dict:
        """Convenience method for limit sell orders"""
        return await self.place_limit_order(symbol, "sell", price, size, time_in_force)

    async def place_multiple_orders(self, orders: List[Dict]) -> List[Dict]:
        """Place multiple orders concurrently"""
        tasks = [
            self.place_limit_order(
                symbol=order["symbol"],
                side=order["side"],
                price=order["price"],
                size=order["size"],
                time_in_force=order.get("time_in_force", "GTC")
            ) for order in orders
        ]
        return await asyncio.gather(*tasks)


    async def get_order_status(self, order_id: str) -> Dict:
        """Retrieve the status of a specific order by order ID"""
        await self._init_session()
        endpoint = f"/api/v1/orders/{order_id}"
        
        timestamp = str(int(time.time() * 1000))
        signature, passphrase = self._generate_signature(timestamp, "GET", endpoint)
        
        headers = self._static_headers.copy()
        headers.update({
            "KC-API-SIGN": signature,
            "KC-API-TIMESTAMP": timestamp,
            "KC-API-PASSPHRASE": passphrase
        })

        try:
            async with self.session.get(f"{self.base_url}{endpoint}", headers=headers) as response:
                return await response.json()
        
        except Exception as e:
            logger.error(f"Error retrieving order status: {str(e)}")
            return {"error": str(e)}

    async def get_limit_fills(self) -> Dict:
        """Retrieve limit order fills"""
        endpoint = "/api/v1/limit/fills"
        return await self._make_request("GET", endpoint)

    async def close(self):
        """Close the aiohttp session and thread pool"""
        if self.session:
            await self.session.close()
        self.executor.shutdown(wait=False)



async def main():
    import json
    try:
        with open('/root/trading_systems/kucoin_dir/config_api.json', 'r') as file:
            api_creds = json.load(file)

        trader = KucoinHFOrderManager(
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
                "price": "0.9",
                "size": "11"
            },
            {
                "symbol": "DOGE-USDT",
                "side": "buy",
                "price": "0.1",
                "size": "11"
            }
        ]

        print(json.dumps(await trader.get_limit_fills(),indent=4))

        #print(await trader.get_order_status('6748d44bb6d5080007087831'))
        
        #print(await trader.place_multiple_orders(orders))

        #print(await trader.cancel_order_by_id('6748d1f78b053b000735f1f6'))
        #print(await trader.place_multiple_orders(orders))

        #print(json.dumps(await trader.get_limit_fills(),indent=4))
        # Execute multiple orders concurrently

        # # Cancel all buy orders
        # await trader.cancel_order(cancel_type='buy')

        # # Cancel all sell orders
        print (await trader.cancel_all_orders())
        # # # Clean up
        await trader.close()

    except Exception as e:
        logger.error(f"Main execution error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())