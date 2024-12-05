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
from datetime import timedelta


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

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
        self.placed_limit_buy_id = []
        self.placed_limit_sell_id = []
        
        #track placed orders
        self.palced_orders =[]
        
        # track filled orders
        self.filled_buy_orders = []
        self.filled_sell_orders = []

        # track all created tasks
        self.pending_tasks = []
    
    def log_timestamp(self):
        """Simple utility function to log timestamps"""
        return datetime.now().strftime('%H:%M:%S.%f')[:-2]


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





    async def _process_order_response(self, response: Dict, start_time: float, 
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
                "orderId": response.get('data', {}).get('orderId', 'unknown'),
                "clientOid": response.get('data', {}).get('clientOid', 'unknown'),
                "order_sent_time": order_sent_time,
                "order_received_time": order_received_time,
                "currency_pair": symbol,
                "order_size": size,
                "side": side
            }
            # if side == 'buy':
            #     #check_fill_buy_task = asyncio.create_task(self.check_if_order_filled(response.get('data', {}).get('orderId'),order_side='buy'))

            # if side == 'sell':
            #     #check_fill_sell_task =asyncio.create_task(self.check_if_order_filled(response.get('data', {}).get('orderId'),order_side='sell'))

                
            # else:
            #     self.placed_limit_sell_id.append(order_result['orderId'])
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
            #logger.debug(f"check orderId to append to order succesfull: {response.get('data',{}).get('orderId')}\n")
            self.palced_orders.append(response.get('data',{})) 
            
            return await asyncio.create_task(self._process_order_response(
                response, start_time, symbol, side, price, size, order_sent_time
            ))

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
            self.placed_limit_buy_id = [
                order for order in self.placed_limit_buy_id 
                if order['order_id'] != order_id
            ]
            self.placed_limit_sell_id = [
                order for order in self.placed_limit_sell_id 
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
            self.placed_limit_buy_id.clear()
            self.placed_limit_sell_id.clear()
            
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



    async def get_order_status_by_clientid(self,clientOid: str) -> Dict:
        """Retrieve the status of a specific order by order ID"""
        await self._init_session()

        endpoint = f"/api/v1/order/client-order/{clientOid}"
        
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
        

    async def get_order_status_by_orderid(self, order_id: str ) -> Dict:
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




    async def all_filled_orders_last_24H(self) -> Dict:
        """Retrieve limit order fills"""
        endpoint = "/api/v1/limit/fills"
        try:
            result= await self._make_request("GET", endpoint)
            if result.get('code') == '200000':
                return result.get('data', [])
            else:
                return result
        except Exception as e:
            logger.error(f"Error retrieving all filled orders: {str(e)}")
            return result



    async def check_if_order_filled(self):
        """Check if placed orders have been filled and store them accordingly."""
        #print(self.succesfully_palced_orders)
    
        for order_info in self.palced_orders:
            order_client_id = order_info.get('clientOid')
            start_time = datetime.now()
            timeout = timedelta(seconds=4)

            while True:
                if datetime.now() - start_time > timeout:
                    break

                try:
                    # Get the status of the order
                    if not order_client_id:
                        break
                    status_response = await self.get_order_status_by_clientid(order_client_id)
                    if status_response.get('code') == '200000':
                        #print(json.dumps(status_response,indent=4))
                        order_data = status_response.get('data', {})
                        dealFunds_value = float(order_data.get('dealFunds', 0))
                        order_side = order_data.get('side')
                        if dealFunds_value != 0:
                            # Order is filled
                            if order_side == 'buy':
                                self.filled_buy_orders.append(order_data)
                            elif order_side == 'sell':
                                self.filled_sell_orders.append(order_data)
                            #logger.info(f"Order client id {order_client_id} filled.")
                            break
                            
                    else:
                        logger.error(f"Error getting status for order {order_client_id}: {status_response.get('msg')}")
                except Exception as e:
                    logger.error(f"Error checking order {order_client_id}: {e}")

                await asyncio.sleep(0.2)


    async def performance_snapshot(self, basecoin: str) -> Dict:
        """
        Analyze trading performance for a specific base coin.
        Args:
            basecoin (str): Base coin symbol (e.g., 'XRP')
        Returns:
            Dict: Performance summary for the specified trading pair
        """
        # Construct full symbol
        symbol = f"{basecoin}-USDT"
        # Get filled orders from last 24 hours
        filled_orders = await self.all_filled_orders_last_24H()
        # Filter orders for the specific symbol
        symbol_orders = [order for order in filled_orders if order['symbol'] == symbol]
        # Performance tracking
        performance = {
            'symbol': symbol,
            'total_trades': len(symbol_orders),
            'buy_trades': 0,
            'sell_trades': 0,
            'total_volume': 0,
            'USDT_buy': 0,
            'USDT_sell': 0,
            'total_profit_loss': 0,
            'profit_loss_percentage': 0
        }
        # Track buy and sell trades
        buy_entries = []
        sell_exits = []
        
        for trade in symbol_orders:
            # Update trade counts and volume
            side = trade['side']
            size = float(trade['size'])
            price = float(trade['price'])
            funds = float(trade['funds'])
            
            performance['total_volume'] += size
            
            if side == 'buy':
                performance['buy_trades'] += 1
                performance['USDT_buy'] += funds
                buy_entries.append({'price': price, 'size': size, 'funds': funds})
            else:  # sell
                performance['sell_trades'] += 1
                performance['USDT_sell'] += funds
                sell_exits.append({'price': price, 'size': size, 'funds': funds})
        
        # Calculate profit/loss
        if buy_entries and sell_exits:
            # Simple average cost and selling price calculation
            avg_buy_price = sum(entry['price'] * entry['size'] for entry in buy_entries) / sum(entry['size'] for entry in buy_entries)
            avg_sell_price = sum(exit['price'] * exit['size'] for exit in sell_exits) / sum(exit['size'] for exit in sell_exits)
            
            # Assume total traded size is the same
            total_traded_size = min(sum(entry['size'] for entry in buy_entries), 
                                    sum(exit['size'] for exit in sell_exits))
            
            # Calculate profit/loss
            total_profit_loss = (avg_sell_price - avg_buy_price) * total_traded_size
            performance['total_profit_loss'] = round(total_profit_loss, 2)
            
            # Calculate profit/loss percentage
            # Use total buy funds as the base for percentage calculation
            total_buy_funds = sum(entry['funds'] for entry in buy_entries)
            performance['profit_loss_percentage'] = round((total_profit_loss / total_buy_funds) * 100, 2)
        
        return performance
    


    async def close(self):
        """Close the aiohttp session and thread pool"""
        if self.pending_tasks:
            logger.info("Waiting for pending tasks to complete...")
            await asyncio.gather(*self.pending_tasks)
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
                "price": "2",
                "size": "1"
            },
            # {
            #     "symbol": "XRP-USDT",
            #     "side": "buy",
            #     "price": "2",
            #     "size": "1.5"
            # },
            {
                "symbol": "XRP-USDT",
                "side": "sell",
                "price": "1",
                "size": "5"
            }
        ]

        print(json.dumps(await trader.place_multiple_orders(orders),indent=4))
        logger.debug('place multiple orders done')

        await trader.check_if_order_filled()
        print(f'filled buy order: {json.dumps(trader.filled_buy_orders,indent=4)}')
        print(f'filled sell orders: {json.dumps(trader.filled_sell_orders,indent=4)}')



        

        # # # Clean up
        await trader.close()

    except Exception as e:
        logger.error(f"Main execution error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())