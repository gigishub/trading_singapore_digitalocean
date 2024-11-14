import websockets
import json
import time
import uuid
import asyncio
from typing import Dict, Optional, Union, List
import aiohttp
from datetime import datetime
import time
import hmac
import hashlib
from urllib.parse import urlencode
import traceback
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class MEXCWebsocketTrading:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.mexc.com"  # Changed from www.mexc.com
        self.ws_url = "wss://wsaws.mexc.com/ws"  # Fixed WebSocket URL
        self.listen_keys = set()
        self.subscriptions = {}
        self.websocket = None

    
    def _generate_signature(self, params: Dict) -> str:
        """Generate HMAC SHA256 signature for authentication"""
        query_string = urlencode(params)
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature

    async def create_listen_key(self) -> Dict:
        """Create a new listen key for user data stream"""
        async with aiohttp.ClientSession() as session:
            try:
                start_time = time.time()
                # Add timestamp parameter
                timestamp = int(time.time() * 1000)
                params = {
                    'timestamp': timestamp
                }
                # Generate signature using the parameters
                signature = self._generate_signature(params)
                params['signature'] = signature

                async with session.post(
                    f"{self.base_url}/api/v3/userDataStream",
                    headers={
                        "X-MEXC-APIKEY": self.api_key,
                        "Content-Type": "application/json"
                    },
                    params=params  # Include the parameters in the request
                ) as response:
                    result = await response.json()
                    execution_time = time.time() - start_time

                    if response.status == 200:
                        self.listen_keys.add(result['listenKey'])
                        return {
                            'success': True,
                            'listen_key': result['listenKey'],
                            'execution_time': execution_time,
                            'response': result
                        }
                    return {
                        'success': False,
                        'error': result,
                        'execution_time': execution_time
                    }
            except Exception as e:
                return {
                    'success': False,
                    'error': str(e),
                    'execution_time': time.time() - start_time
                }


    async def place_limit_order(self, symbol: str, side: str, quantity: float, price: float,
                                client_order_id: Optional[str] = None) -> Dict:
        """Place a limit order"""
        try:
            start_time = time.time()
            timestamp = int(time.time() * 1000)
            params = {
                "symbol": symbol,
                "side": side.upper(),
                "type": "LIMIT",
                "quantity": str(quantity),
                "price": str(price),
                "newClientOrderId": client_order_id or str(uuid.uuid4()),
                "recvWindow": 5000,
                "timestamp": timestamp
            }

            # Generate the query string without the signature
            query_string = urlencode(sorted(params.items()))
            # Generate the signature
            signature = self._generate_signature(params)
            # Append the signature to the query string
            final_query_string = f"{query_string}&signature={signature}"

            headers = {
                "X-MEXC-APIKEY": self.api_key,
                "Content-Type": "application/x-www-form-urlencoded"
            }

            # Send the request with the query string as the body
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/api/v3/order",
                    headers=headers,
                    data=final_query_string  # Send the parameters in the body
                ) as response:
                    result = await response.json()
                    execution_time = time.time() - start_time
                    if response.status == 200:
                        return {
                            'success': True,
                            'order_id': result.get('orderId'),
                            'client_oid': params['newClientOrderId'],
                            'execution_time': execution_time,
                            'response': result
                        }
                    else:
                        return {
                            'success': False,
                            'error': result,
                            'execution_time': execution_time
                        }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'execution_time': time.time() - start_time
            }

    async def get_listen_keys(self) -> Dict:
        """Query all active listen keys"""
        async with aiohttp.ClientSession() as session:
            try:
                start_time = time.time()
                async with session.get(
                    f"{self.base_url}/api/v3/userDataStream",
                    headers={"X-MEXC-APIKEY": self.api_key}
                ) as response:
                    result = await response.json()
                    return {
                        'success': True,
                        'listen_keys': result['listenKey'],
                        'execution_time': time.time() - start_time,
                        'response': result
                    }
            except Exception as e:
                return {
                    'success': False,
                    'error': str(e),
                    'execution_time': time.time() - start_time
                }


    async def keep_alive_listen_key(self, listen_key: str) -> Dict:
        """Keep-alive a listen key to prevent timeout"""
        async with aiohttp.ClientSession() as session:
            try:
                start_time = time.time()
                # Add timestamp parameter
                timestamp = int(time.time() * 1000)
                params = {
                    'listenKey': listen_key,
                    'timestamp': timestamp
                }
                # Generate signature using the parameters
                signature = self._generate_signature(params)
                params['signature'] = signature

                async with session.put(
                    f"{self.base_url}/api/v3/userDataStream",
                    headers={
                        "X-MEXC-APIKEY": self.api_key,
                        "Content-Type": "application/json"
                    },
                    params=params  # Include parameters in the request
                ) as response:
                    execution_time = time.time() - start_time
                    if response.status == 200:
                        return {
                            'success': True,
                            'listen_key': listen_key,
                            'execution_time': execution_time,
                            'response': await response.json()
                        }
                    result = await response.json()
                    return {
                        'success': False,
                        'error': result,
                        'execution_time': execution_time
                    }
            except Exception as e:
                return {
                    'success': False,
                    'error': str(e),
                    'execution_time': time.time() - start_time
                }
    

    async def delete_listen_key(self, listen_key: str) -> Dict:
        """Delete a listen key to close the stream"""
        async with aiohttp.ClientSession() as session:
            try:
                start_time = time.time()
                # Add timestamp parameter
                timestamp = int(time.time() * 1000)
                params = {
                    'listenKey': listen_key,
                    'timestamp': timestamp
                }
                # Generate signature using the parameters
                signature = self._generate_signature(params)
                params['signature'] = signature

                async with session.delete(
                    f"{self.base_url}/api/v3/userDataStream",
                    headers={
                        "X-MEXC-APIKEY": self.api_key,
                        "Content-Type": "application/json"
                    },
                    params=params  # Include parameters in the request
                ) as response:
                    execution_time = time.time() - start_time
                    if response.status == 200:
                        self.listen_keys.discard(listen_key)
                        return {
                            'success': True,
                            'execution_time': execution_time,
                            'response': await response.json()
                        }
                    result = await response.json()
                    return {
                        'success': False,
                        'error': result,
                        'execution_time': execution_time
                    }
            except Exception as e:
                return {
                    'success': False,
                    'error': str(e),
                    'execution_time': time.time() - start_time
                }




    async def place_limit_order2(self, symbol: str, side: str, quantity: float, price: float,
                              client_order_id: Optional[str] = None) -> Dict:
        """Place a limit order"""
        try:
            start_time = time.time()
            order_data = {
                "symbol": symbol,
                "side": side.upper(),
                "type": "LIMIT",
                "quantity": str(quantity),
                "price": str(price),
                "clientOrderId": client_order_id or str(uuid.uuid4())
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/api/v3/order",
                    headers={"X-MEXC-APIKEY": self.api_key},
                    json=order_data
                ) as response:
                    result = await response.json()
                    return {
                        'success': response.status == 200,
                        'order_id': result.get('orderId'),
                        'client_oid': order_data['clientOrderId'],
                        'execution_time': time.time() - start_time,
                        'response': {
                            'code': str(response.status),
                            'data': result
                        }
                    }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'execution_time': time.time() - start_time
            }

    async def place_market_order(self, symbol: str, side: str, quantity: float,
                               client_order_id: Optional[str] = None) -> Dict:
        """Place a market order"""
        try:
            start_time = time.time()
            order_data = {
                "symbol": symbol,
                "side": side.upper(),
                "type": "MARKET",
                "quantity": str(quantity),
                "clientOrderId": client_order_id or str(uuid.uuid4())
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/api/v3/order",
                    headers={"X-MEXC-APIKEY": self.api_key},
                    json=order_data
                ) as response:
                    result = await response.json()
                    return {
                        'success': response.status == 200,
                        'order_id': result.get('orderId'),
                        'client_oid': order_data['clientOrderId'],
                        'execution_time': time.time() - start_time,
                        'response': {
                            'code': str(response.status),
                            'data': result
                        }
                    }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'execution_time': time.time() - start_time
            }

    async def connect_websocket(self, listen_key: str):
        """Connect to MEXC WebSocket with listen key"""
        ws_url_with_key = f"{self.ws_url}?listenKey={listen_key}"
        try:
            self.websocket = await websockets.connect(ws_url_with_key)
            print("WebSocket connected")
        except Exception as e:
            print(f"WebSocket connection error: {e}")


    async def subscribe_account_updates(self, callback):
        """Subscribe to account updates"""
        if not self.websocket:
            await self.connect_websocket()
        
        subscription_message = {
            "method": "SUBSCRIPTION",
            "params": ["spot@private.account.v3.api"]
        }
        await self.websocket.send(json.dumps(subscription_message))
        self.subscriptions["account_updates"] = callback
        
        asyncio.create_task(self._handle_messages())

    async def subscribe_deals(self, callback):
        """Subscribe to deals updates"""
        if not self.websocket:
            await self.connect_websocket()
        
        subscription_message = {
            "method": "SUBSCRIPTION",
            "params": ["spot@private.deals.v3.api"]
        }
        await self.websocket.send(json.dumps(subscription_message))
        self.subscriptions["deals"] = callback

    async def subscribe_orders(self, callback):
        """Subscribe to order updates"""
        if not self.websocket:
            await self.connect_websocket()
        
        subscription_message = {
            "method": "SUBSCRIPTION",
            "params": ["spot@private.orders.v3.api"]
        }
        await self.websocket.send(json.dumps(subscription_message))
        self.subscriptions["orders"] = callback

    async def _handle_messages(self):
        """Handle incoming WebSocket messages"""
        try:
            while True:
                message = await self.websocket.recv()
                data = json.loads(message)
                
                channel = data.get('c')
                if channel in self.subscriptions:
                    await self.subscriptions[channel](data)
                
        except websockets.exceptions.ConnectionClosed:
            # Implement reconnection logic here
            pass
        except Exception as e:
            print(f"Error handling message: {e}")


    async def close(self):
        try:
            """Close WebSocket connection and cleanup"""
            if self.websocket:
                await self.websocket.close()
            for listen_key in list(self.listen_keys):
                await self.delete_listen_key(listen_key)
                logging.debug(f"Listen key {listen_key} deleted")
            self.listen_keys.clear()
        except Exception as e:
            logging.error(f"Error closing connection: {str(e)}")



async def main():
    import json
    try:
        with open('/root/trading_systems/MEXC_dir/api_creds.json', 'r') as file:
            api_creds = json.load(file)

        trader = MEXCWebsocketTrading(
            api_key=api_creds['api_key'],
            api_secret=api_creds['api_secret']
        )

        key_info= await trader.create_listen_key()
        print(f'key_info: {key_info}')
        await asyncio.sleep(1)

        order_response = await trader.place_limit_order('XRPUSDT', 'buy', 5, 0.2)
        logging.debug(f"Order placed{order_response}")
        await asyncio.sleep(1)

        for listen_key in list(trader.listen_keys):
            print(await trader.delete_listen_key(listen_key))
            print(f"Listen key {listen_key} deleted")
            await asyncio.sleep(1)
        

        
    except Exception as e:
        print(f"Main execution error: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())