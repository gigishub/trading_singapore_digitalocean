import asyncio
import aiohttp
import json
import hmac
import hashlib
import base64
import time
import websockets
import uuid
from datetime import datetime

class KucoinOpenOrdersMonitor:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.api_url = "https://api.kucoin.com"
        self.ws = None
        self.ping_task = None
        self.open_orders = {}  # Track open orders
        
    def generate_signature(self, timestamp: str, method: str, endpoint: str, body: str = ''):
        """Generate signature for authentication"""
        str_to_sign = f"{timestamp}{method}{endpoint}{body}"
        signature = base64.b64encode(
            hmac.new(
                self.api_secret.encode('utf-8'),
                str_to_sign.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode('utf-8')
        return signature

    def generate_passphrase(self):
        """Generate encrypted passphrase"""
        return base64.b64encode(
            hmac.new(
                self.api_secret.encode('utf-8'),
                self.api_passphrase.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode('utf-8')

    async def get_initial_open_orders(self):
        """Get initial list of open orders via REST API"""
        endpoint = "/api/v1/orders"
        timestamp = str(int(time.time() * 1000))
        signature = self.generate_signature(timestamp, "GET", endpoint)
        encrypted_passphrase = self.generate_passphrase()
        
        headers = {
            "KC-API-KEY": self.api_key,
            "KC-API-SIGN": signature,
            "KC-API-TIMESTAMP": timestamp,
            "KC-API-PASSPHRASE": encrypted_passphrase,
            "KC-API-KEY-VERSION": "2",
            "Content-Type": "application/json"
        }
        
        params = {"status": "active"}  # Get only active orders
        
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.api_url}{endpoint}",
                headers=headers,
                params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data['code'] == '200000':
                        orders = data.get('data', {}).get('items', [])
                        # Store orders in our tracking dict
                        for order in orders:
                            self.open_orders[order['id']] = order
                        return orders
                    else:
                        print(f"Failed to get open orders: {data.get('msg')}")
                        return []
                else:
                    response_text = await response.text()
                    print(f"Failed to get open orders: {response.status}")
                    print(f"Response: {response_text}")
                    return []

    async def get_ws_token(self):
        """Get WebSocket token with proper authentication"""
        endpoint = "/api/v1/bullet-private"
        timestamp = str(int(time.time() * 1000))
        signature = self.generate_signature(timestamp, "POST", endpoint)
        encrypted_passphrase = self.generate_passphrase()
        
        headers = {
            "KC-API-KEY": self.api_key,
            "KC-API-SIGN": signature,
            "KC-API-TIMESTAMP": timestamp,
            "KC-API-PASSPHRASE": encrypted_passphrase,
            "KC-API-KEY-VERSION": "2",
            "Content-Type": "application/json"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.api_url}{endpoint}",
                headers=headers
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('data')
                else:
                    response_text = await response.text()
                    print(f"Token request failed: {response.status}")
                    print(f"Response: {response_text}")
                    return None

    async def connect(self):
        """Establish WebSocket connection"""
        # First get initial open orders
        print("Fetching initial open orders...")
        initial_orders = await self.get_initial_open_orders()
        print(f"Found {len(initial_orders)} open orders")
        
        # Then connect to WebSocket
        token_data = await self.get_ws_token()
        if not token_data:
            print("Failed to get WebSocket token")
            return False

        ws_endpoint = token_data['instanceServers'][0]['endpoint']
        token = token_data['token']
        ws_url = f"{ws_endpoint}?token={token}&[connectId={str(uuid.uuid4())}]"

        try:
            self.ws = await websockets.connect(ws_url)
            print("WebSocket connection established")
            self.ping_task = asyncio.create_task(self.ping_loop())
            return True
        except Exception as e:
            print(f"WebSocket connection failed: {e}")
            return False

    async def ping_loop(self):
        """Maintain connection with periodic pings"""
        while True:
            if self.ws and not self.ws.closed:
                try:
                    await self.ws.send(json.dumps({
                        "id": str(uuid.uuid4()),
                        "type": "ping"
                    }))
                except Exception as e:
                    print(f"Ping failed: {e}")
                    break
            await asyncio.sleep(20)

    async def subscribe(self):
        """Subscribe to order updates channel"""
        if not self.ws:
            print("WebSocket not connected")
            return False

        try:
            subscribe_message = {
                "id": str(uuid.uuid4()),
                "type": "subscribe",
                "topic": "/spotMarket/tradeOrders",
                "privateChannel": True,
                "response": True
            }
            
            await self.ws.send(json.dumps(subscribe_message))
            response = await self.ws.recv()
            print(f"Subscription response: {response}")
            return True
        except Exception as e:
            print(f"Subscription failed: {e}")
            return False

    def format_order_info(self, order):
        """Format order information for display"""
        return (f"Symbol: {order.get('symbol', 'N/A')} | "
                f"Type: {order.get('type', 'N/A')} | "
                f"Side: {order.get('side', 'N/A')} | "
                f"Price: {order.get('price', 'N/A')} | "
                f"Size: {order.get('size', 'N/A')} | "
                f"Filled: {order.get('filledSize', '0')}")

    async def process_order_update(self, data):
        """Process order update messages"""
        order_id = data.get('orderId')
        if not order_id:
            return
            
        status = data.get('type', '').lower()
        
        if status == 'open':
            self.open_orders[order_id] = data
            print(f"\n[{datetime.now()}] New order opened:")
            print(self.format_order_info(data))
            
        elif status in ['match', 'filled']:
            if order_id in self.open_orders:
                self.open_orders[order_id].update(data)
                if status == 'filled':
                    del self.open_orders[order_id]
                    print(f"\n[{datetime.now()}] Order filled and removed:")
                else:
                    print(f"\n[{datetime.now()}] Order partially filled:")
                print(self.format_order_info(data))
                
        elif status == 'canceled':
            if order_id in self.open_orders:
                del self.open_orders[order_id]
                print(f"\n[{datetime.now()}] Order canceled and removed:")
                print(self.format_order_info(data))
        
        print(f"Current open orders: {len(self.open_orders)}")

    async def listen(self):
        """Listen for order updates"""
        while True:
            try:
                message = await self.ws.recv()
                data = json.loads(message)
                
                if data['type'] == 'message':
                    await self.process_order_update(data['data'])
                elif data['type'] == 'error':
                    print(f"Error received: {data}")
                
            except Exception as e:
                print(f"Error in message handling: {e}")
                break

    async def close(self):
        """Close WebSocket connection"""
        if self.ping_task:
            self.ping_task.cancel()
        if self.ws:
            await self.ws.close()

async def main():
    # Load your API credentials
    with open('/root/trading_systems/kucoin_dir/config_api.json', 'r') as f:
        config = json.load(f)

    monitor = KucoinOpenOrdersMonitor(
        api_key=config['api_key'],
        api_secret=config['api_secret'],
        api_passphrase=config['api_passphrase']
    )

    print("Starting KuCoin Open Orders Monitor...")
    
    if await monitor.connect():
        await monitor.subscribe()
        print("\nMonitoring open orders... Press Ctrl+C to exit\n")
        
        try:
            await monitor.listen()
        except KeyboardInterrupt:
            print("\nShutting down...")
        except Exception as e:
            print(f"\nError in main loop: {e}")
        finally:
            await monitor.close()

if __name__ == "__main__":
    asyncio.run(main())