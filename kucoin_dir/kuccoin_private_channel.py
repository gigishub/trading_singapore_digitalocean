
import asyncio
import aiohttp
import json
import hmac
import hashlib
import base64
import time
import websockets
import uuid

class KucoinPrivateWebsocket:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.api_url = "https://api.kucoin.com"
        self.ws = None
        self.ping_task = None
        
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
        token_data = await self.get_ws_token()
        if not token_data:
            print("Failed to get WebSocket token")
            return False

        # Construct WebSocket URL
        ws_endpoint = token_data['instanceServers'][0]['endpoint']
        token = token_data['token']
        ws_url = f"{ws_endpoint}?token={token}&[connectId={str(uuid.uuid4())}]"

        try:
            self.ws = await websockets.connect(ws_url)
            print("WebSocket connection established")
            
            # Start ping task
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
            await asyncio.sleep(20)  # KuCoin requires ping every 30s, we do 20s to be safe

    async def subscribe(self, topic: str):
        """Subscribe to a private channel"""
        if not self.ws:
            print("WebSocket not connected")
            return False

        try:
            subscribe_message = {
                "id": str(uuid.uuid4()),
                "type": "subscribe",
                "topic": topic,
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

    async def listen(self):
        """Listen for messages"""
        while True:
            try:
                message = await self.ws.recv()
                data = json.loads(message)
                if data['type'] != 'pong':  # Ignore pong messages
                    print(f"Received: {message}")
                yield data
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

    client = KucoinPrivateWebsocket(
        api_key=config['api_key'],
        api_secret=config['api_secret'],
        api_passphrase=config['api_passphrase']
    )

    # Connect and subscribe to private channels
    if await client.connect():
        # Subscribe to private order updates
        await client.subscribe("/spotMarket/tradeOrders")
        
        try:
            async for message in client.listen():
                # Handle your messages here
                print(f"Processed message: {message}")
        except Exception as e:
            print(f"Error in main loop: {e}")
        finally:
            await client.close()

if __name__ == "__main__":
    asyncio.run(main())