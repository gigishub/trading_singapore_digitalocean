import websocket
import json
import time
import hmac
import hashlib
import base64
from threading import Thread, Event
import logging
from queue import Queue

class GateIOWebSocket:
    def __init__(self, api_key, api_secret, api_version='v3'):
        self.api_key = api_key
        self.api_secret = api_secret.encode()
        self.ws = None
        self.connected = False
        self.authenticated = False
        self.base_url = f"wss://ws.gate.io/{api_version}/"
        self.api_version = api_version
        self.message_queue = Queue()
        self.auth_event = Event()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('GateIOWebSocket')
        
    def generate_signature(self, nonce):
        """Generate authentication signature based on API version"""
        if self.api_version == 'v3':
            message = str(nonce).encode()
            signature = base64.b64encode(
                hmac.new(self.api_secret, message, hashlib.sha512).digest()
            ).decode()
        else:  # v4
            message = str(nonce).encode()
            signature = hmac.new(
                self.api_secret, message, hashlib.sha512
            ).hexdigest()
        return signature

    def on_message(self, ws, message):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(message)
            self.logger.debug(f"Received message: {data}")
            
            # Handle authentication response
            if data.get('method') == 'server.sign' or \
               (data.get('id') == 12312 and 'result' in data):
                if data.get('error') is None and data.get('result', {}).get('status') == 'success':
                    self.authenticated = True
                    self.auth_event.set()
                    self.logger.info("Authentication successful")
                else:
                    self.logger.error(f"Authentication failed: {data}")
                    self.auth_event.set()
            
            # Put message in queue for processing
            self.message_queue.put(data)
            
        except json.JSONDecodeError:
            self.logger.error(f"Failed to parse message: {message}")

    def on_error(self, ws, error):
        self.logger.error(f"WebSocket error: {error}")
        self.connected = False
        self.authenticated = False
        self.auth_event.set()  # Prevent hanging

    def on_close(self, ws, close_status_code, close_msg):
        self.logger.info(f"WebSocket connection closed: {close_status_code} - {close_msg}")
        self.connected = False
        self.authenticated = False
        self.auth_event.set()  # Prevent hanging

    def on_open(self, ws):
        """Handle WebSocket connection open"""
        self.logger.info("WebSocket connection established")
        self.connected = True
        
        # Send authentication request
        nonce = int(time.time() * 1000)
        signature = self.generate_signature(nonce)
        
        auth_message = {
            "id": 12312,
            "method": "server.sign",
            "params": [self.api_key, signature, nonce]
        }
        
        ws.send(json.dumps(auth_message))

    def connect(self):
        """Establish WebSocket connection with automatic reconnection"""
        # Initialize WebSocket with callbacks
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(
            self.base_url,
            on_message=self.on_message,
            on_open=self.on_open,
            on_error=self.on_error,
            on_close=self.on_close
        )

        # Start WebSocket connection in a separate thread
        ws_thread = Thread(target=self.ws.run_forever)
        ws_thread.daemon = True
        ws_thread.start()

        # Wait for authentication with timeout
        timeout = 5
        self.auth_event.clear()
        if not self.auth_event.wait(timeout):
            raise TimeoutError("Authentication timed out")
        
        if not self.authenticated:
            raise ConnectionError("Authentication failed")

    def place_order(self, market, order_type, side, amount, price=None):
        """
        Place an order as quickly as possible
        
        Args:
            market (str): Trading pair (e.g., "BTC_USDT")
            order_type (int): 1 for limit order, 2 for market order
            side (int): 1 for sell, 2 for buy
            amount (str): Amount to trade
            price (str, optional): Price for limit orders
        """
        if not (self.connected and self.authenticated):
            raise ConnectionError("WebSocket not connected or not authenticated")

        order_id = int(time.time() * 1000)
        order_data = {
            "id": order_id,
            "method": "order.put",
            "params": [
                market,
                order_type,
                side,
                amount,
                price if price else "0"
            ]
        }

        try:
            self.ws.send(json.dumps(order_data))
            
            # Wait for order response
            timeout = 5
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    response = self.message_queue.get(timeout=0.1)
                    if response.get('id') == order_id:
                        return response
                except Exception:
                    continue
                    
            raise TimeoutError("Order response timed out")
            
        except Exception as e:
            self.logger.error(f"Error placing order: {e}")
            raise

    def close(self):
        """Clean up and close the WebSocket connection"""
        if self.ws:
            self.ws.close()
        self.connected = False
        self.authenticated = False

# Example usage
# if __name__ == "__main__":
#     API_KEY = "your_api_key"
#     API_SECRET = "your_api_secret"
    
#     # Initialize and connect
#     client = GateIOWebSocket(API_KEY, API_SECRET)
    
#     try:
#         client.connect()
#         print("Connected and authenticated successfully!")
        
#         # Example: Place a limit buy order
#         response = client.place_order(
#             market="BTC_USDT",
#             order_type=1,  # limit order
#             side=2,        # buy
#             amount="0.001",
#             price="30000"
#         )
#         print(f"Order response: {response}")
        
#     except Exception as e:
#         print(f"Error: {e}")
#     finally:
#         client.close()
if __name__ == "__main__":
    import orjson
    """Example usage"""
    with open('/root/trading_systems/gate_io/api_creds.json', 'r') as f:
        creds = orjson.loads(f.read())
    api_key = creds['api_key']
    api_secret = creds['api_secret']
    symbol = "BTC_USDT"
    
    # Initialize and connect
    client = GateIOWebSocket(api_key, api_secret)
    client.connect()
    
    # Example: Place a limit buy order
    try:
        response = client.place_order(
            market="BTC_USDT",
            order_type=1,  # limit order
            side=2,        # buy
            amount="0.00001",
            price="30000"
        )
        print(f"Order response: {response}")
    finally:
        client.close()