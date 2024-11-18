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
        # Convert secret key to bytes if it's not already
        self.api_secret = api_secret.encode() if isinstance(api_secret, str) else api_secret
        self.ws = None
        self.connected = False
        self.authenticated = False
        # Fix the version string to ensure single 'v'
        self.api_version = api_version.replace('vv', 'v')  # Fix double v if present
        self.base_url = f"wss://ws.gate.io/{self.api_version}/"
        self.message_queue = Queue()
        self.auth_event = Event()
        
        # Setup logging
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('GateIOWebSocket')
        self.logger.debug(f"API Version string : {self.api_version}")

    def generate_signature_v3(self, nonce):
        """Generate signature for API v3 with additional validation"""
        try:
            # Ensure nonce is properly formatted
            nonce_str = str(nonce)
            message = nonce_str.encode('utf-8')
            
            self.logger.debug(f"Generating v3 signature")
            self.logger.debug(f"Nonce (raw): {nonce}")
            self.logger.debug(f"Message to sign (utf-8): {message}")
            self.logger.debug(f"Secret key length: {len(self.api_secret)} bytes")
            
            # Generate HMAC-SHA512
            hmac_obj = hmac.new(self.api_secret, message, hashlib.sha512)
            # Get the digest
            signature = base64.b64encode(hmac_obj.digest()).decode('utf-8')
            
            self.logger.debug(f"Generated signature: {signature[:10]}...")
            self.logger.debug(f"Signature length: {len(signature)}")
            
            return signature
            
        except Exception as e:
            self.logger.error(f"Error in signature generation: {str(e)}")
            raise

    def on_message(self, ws, message):
        """Handle incoming WebSocket messages with enhanced error reporting"""
        try:
            data = json.loads(message)
            self.logger.debug(f"Received message: {json.dumps(data, indent=2)}")
            
            # Handle authentication response
            if (data.get('method') == 'server.sign' or 
                (data.get('id') == 12312 and 'result' in data)):
                
                if 'error' in data:
                    error_info = data['error']
                    error_code = error_info.get('code', 'Unknown')
                    error_message = error_info.get('message', 'Unknown error')
                    
                    self.logger.error(f"Authentication Error Details:")
                    self.logger.error(f"Error Code: {error_code}")
                    self.logger.error(f"Error Message: {error_message}")
                    self.logger.error(f"Full Response: {json.dumps(data, indent=2)}")
                    
                    if error_code == 4:
                        self.logger.error("Code 4 typically indicates:")
                        self.logger.error("1. Invalid API key format")
                        self.logger.error("2. Incorrect signature generation")
                        self.logger.error("3. Timestamp/nonce synchronization issues")
                    
                    self.authenticated = False
                
                elif data.get('result', {}).get('status') == 'success':
                    self.logger.info("Authentication successful!")
                    self.authenticated = True
                
                self.auth_event.set()
            
            self.message_queue.put(data)
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse message: {message}")
            self.logger.error(f"Parse error: {str(e)}")

    def on_open(self, ws):
        """Handle WebSocket connection open with enhanced debugging"""
        self.logger.info(f"WebSocket connection established to {self.base_url}")
        self.connected = True
        
        # Generate authentication request with current timestamp
        nonce = int(time.time() * 1000)
        self.logger.debug(f"Current time: {time.time()}")
        self.logger.debug(f"Generated nonce: {nonce}")
        
        signature = self.generate_signature_v3(nonce)
        
        auth_message = {
            "id": 12312,
            "method": "server.sign",
            "params": [self.api_key, signature, nonce]
        }
        
        self.logger.debug("Authentication Request Details:")
        self.logger.debug(f"API Key: {self.api_key[:5]}...")
        self.logger.debug(f"API Key Length: {len(self.api_key)}")
        self.logger.debug(f"Signature: {signature[:10]}...")
        self.logger.debug(f"Nonce: {nonce}")
        self.logger.debug(f"Full auth message: {json.dumps(auth_message, indent=2)}")
        
        ws.send(json.dumps(auth_message))

    def on_error(self, ws, error):
        self.logger.error(f"WebSocket error: {error}")
        self.connected = False
        self.authenticated = False
        self.auth_event.set()

    def on_close(self, ws, close_status_code, close_msg):
        self.logger.info(f"WebSocket connection closed: {close_status_code} - {close_msg}")
        self.connected = False
        self.authenticated = False
        self.auth_event.set()

    def connect(self):
        """Establish WebSocket connection with automatic reconnection"""
        self.logger.info(f"Initiating connection to Gate.io WebSocket API {self.api_version}")
        
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(
            self.base_url,
            on_message=self.on_message,
            on_open=self.on_open,
            on_error=self.on_error,
            on_close=self.on_close
        )

        ws_thread = Thread(target=self.ws.run_forever)
        ws_thread.daemon = True
        ws_thread.start()

        # Wait for authentication with timeout
        timeout = 5
        self.auth_event.clear()
        self.logger.info(f"Waiting for authentication response (timeout: {timeout}s)")
        
        if not self.auth_event.wait(timeout):
            self.logger.error("Authentication timed out after 5 seconds")
            raise TimeoutError("Authentication timed out")
        
        if not self.authenticated:
            self.logger.error("Authentication failed - check previous error messages")
            raise ConnectionError("Authentication failed")

    def place_order(self, market, order_type, side, amount, price=None):
        """Place an order through WebSocket"""
        if not self.connected:
            self.logger.error("Cannot place order: WebSocket not connected")
            raise ConnectionError("WebSocket not connected")
        
        if not self.authenticated:
            self.logger.error("Cannot place order: Not authenticated")
            raise ConnectionError("Not authenticated")

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

        self.logger.info(f"Placing {['sell', 'buy'][side-1]} order for {amount} {market}")
        self.logger.debug(f"Order details: {json.dumps(order_data, indent=2)}")

        try:
            self.ws.send(json.dumps(order_data))
            
            timeout = 5
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    response = self.message_queue.get(timeout=0.1)
                    if response.get('id') == order_id:
                        if 'error' in response:
                            self.logger.error(f"Order error: {response['error']}")
                        else:
                            self.logger.info(f"Order placed successfully: {response.get('result')}")
                        return response
                except Exception as e:
                    self.logger.debug(f"Waiting for order response... ({e})")
                    continue
                    
            self.logger.error("Order response timed out after 5 seconds")
            raise TimeoutError("Order response timed out")
            
        except Exception as e:
            self.logger.error(f"Error placing order: {str(e)}")
            raise

    def close(self):
        """Clean up and close the WebSocket connection"""
        self.logger.info("Closing WebSocket connection...")
        if self.ws:
            self.ws.close()
        self.connected = False
        self.authenticated = False
        self.logger.info("WebSocket connection closed")
# Example usage
if __name__ == "__main__":

    import orjson
    """Example usage"""
    with open('/root/trading_systems/gate_io/api_creds.json', 'r') as f:
        creds = orjson.loads(f.read())
    api_key = creds['api_key']
    api_secret = creds['api_secret']
    symbol = "BTC_USDT"
    

    # Your API credentials
    API_KEY = api_key = creds['api_key']
    API_SECRET = api_secret = creds['api_secret']  # Make sure this is the raw secret key
    
    # Initialize and connect
    client = GateIOWebSocket(API_KEY, API_SECRET)
    
    try:
        # Add this after loading your credentials
        print(f"API Key length: {len(api_key)}")
        print(f"API Secret length: {len(api_secret)}")
        print(f"Current system time: {time.time()}")
        client.connect()
        print("Connected and authenticated successfully!")
        
        # Example: Place a limit buy order
        response = client.place_order(
            market="BTC_USDT",
            order_type=1,  # limit order
            side=2,        # buy
            amount="0.00001",
            price="30000"
        )
        print(f"Order response: {response}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()