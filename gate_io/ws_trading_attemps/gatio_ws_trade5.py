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
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret  # Ensure this is a string
        self.ws = None
        self.connected = False
        self.authenticated = False
        self.base_url = 'wss://ws.gate.io/v3/'
        self.message_queue = Queue()
        self.auth_event = Event()

        # Setup logging
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('GateIOWebSocket')

    def get_sign(self, secret_key, message):
        """Generate HMAC SHA512 signature."""
        # Convert to bytes if necessary
        if isinstance(secret_key, str):
            secret_key = secret_key.encode('utf-8')
        if isinstance(message, str):
            message = message.encode('utf-8')

        h = hmac.new(secret_key, message, hashlib.sha512)
        signature = base64.b64encode(h.digest()).decode()
        return signature

    def on_message(self, ws, message):
        """Handle incoming WebSocket messages."""
        try:
            data = json.loads(message)
            self.logger.debug(f"Received message: {json.dumps(data, indent=2)}")

            # Handle authentication response
            if data.get('method') == 'server.sign' or data.get('id') == 12312:
                if data.get('error'):
                    error_info = data['error']
                    self.logger.error(f"Authentication failed: {error_info}")
                    self.authenticated = False
                else:
                    self.logger.info("Authentication successful!")
                    self.authenticated = True
                self.auth_event.set()

            self.message_queue.put(data)

        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse message: {message}")
            self.logger.error(f"Parse error: {str(e)}")

    def on_open(self, ws):
        """Handle WebSocket connection open."""
        self.logger.info(f"WebSocket connection established to {self.base_url}")
        self.connected = True

        # Generate nonce and signature
        nonce = int(time.time() * 1000)
        signature = self.get_sign(self.api_secret, str(nonce))

        auth_message = {
            "id": 12312,
            "method": "server.sign",
            "params": [self.api_key, signature, nonce]
        }

        self.logger.debug("Authentication Request Details:")
        self.logger.debug(f"Auth message: {json.dumps(auth_message, indent=2)}")

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
        """Establish WebSocket connection."""
        self.logger.info(f"Initiating connection to Gate.io WebSocket API v3")

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
        timeout = 10
        self.auth_event.clear()
        self.logger.info(f"Waiting for authentication response (timeout: {timeout}s)")

        if not self.auth_event.wait(timeout):
            self.logger.error("Authentication timed out")
            raise TimeoutError("Authentication timed out")

        if not self.authenticated:
            self.logger.error("Authentication failed - check previous error messages")
            raise ConnectionError("Authentication failed")

    def place_order(self, market, side, amount, price=None):
        """Place an order through WebSocket."""
        if not self.connected:
            self.logger.error("Cannot place order: WebSocket not connected")
            raise ConnectionError("WebSocket not connected")

        if not self.authenticated:
            self.logger.error("Cannot place order: Not authenticated")
            raise ConnectionError("Not authenticated")

        order_id = int(time.time() * 1000)
        order_data = {
            "id": order_id,
            "method": "order.put_limit",
            "params": [
                market,
                "buy" if side.lower() == "buy" else "sell",
                price if price else "0",
                amount
            ]
        }

        self.logger.info(f"Placing {side} order for {amount} {market}")
        self.logger.debug(f"Order details: {json.dumps(order_data, indent=2)}")

        try:
            self.ws.send(json.dumps(order_data))

            timeout = 10
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    response = self.message_queue.get(timeout=0.1)
                    if response.get('id') == order_id:
                        if response.get('error'):
                            self.logger.error(f"Order error: {response['error']}")
                            return response
                        else:
                            self.logger.info(f"Order placed successfully: {response.get('result')}")
                            return response
                except Exception as e:
                    self.logger.debug(f"Waiting for order response... ({e})")
                    continue

            self.logger.error("Order response timed out")
            raise TimeoutError("Order response timed out")

        except Exception as e:
            self.logger.error(f"Error placing order: {str(e)}")
            raise

    def close(self):
        """Clean up and close the WebSocket connection."""
        self.logger.info("Closing WebSocket connection...")
        if self.ws:
            self.ws.close()
        self.connected = False
        self.authenticated = False
        self.logger.info("WebSocket connection closed")

# Example usage
if __name__ == "__main__":
    import json
    """Example usage"""
    with open('/root/trading_systems/gate_io/api_creds.json', 'r') as f:
        creds = json.loads(f.read())
    api_key = "fe0f98cb7474719a521d09a4ffc55416"
    api_secret = "8e1e2d2b92e71fbdc7476edd82b14f6a65fd959cdd0d3071166b2568fec919e4"
    symbol = "BTC_USDT"

    # Initialize and connect
    client = GateIOWebSocket(api_key, api_secret)

    try:
        print(f"API Key length: {len(api_key)}")
        print(f"API Secret length: {len(api_secret)}")
        print(f"Current system time: {time.time()}")
        client.connect()
        print("Connected and authenticated successfully!")

        # Example: Place a limit buy order
        response = client.place_order(
            market="BTC_USDT",
            side="buy",
            amount="0.00001",
            price="30000"
        )
        print(f"Order response: {response}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()