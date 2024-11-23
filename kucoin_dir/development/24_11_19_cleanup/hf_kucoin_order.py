import requests
import time
import uuid
import hmac
import base64
import hashlib
from typing import Optional, Dict, Union
from datetime import datetime
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor

# Set up logging for critical errors only in production
logging.basicConfig(level=logging.ERROR,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KucoinHFTrading:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str, debug: bool = False):
        """Initialize KuCoin HF trading client with optimized session"""
        if not all([api_key, api_secret, api_passphrase]):
            raise ValueError("API key, secret, and passphrase are required")
            
        self.api_key = api_key
        self.api_secret = api_secret if isinstance(api_secret, str) else str(api_secret)
        self.api_passphrase = api_passphrase
        self.base_url = "https://api.kucoin.com"
        self.debug = debug
        
        # Optimize session for HF trading
        self.session = self._create_optimized_session()
        
        # Pre-calculate static headers
        self._static_headers = {
            "KC-API-KEY": self.api_key,
            "KC-API-KEY-VERSION": "2",
            "Content-Type": "application/json"
        }
        
        # ThreadPool for async operations
        self.executor = ThreadPoolExecutor(max_workers=4)

    def _create_optimized_session(self) -> requests.Session:
        """Create an optimized session with connection pooling"""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=2,  # Only retry twice
            backoff_factor=0.1,  # Minimal backoff
            status_forcelist=[500, 502, 503, 504]  # Only retry on server errors
        )
        
        # Configure connection pooling
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,  # Keep up to 10 concurrent connections
            pool_maxsize=10,
            pool_block=False
        )
        
        session.mount("https://", adapter)
        session.headers.update({"Connection": "keep-alive"})
        return session


    def _generate_signature(self, timestamp: str, method: str, endpoint: str, body: str = "") -> tuple:
        """Generate signature with minimal overhead"""
        try:
            signature_string = f"{timestamp}{method}{endpoint}{body}"
            
            # Pre-encode API secret
            secret_bytes = self.api_secret.encode('utf-8')
            
            # Generate signatures in parallel using ThreadPool
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

    def _make_request(self, method: str, endpoint: str, data: Dict = None) -> Dict:
        """Make API request with minimal overhead"""
        url = f"{self.base_url}{endpoint}"
        timestamp = str(int(time.time() * 1000))
        
        body = ""
        if data:
            import json
            body = json.dumps(data)
        
        try:
            # Generate signature and dynamic headers
            signature, passphrase = self._generate_signature(timestamp, method, endpoint, body)
            
            # Update headers efficiently
            headers = self._static_headers.copy()
            headers.update({
                "KC-API-SIGN": signature,
                "KC-API-TIMESTAMP": timestamp,
                "KC-API-PASSPHRASE": passphrase
            })

            # Make request with optimized timeouts
            if method == "GET":
                response = self.session.get(url, headers=headers, timeout=5)
            else:  # POST
                response = self.session.post(url, headers=headers, data=body, timeout=5)

            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}: {response.text}")

            return response.json()

        except Exception as e:
            logger.error(f"Request error: {str(e)}")
            raise

    def place_order_with_timing(self, 
                              symbol: str,
                              side: str,
                              order_type: str,
                              size: Optional[str] = None,
                              price: Optional[str] = None,
                              funds: Optional[str] = None,
                              client_oid: Optional[str] = None,
                              time_in_force: str = "GTC") -> Dict:
        """Place a high-frequency order with optimized timing"""
        start_time = time.perf_counter()  # More precise timing
        
        try:
            # Prepare order data with minimal overhead
            order_data = {
                "clientOid": client_oid or str(uuid.uuid4()),
                "symbol": symbol,
                "type": order_type,
                "side": side,
                "timeInForce": time_in_force
            }

            if order_type == "limit":
                order_data.update({"price": price, "size": size})
            elif order_type == "market":
                order_data.update({"size": size} if size else {"funds": funds})

            response = self._make_request("POST", "/api/v1/hf/orders", order_data)
            
            execution_time = time.perf_counter() - start_time
            
            return {
                "success": response.get('code') == '200000',
                "order_id": response.get('data', {}).get('orderId'),
                "client_oid": order_data['clientOid'],
                "execution_time": execution_time,
                "response": response
            }

        except Exception as e:
            execution_time = time.perf_counter() - start_time
            logger.error(f"Error placing order: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "execution_time": execution_time
            }

    def __del__(self):
        """Cleanup resources"""
        self.executor.shutdown(wait=False)
        self.session.close()

if __name__ == "__main__":
    import json
    try:
        with open('/root/trading_systems/kucoin_dir/config_api.json', 'r') as file:
            api_creds = json.load(file)

        trader = KucoinHFTrading(
            api_key=api_creds['api_key'],
            api_secret=api_creds['api_secret'],
            api_passphrase=api_creds['api_passphrase'],
            debug=False  # Set to False for production
        )
        
        result = trader.place_order_with_timing(
            symbol="XRP-USDT",
            side="buy",
            order_type="limit",
            price="0.2",
            size="11",
            time_in_force="GTC"
        )
        
        print(f"Order execution time: {result['execution_time']*1000:.2f}ms")
        print(f"Success: {result['success']}")
        if not result['success']:
            print(f"Error: {result.get('error', 'Unknown error')}")
        
    except Exception as e:
        logger.error(f"Main execution error: {str(e)}", exc_info=True)


        