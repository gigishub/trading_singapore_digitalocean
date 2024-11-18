import json
import time
import hmac
import hashlib
from websocket import create_connection
import orjson
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Load credentials
with open('/root/trading_systems/gate_io/api_creds.json', 'r') as f:
    creds = orjson.loads(f.read())

api_key = creds['api_key']
secret_key = creds['api_secret']

def get_sign(secret_key, message):
    if isinstance(secret_key, str):
        secret_key = secret_key.encode('utf-8')
    if isinstance(message, str):
        message = message.encode('utf-8')
    h = hmac.new(secret_key, message, hashlib.sha512)
    return h.hexdigest()

ws = create_connection("wss://ws.gate.io/v4/")

nonce = int(time.time() * 1000)
signature_string = str(nonce)
signature = get_sign(secret_key, signature_string)

# Log the details
logging.debug(f"Timestamp (nonce): {nonce}")
logging.debug(f"Signature: {signature}")
logging.debug(f"API Key Length: {len(api_key)}")

# Authenticate
ws.send(json.dumps({
    "id": 12312,
    "method": "server.sign",
    "params": [api_key, signature, nonce]
}))

response = ws.recv()
logging.debug(f"Response: {response}")

ws.close()