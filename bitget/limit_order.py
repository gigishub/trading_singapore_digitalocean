import os
import time
import hmac
import hashlib
import base64
import json
import requests
import logging
import asyncio
import aiohttp
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

async def main():
    symbol = 'XRPUSDT'  # Adjust symbol as per Bitget's format for spot trading
    side = 'buy'             # 'buy' or 'sell'
    price = '0.2'          # Limit price
    quantity = '11'       # Quantity to buy/sell
    
    # API credentials from environment variables
    with open('/root/trading_systems/bitget/api_creds.json', 'r') as file:
        api_creds = json.load(file)

    api_key = api_creds['api_key']
    secret_key = api_creds['secret_key']
    passphrase = api_creds['passphrase']

    # API endpoint
    base_url = 'https://api.bitget.com'

    release_time = datetime.now() + timedelta(seconds=4)  # Example release time

    logger.info(f'Release time: {release_time}')
    async with aiohttp.ClientSession() as session:
        while datetime.now() < release_time:
            await asyncio.sleep(0.00001)

        logger.info('Placing order...')
        order_response = await place_limit_order_session(session, base_url, api_key, secret_key, passphrase, symbol, side, price, quantity)
        print(order_response)

def get_signature(secret_key, timestamp, method, request_path, body=''):
    message = f'{timestamp}{method}{request_path}{body}'
    mac = hmac.new(bytes(secret_key, encoding='utf-8'), bytes(message, encoding='utf-8'), digestmod='sha256')
    d = mac.digest()
    return base64.b64encode(d).decode('utf-8')

def get_headers(api_key, passphrase, timestamp, sign):
    headers = {
        'Content-Type': 'application/json',
        'ACCESS-KEY': api_key,
        'ACCESS-SIGN': sign,
        'ACCESS-TIMESTAMP': timestamp,
        'ACCESS-PASSPHRASE': passphrase,
        'locale': 'en-US'
    }
    return headers

async def place_limit_order_session(session, base_url, api_key, secret_key, passphrase, symbol, side, price, quantity):
    timestamp = str(int(time.time() * 1000))
    method = 'POST'
    request_path = '/api/v2/spot/trade/place-order'
    body = {
        'symbol': symbol,
        'side': side,       # 'buy' or 'sell'
        'orderType': 'limit',
        'price': str(price),
        'size': str(quantity),
        'force': 'gtc'      # Good Till Cancelled
    }
    body_json = json.dumps(body)
    sign = get_signature(secret_key, timestamp, method, request_path, body_json)
    headers = get_headers(api_key, passphrase, timestamp, sign)
    url = base_url + request_path
    
    # Measure execution time
    start_time = time.time()
    async with session.post(url, headers=headers, data=body_json) as response:
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f'Order placed in {execution_time:.6f} seconds')
        
        if response.status == 200:
            return await response.json()
        else:
            logger.error(f'Error placing order: {await response.text()}')
            return None

# Example usage
if __name__ == '__main__':
    asyncio.run(main())