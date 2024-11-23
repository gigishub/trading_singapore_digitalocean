import asyncio
import aiohttp
import json
import hmac
import hashlib
import base64
import time

class KucoinPrivateWebsocketDebug:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.api_url = "https://api.kucoin.com"
        
    def generate_signature(self, timestamp: str):
        """Generate precise signature for Kucoin API"""
        body = ''  # Empty body for GET request
        signature_str = f"{timestamp}GET/api/v1/bullet-private{body}"
        signature = base64.b64encode(
            hmac.new(
                self.api_secret.encode('utf-8'), 
                signature_str.encode('utf-8'), 
                hashlib.sha256
            ).digest()
        ).decode('utf-8')
        return signature

    async def debug_token_request(self):
        """Request WebSocket token with precise authentication"""
        timestamp = str(int(time.time() * 1000))
        signature = self.generate_signature(timestamp)
        
        print(f"Debug Information:")
        print(f"Timestamp: {timestamp}")
        print(f"API Key: {self.api_key}")
        print(f"Signature: {signature}")
        print(f"Passphrase: {self.api_passphrase}")

        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    "KC-API-KEY": self.api_key,
                    "KC-API-SIGN": signature,
                    "KC-API-TIMESTAMP": timestamp,
                    "KC-API-PASSPHRASE": self.api_passphrase,
                    "KC-API-METHOD": "GET",
                    "Content-Type": "application/json"
                }
                
                async with session.post(
                    f"{self.api_url}/api/v1/bullet-private", 
                    headers=headers
                ) as response:
                    print(f"Response Status: {response.status}")
                    response_text = await response.text()
                    print(f"Response Body: {response_text}")
                    
                    if response.status == 200:
                        return await response.json()
                    else:
                        print("Token request failed")
        except Exception as e:
            print(f"Token Request Error: {e}")
        return None


async def main():
    import json
    with open('/root/trading_systems/kucoin_dir/config_api.json','r') as f:
            data = json.load(f)

    API_KEY = data['api_key']
    API_SECRET = data['api_secret']  # Corrected
    API_PASSPHRASE = data['api_passphrase']  # Corrected
    debugger = KucoinPrivateWebsocketDebug(API_KEY, API_SECRET, API_PASSPHRASE)
    await debugger.debug_token_request()

if __name__ == "__main__":
    asyncio.run(main())   
   

