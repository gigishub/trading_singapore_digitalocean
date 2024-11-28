import time
import base64
import hmac
import hashlib
import requests
import json
import uuid

def check_spot_orders():
    api_key = '6742fc248781350001def902'
    api_secret =  '648a3d3c-8852-453a-a6b6-8831d5b3078e'
    api_passphrase ='buhyabuhna'
    
    # Generate client order id
    client_oid = str(uuid.uuid4().hex)
    
    # Endpoint for spot orders
    url = 'https://api.kucoin.com/api/v1/orders'
    
    # Current timestamp
    now = int(time.time() * 1000)
    
    # Request data
    data = {"clientOid": client_oid}
    data_json = json.dumps(data)
    
    # Generate signature
    str_to_sign = str(now) + 'GET' + '/api/v1/orders' + '?status=active'
    signature = base64.b64encode(
        hmac.new(api_secret.encode('utf-8'), 
                str_to_sign.encode('utf-8'), 
                hashlib.sha256).digest()
    )
    
    # Generate encrypted passphrase
    passphrase = base64.b64encode(
        hmac.new(api_secret.encode('utf-8'), 
                api_passphrase.encode('utf-8'), 
                hashlib.sha256).digest()
    )
    
    # Headers
    headers = {
        "KC-API-SIGN": signature,
        "KC-API-TIMESTAMP": str(now),
        "KC-API-KEY": api_key,
        "KC-API-PASSPHRASE": passphrase,
        "KC-API-KEY-VERSION": "2",
        "Content-Type": "application/json"
    }
    
    try:
        # Make request
        response = requests.request('GET', url, headers=headers, data=data_json)
        
        # Check if request was successful
        if response.status_code == 200:
            result = response.json()
            if result.get('code') == '200000':
                print("✅ API connection successful!")
                print("\nOrders:", json.dumps(result.get('data', []), indent=2))
                return result['data']
            else:
                print(f"❌ API request failed with code {result.get('code')}:")
                print(result.get('msg', 'No error message provided'))
                return None
        else:
            print(f"❌ Request failed with status code: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error occurred: {str(e)}")
        return None

def main():
    print("Checking spot orders...")
    orders = check_spot_orders()
    
    if orders is not None:
        # You can process the orders here
        if len(orders) == 0:
            print("No active orders found")
        else:
            print(f"Found {len(orders)} active orders")

if __name__ == "__main__":
    main()