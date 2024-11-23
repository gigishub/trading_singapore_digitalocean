import requests
import json
from datetime import datetime

def get_binance_price(symbol):
    """
    Fetch current price of a trading pair from Binance
    """
    try:
        # Binance API endpoint for price
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol.upper()}"
        
        # Make the request
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        data = response.json()
        return {
            "exchange": "Binance",
            "symbol": symbol,
            "price": float(data["price"]),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    except requests.exceptions.RequestException as e:
        return {"error": f"Failed to fetch price: {str(e)}"}
    except KeyError as e:
        return {"error": f"Symbol not found or invalid response format: {str(e)}"}

def main1():
    symbol = "BTCUSDT"
    result = get_binance_price(symbol)
    
    if "error" in result:
        print(result["error"])
    else:
        print(f"\nPrice Data:")
        print(f"Exchange: {result['exchange']}")
        print(f"Symbol: {result['symbol']}")
        print(f"Price: ${result['price']:.8f}")
        print(f"Time: {result['timestamp']}")

# if __name__ == "__main__":
#     main1()


import requests
import json
from datetime import datetime

def get_kucoin_ticker(symbol):
    """
    Retrieve current trading information for a specific trading pair on KuCoin.
    
    Args:
        symbol (str): Trading pair symbol (e.g., 'BTC-USDT')
        
    Returns:
        dict: Trading information including price, volume, and 24h changes
    """
    try:
        # KuCoin API endpoint for ticker information
        base_url = "https://api.kucoin.com"
        endpoint = f"/api/v1/market/orderbook/level1?symbol={symbol}"
        
        # Make API request
        response = requests.get(base_url + endpoint)
        response.raise_for_status()
        
        # Get 24h stats for additional information
        stats_endpoint = f"/api/v1/market/stats?symbol={symbol}"
        stats_response = requests.get(base_url + stats_endpoint)
        stats_response.raise_for_status()
        
        # Parse responses
        ticker_data = response.json()
        stats_data = stats_response.json()
        
        if ticker_data['code'] == '200000' and stats_data['code'] == '200000':
            price_data = ticker_data['data']
            stats = stats_data['data']
            
            return {
                'symbol': symbol,
                'price': float(price_data['price']),
                'size': float(price_data['size']),
                'time': datetime.fromtimestamp(int(price_data['time']) / 1000),
                'volume_24h': float(stats['vol']),
                'high_24h': float(stats['high']),
                'low_24h': float(stats['low']),
                'change_24h': float(stats['changeRate']) * 100  # Convert to percentage
            }
        else:
            raise Exception("Failed to retrieve data from KuCoin API")
            
    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        return None
    except (KeyError, ValueError) as e:
        print(f"Error parsing response data: {e}")
        return None

# Example usage
if __name__ == "__main__":
    # Example: Get BTC-USDT trading information
    trading_pair = "BTC-USDT"
    data = get_kucoin_ticker(trading_pair)
    
    if data:
        print(f"\nTrading Information for {data['symbol']}:")
        print(f"Current Price: ${data['price']:,.2f}")
        print(f"24h Volume: {data['volume_24h']:,.2f}")
        print(f"24h High: ${data['high_24h']:,.2f}")
        print(f"24h Low: ${data['low_24h']:,.2f}")
        print(f"24h Change: {data['change_24h']:,.2f}%")
        print(f"Last Updated: {data['time']}")