import requests
from datetime import datetime
from typing import Dict, Optional
from statistics import mean
import time
import json

class TokenPriceChecker:
    def __init__(self):
        self.exchanges = {
            'binance': 'https://api.binance.com/api/v3/ticker/price?symbol={}USDT',
            'kucoin': 'https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={}-USDT',
            'huobi': 'https://api.huobi.pro/market/trade?symbol={}usdt',  # Updated Huobi endpoint
            'gate': 'https://api.gateio.ws/api/v4/spot/tickers?currency_pair={}_USDT',
            'okx': 'https://www.okx.com/api/v5/market/ticker?instId={}-USDT',
            'bitget': 'https://api.bitget.com/api/v3/spot/market/ticker?symbol={}USDT',  # Updated Bitget endpoint
            'bybit': 'https://api.bybit.com/v5/market/tickers?category=spot&symbol={}USDT'
        }
        
    def _fetch_price(self, url: str, exchange: str, token: str) -> Optional[float]:
        """
        Fetch price from a specific exchange with error handling
        """
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()
            
            # Handle different response formats for each exchange
            if exchange == 'binance':
                return float(data['price'])
            elif exchange == 'kucoin':
                return float(data['data']['price']) if data['data'] else None
            elif exchange == 'huobi':
                return float(data['tick']['data'][0]['price']) if data['tick'] and data['tick']['data'] else None
            elif exchange == 'gate':
                return float(data[0]['last']) if data else None
            elif exchange == 'okx':
                return float(data['data'][0]['last']) if data['data'] else None
            elif exchange == 'bitget':
                return float(data['data']['lastPr']) if 'data' in data else None
            elif exchange == 'bybit':
                return float(data['result']['list'][0]['lastPrice']) if data['result']['list'] else None
                
        except (requests.RequestException, KeyError, ValueError, TypeError):
            return None

    def get_token_prices(self, token: str) -> Dict:
        """
        Get token prices from all major exchanges
        
        Args:
            token (str): Token symbol (e.g., 'BTC', 'ETH')
            
        Returns:
            Dict: Dictionary containing prices from each exchange, average price,
                 and human-readable timestamp
        """
        result = {
            'exchanges': {},
            'average_price': None,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
        }
        
        valid_prices = []
        
        # Fetch prices from all exchanges
        for exchange, url_template in self.exchanges.items():
            url = url_template.format(token.upper())
            price = self._fetch_price(url, exchange, token)
            
            if price is not None:
                result['exchanges'][exchange] = price
                valid_prices.append(price)
            else:
                result['exchanges'][exchange] = 'Not Available'
        
        # Calculate average price if we have any valid prices
        if valid_prices:
            result['average_price'] = round(mean(valid_prices), 2)
        
        return result

# Example usage showing dictionary structure
def main():
    checker = TokenPriceChecker()
    
    # Get Bitcoin prices and show raw dictionary structure
    btc_prices = checker.get_token_prices('MIGGLES')
    print("\nRaw Dictionary Structure (BTC):")
    print(json.dumps(btc_prices, indent=2))
    
    print("\nHow to access the data:")
    print("1. Access specific exchange price:")
    print("   btc_prices['exchanges']['binance']")
    print("   btc_prices['exchanges']['bybit']")
    print("\n2. Access average price:")
    print("   btc_prices['average_price']")
    print("\n3. Access timestamp:")
    print("   btc_prices['timestamp']")

if __name__ == "__main__":
    main()