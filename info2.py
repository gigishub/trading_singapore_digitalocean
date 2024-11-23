import requests
from datetime import datetime
from typing import Dict, Optional
from statistics import mean
import time

class TokenPriceChecker:
    def __init__(self):
        self.exchanges = {
            'binance': 'https://api.binance.com/api/v3/ticker/price?symbol={}USDT',
            'kucoin': 'https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={}-USDT',
            'huobi': 'https://api.huobi.pro/market/detail/merged?symbol={}usdt',
            'gate': 'https://api.gateio.ws/api/v4/spot/tickers?currency_pair={}_USDT',
            'okx': 'https://www.okx.com/api/v5/market/ticker?instId={}-USDT',
            'bitget': 'https://api.bitget.com/api/spot/v1/market/ticker?symbol={}USDT',
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
                return float(data['tick']['close'])
            elif exchange == 'gate':
                return float(data[0]['last']) if data else None
            elif exchange == 'okx':
                return float(data['data'][0]['last']) if data['data'] else None
            elif exchange == 'bitget':
                return float(data['data']['close']) if data['data'] else None
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

    def get_token_price_history(self, token: str, interval_seconds: int = 300, duration_hours: int = 24) -> Dict:
        """
        Monitor token prices over time
        
        Args:
            token (str): Token symbol
            interval_seconds (int): Interval between price checks in seconds
            duration_hours (int): Duration to monitor in hours
            
        Returns:
            Dict: Dictionary containing price history for each exchange
        """
        iterations = int((duration_hours * 3600) / interval_seconds)
        history = {
            'price_history': [],
            'start_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
        }
        
        for _ in range(iterations):
            prices = self.get_token_prices(token)
            history['price_history'].append(prices)
            time.sleep(interval_seconds)
            
        history['end_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
        return history

# Example usage
def main():
    # Create an instance of the TokenPriceChecker
    checker = TokenPriceChecker()
    
    # Example 1: Get current prices for Bitcoin
    print("Getting Bitcoin prices...")
    btc_prices = checker.get_token_prices('BTC')
    
    print("\nBitcoin Prices Across Exchanges:")
    print("-" * 50)
    for exchange, price in btc_prices['exchanges'].items():
        print(f"{exchange.capitalize():<10}: {price}")
    print(f"\nAverage Price: ${btc_prices['average_price']:,.2f}")
    print(f"Timestamp: {btc_prices['timestamp']}")
    
    # Example 2: Get current prices for Ethereum
    print("\nGetting Ethereum prices...")
    eth_prices = checker.get_token_prices('ETH')
    
    print("\nEthereum Prices Across Exchanges:")
    print("-" * 50)
    for exchange, price in eth_prices['exchanges'].items():
        print(f"{exchange.capitalize():<10}: {price}")
    print(f"\nAverage Price: ${eth_prices['average_price']:,.2f}")
    print(f"Timestamp: {eth_prices['timestamp']}")
    
    # Example 3: Monitor a token's price for a short duration (1 minute with 20-second intervals)
    print("\nMonitoring SOL prices for 1 minute...")
    sol_history = checker.get_token_price_history('SOL', interval_seconds=20, duration_hours=1/60)
    
    print("\nSOL Price History:")
    print("-" * 50)
    for entry in sol_history['price_history']:
        print(f"Time: {entry['timestamp']}")
        print(f"Average Price: ${entry['average_price']:,.2f}")
        print("-" * 30)

if __name__ == "__main__":
    main()