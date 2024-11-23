import requests
import json
from datetime import datetime

class KucoinMarketInfo:
    def __init__(self):
        self.base_url = "https://api.kucoin.com"
        
    def safe_float_convert(self, value, default="Not Available"):
        """Helper function to safely convert values to float"""
        try:
            return float(value) if value is not None else default
        except (ValueError, TypeError):
            return default

    def get_symbol_rules(self, symbol):
        """
        Get trading rules and precision information for a symbol
        """
        try:
            endpoint = "/api/v1/symbols"
            response = requests.get(self.base_url + endpoint)
            response.raise_for_status()
            
            data = response.json()
            if data['code'] == '200000':
                symbol_info = next((item for item in data['data'] if item['symbol'] == symbol), None)
                
                if symbol_info:
                    return {
                        'base_currency': symbol_info.get('baseCurrency', 'Unknown'),
                        'quote_currency': symbol_info.get('quoteCurrency', 'Unknown'),
                        'base_min': self.safe_float_convert(symbol_info.get('baseMinSize')),
                        'base_max': self.safe_float_convert(symbol_info.get('baseMaxSize')),
                        'price_increment': self.safe_float_convert(symbol_info.get('priceIncrement')),
                        'base_increment': self.safe_float_convert(symbol_info.get('baseIncrement')),
                        'quote_min': self.safe_float_convert(symbol_info.get('quoteMinSize')),
                        'quote_max': self.safe_float_convert(symbol_info.get('quoteMaxSize')),
                        'market_status': symbol_info.get('enableTrading', 'Unknown')
                    }
            return {
                'error': 'Symbol rules not found',
                'base_currency': 'Unknown',
                'quote_currency': 'Unknown',
                'base_min': 'Not Available',
                'base_max': 'Not Available',
                'price_increment': 'Not Available',
                'base_increment': 'Not Available',
                'quote_min': 'Not Available',
                'quote_max': 'Not Available',
                'market_status': 'Unknown'
            }
        except Exception as e:
            print(f"Warning: Error getting symbol rules: {e}")
            return None

    def get_currency_info(self, currency):
        """
        Get detailed information about a specific currency
        """
        try:
            endpoint = "/api/v1/currencies/" + currency
            response = requests.get(self.base_url + endpoint)
            response.raise_for_status()
            
            data = response.json()
            if data['code'] == '200000':
                return data['data']
            return None
        except Exception as e:
            print(f"Warning: Error getting currency info: {e}")
            return None

    def get_kucoin_ticker(self, symbol):
        """
        Retrieve current trading information and market rules for a specific trading pair on KuCoin.
        
        Args:
            symbol (str): Trading pair symbol (e.g., 'BTC-USDT')
            
        Returns:
            dict: Trading information including price, volume, and market rules
        """
        result = {
            'symbol': symbol,
            'price': 'Not Available',
            'size': 'Not Available',
            'time': 'Not Available',
            'volume_24h': 'Not Available',
            'high_24h': 'Not Available',
            'low_24h': 'Not Available',
            'change_24h': 'Not Available',
            'status': {
                'price_data': False,
                'stats_data': False,
                'symbol_rules': False,
                'currency_info': False
            }
        }
        
        try:
            # Get current ticker data
            ticker_response = requests.get(f"{self.base_url}/api/v1/market/orderbook/level1?symbol={symbol}")
            ticker_data = ticker_response.json()
            
            if ticker_data['code'] == '200000' and 'data' in ticker_data:
                price_data = ticker_data['data']
                result['price'] = self.safe_float_convert(price_data.get('price'))
                result['size'] = self.safe_float_convert(price_data.get('size'))
                if 'time' in price_data:
                    try:
                        result['time'] = datetime.fromtimestamp(int(price_data['time']) / 1000)
                    except:
                        result['time'] = 'Not Available'
                result['status']['price_data'] = True
            
            # Get 24h stats
            stats_response = requests.get(f"{self.base_url}/api/v1/market/stats?symbol={symbol}")
            stats_data = stats_response.json()
            
            if stats_data['code'] == '200000' and 'data' in stats_data:
                stats = stats_data['data']
                result['volume_24h'] = self.safe_float_convert(stats.get('vol'))
                result['high_24h'] = self.safe_float_convert(stats.get('high'))
                result['low_24h'] = self.safe_float_convert(stats.get('low'))
                result['change_24h'] = self.safe_float_convert(stats.get('changeRate'), 'Not Available')
                if result['change_24h'] != 'Not Available':
                    result['change_24h'] *= 100  # Convert to percentage
                result['status']['stats_data'] = True
            
            # Get symbol rules
            symbol_rules = self.get_symbol_rules(symbol)
            if symbol_rules:
                result.update({
                    'base_currency': symbol_rules.get('base_currency', 'Unknown'),
                    'quote_currency': symbol_rules.get('quote_currency', 'Unknown'),
                    'min_order_size': symbol_rules.get('base_min', 'Not Available'),
                    'max_order_size': symbol_rules.get('base_max', 'Not Available'),
                    'price_increment': symbol_rules.get('price_increment', 'Not Available'),
                    'size_increment': symbol_rules.get('base_increment', 'Not Available'),
                    'min_quote_value': symbol_rules.get('quote_min', 'Not Available'),
                    'is_trading_enabled': symbol_rules.get('market_status', 'Unknown')
                })
                result['status']['symbol_rules'] = True
            
                # Get base currency info if we have the base currency
                if symbol_rules.get('base_currency') != 'Unknown':
                    base_currency_info = self.get_currency_info(symbol_rules['base_currency'])
                    if base_currency_info:
                        result.update({
                            'withdrawal_precision': base_currency_info.get('precision', 'Not Available'),
                            'deposit_status': base_currency_info.get('isDepositEnabled', 'Unknown'),
                            'withdrawal_status': base_currency_info.get('isWithdrawEnabled', 'Unknown')
                        })
                        result['status']['currency_info'] = True
            
            # Calculate suggested price precision if we have price increment
            if isinstance(result.get('price_increment'), (int, float)):
                price_increment_str = str(result['price_increment'])
                if '.' in price_increment_str:
                    result['suggested_price_precision'] = len(price_increment_str.split('.')[-1])
                else:
                    result['suggested_price_precision'] = 0
            
            return result
                
        except requests.exceptions.RequestException as e:
            print(f"Error making API request: {e}")
            return result
        except Exception as e:
            print(f"Unexpected error: {e}")
            return result

# Example usage
if __name__ == "__main__":
    kucoin = KucoinMarketInfo()
    trading_pair = "MIGGLES-USDT"
    data = kucoin.get_kucoin_ticker(trading_pair)
    
    print(f"\nTrading Information for {data['symbol']}:")
    print("\nData Retrieval Status:")
    for key, status in data['status'].items():
        print(f"{key.replace('_', ' ').title()}: {'✓' if status else '✗'}")
    
    print(f"\nPrice Information:")
    print(f"Current Price: {data['price']}" + (" USDT" if data['price'] != 'Not Available' else ""))
    print(f"24h Volume: {data['volume_24h']}")
    print(f"24h High: {data['high_24h']}")
    print(f"24h Low: {data['low_24h']}")
    print(f"24h Change: {data['change_24h']}" + ("%" if isinstance(data['change_24h'], (int, float)) else ""))
    
    print("\nTrading Rules:")
    print(f"Min Order Size: {data['min_order_size']} {data['base_currency']}")
    print(f"Max Order Size: {data['max_order_size']} {data['base_currency']}")
    print(f"Price Increment: {data['price_increment']}")
    print(f"Size Increment: {data['size_increment']}")
    if 'suggested_price_precision' in data:
        print(f"Suggested Price Precision: {data['suggested_price_precision']} decimals")
    
    print("\nCurrency Status:")
    print(f"Trading Enabled: {data['is_trading_enabled']}")
    print(f"Deposits Enabled: {data.get('deposit_status', 'Unknown')}")
    print(f"Withdrawals Enabled: {data.get('withdrawal_status', 'Unknown')}")
    print(f"Withdrawal Precision: {data.get('withdrawal_precision', 'Unknown')} decimals")