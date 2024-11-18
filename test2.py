import requests
from datetime import datetime, timedelta
import time
from typing import Dict, Optional, Tuple

class TokenHistoryChecker:
    def __init__(self):
        self.coingecko_api = "https://api.coingecko.com/api/v3"
        self.dex_screener_api = "https://api.dexscreener.com/latest/dex"
        
    def check_token_history(self, symbol: str, listing_date: str) -> Dict:
        """
        Check if a token existed before a specific listing date and get price info
        
        Args:
            symbol: Token symbol (e.g., 'CELA')
            listing_date: Date string in format 'YYYY-MM-DD'
            
        Returns:
            Dict containing token history and price information
        """
        result = {
            "symbol": symbol,
            "listing_date": listing_date,
            "is_new_token": True,
            "first_found_date": None,
            "first_found_on": None,
            "contract_address": None,
            "current_price": None,
            "initial_price": None,
            "price_change": None,
            "volume_24h": None,
            "liquidity": None,
            "error": None
        }
        
        try:
            # Check DEX Screener first (usually faster and more reliable for new tokens)
            dex_data = self._check_dexscreener(symbol)
            if dex_data and dex_data["first_date"] != "1970-01-01":  # Valid date check
                result.update({
                    "is_new_token": False,
                    "first_found_date": dex_data["first_date"],
                    "first_found_on": f"DEX ({dex_data['dex_name']})",
                    "contract_address": dex_data.get("contract_address"),
                    "current_price": dex_data.get("current_price"),
                    "initial_price": dex_data.get("initial_price"),
                    "price_change": dex_data.get("price_change"),
                    "volume_24h": dex_data.get("volume_24h"),
                    "liquidity": dex_data.get("liquidity")
                })
                return result
            
            # Fallback to CoinGecko
            coingecko_data = self._check_coingecko(symbol)
            if coingecko_data:
                result.update({
                    "is_new_token": False,
                    "first_found_date": coingecko_data["first_date"],
                    "first_found_on": "CoinGecko",
                    "contract_address": coingecko_data.get("contract_address"),
                    "current_price": coingecko_data.get("current_price"),
                    "initial_price": coingecko_data.get("initial_price"),
                    "price_change": coingecko_data.get("price_change"),
                    "volume_24h": coingecko_data.get("volume_24h"),
                    "liquidity": coingecko_data.get("liquidity")
                })
                return result
                
        except Exception as e:
            result["error"] = str(e)
            
        return result
    
    def _check_dexscreener(self, symbol: str) -> Optional[Dict]:
        """Check if token exists on DEX Screener with price data"""
        try:
            search_url = f"{self.dex_screener_api}/search?q={symbol}"
            response = requests.get(search_url)
            data = response.json()
            
            if data.get('pairs'):
                valid_pairs = [
                    pair for pair in data['pairs'] 
                    if pair.get('timestamp', 0) > 0 and
                    (pair.get('baseToken', {}).get('symbol', '').upper() == symbol.upper() or
                     pair.get('quoteToken', {}).get('symbol', '').upper() == symbol.upper())
                ]
                
                if not valid_pairs:
                    return None
                    
                # Sort pairs by timestamp to find the oldest
                oldest_pair = sorted(valid_pairs, key=lambda x: x.get('timestamp', float('inf')))[0]
                # Get the most liquid pair for current price
                most_liquid_pair = sorted(valid_pairs, key=lambda x: float(x.get('liquidity', {}).get('usd', 0)), reverse=True)[0]
                
                timestamp = oldest_pair.get('timestamp', 0) / 1000  # Convert from milliseconds
                
                if timestamp > 0:
                    return {
                        "first_date": datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d'),
                        "dex_name": oldest_pair.get('dexId', 'Unknown DEX'),
                        "contract_address": oldest_pair.get('baseToken', {}).get('address'),
                        "current_price": float(most_liquid_pair.get('priceUsd', 0)),
                        "initial_price": float(oldest_pair.get('priceUsd', 0)),
                        "price_change": float(most_liquid_pair.get('priceChange', {}).get('h24', 0)),
                        "volume_24h": float(most_liquid_pair.get('volume', {}).get('h24', 0)),
                        "liquidity": float(most_liquid_pair.get('liquidity', {}).get('usd', 0))
                    }
            return None
            
        except Exception as e:
            print(f"DEX Screener API error: {e}")
            return None
            
    def _check_coingecko(self, symbol: str) -> Optional[Dict]:
        """Check if token exists on CoinGecko with price data"""
        try:
            search_url = f"{self.coingecko_api}/search?query={symbol}"
            response = requests.get(search_url)
            data = response.json()
            
            if data.get('coins'):
                for coin in data['coins']:
                    if coin['symbol'].upper() == symbol.upper():
                        # Get current data
                        coin_id = coin['id']
                        current_url = f"{self.coingecko_api}/coins/{coin_id}"
                        current_data = requests.get(current_url).json()
                        
                        # Get historical data for initial price
                        if current_data.get('genesis_date'):
                            history_url = f"{self.coingecko_api}/coins/{coin_id}/history?date={current_data['genesis_date']}"
                            historical_data = requests.get(history_url).json()
                            
                            return {
                                "first_date": current_data['genesis_date'],
                                "contract_address": list(current_data.get('platforms', {}).values())[0] if current_data.get('platforms') else None,
                                "current_price": current_data.get('market_data', {}).get('current_price', {}).get('usd'),
                                "initial_price": historical_data.get('market_data', {}).get('current_price', {}).get('usd'),
                                "price_change": current_data.get('market_data', {}).get('price_change_percentage_24h'),
                                "volume_24h": current_data.get('market_data', {}).get('total_volume', {}).get('usd'),
                                "liquidity": None  # CoinGecko doesn't provide direct liquidity data
                            }
            return None
            
        except Exception as e:
            print(f"CoinGecko API error: {e}")
            return None

def main():
    checker = TokenHistoryChecker()
    
    # Example for CELA/USDT
    result = checker.check_token_history('CELA', '2024-11-14')
    
    if result['error']:
        print(f"Error checking token: {result['error']}")
    else:
        if result['is_new_token']:
            print(f"{result['symbol']} appears to be a new token listing")
        else:
            print(f"\nToken Information for {result['symbol']}:")
            print(f"First found on: {result['first_found_on']}")
            print(f"First listed date: {result['first_found_date']}")
            if result['contract_address']:
                print(f"Contract address: {result['contract_address']}")
            if result['current_price']:
                print(f"\nPrice Information:")
                print(f"Current Price: ${result['current_price']:.8f}")
                if result['initial_price']:
                    print(f"Initial Price: ${result['initial_price']:.8f}")
                if result['price_change']:
                    print(f"24h Price Change: {result['price_change']:.2f}%")
                if result['volume_24h']:
                    print(f"24h Volume: ${result['volume_24h']:,.2f}")
                if result['liquidity']:
                    print(f"Liquidity: ${result['liquidity']:,.2f}")

if __name__ == "__main__":
    main()