import requests
import pandas as pd
from datetime import datetime, timedelta

def fetch_kucoin_ohlcv(symbol='XRP-USDT', timeframe='1min', start_time=None, end_time=None):
    """
    Fetch OHLCV data from KuCoin API
    
    Parameters:
    - symbol: Trading pair (default XRP-USDT)
    - timeframe: Candle timeframe (1min, 3min, 5min, 15min, 30min, 1hour, 2hour, 4hour, 6hour, 8hour, 12hour, 1day, 1week)
    - start_time: Start time (optional, defaults to 30 days ago)
    - end_time: End time (optional, defaults to current time)
    
    Returns:
    - DataFrame with OHLCV data
    """
    base_url = "https://api.kucoin.com"
    endpoint = "/api/v1/market/candles"
    
    # Set default time range if not provided
    if start_time is None:
        start_time = int((datetime.now() - timedelta(days=30)).timestamp() * 1000)
    else:
        start_time = int(start_time.timestamp() * 1000)
    
    if end_time is None:
        end_time = int(datetime.now().timestamp() * 1000)
    else:
        end_time = int(end_time.timestamp() * 1000)
    
    # Prepare request parameters
    params = {
        'symbol': symbol,
        'type': timeframe,
        'startAt': start_time // 1000,
        'endAt': end_time // 1000
    }
    
    try:
        # Make API request
        response = requests.get(base_url + endpoint, params=params)
        response.raise_for_status()
        
        # Parse response
        data = response.json()
        
        if data.get('code') != '200000':
            raise ValueError(f"API Error: {data}")
        
        # Process candles
        candles = data.get('data', [])
        
        # Create DataFrame with expected structure
        df = pd.DataFrame(candles, columns=[
            'Timestamp', 'Open', 'Close', 'High', 'Low', 'Volume', 'Turnover'
        ])
        
        # Convert timestamp and price columns to numeric
        df['Timestamp'] = pd.to_datetime(df['Timestamp'].astype(int), unit='s')
        df[['Open', 'Close', 'High', 'Low', 'Volume', 'Turnover']] = df[['Open', 'Close', 'High', 'Low', 'Volume', 'Turnover']].astype(float)
        
        # Set timestamp as index
        df.set_index('Timestamp', inplace=True)
        
        # Sort index in ascending order
        df.sort_index(inplace=True)
        
        print(f"Fetched {len(df)} candles for {symbol}")
        print("\nDataFrame Preview:")
        print(df.head())
        
        return df
    
    except requests.RequestException as e:
        print(f"Request Error: {e}")
        return None
    except Exception as e:
        print(f"Unexpected Error: {e}")
        return None

def expected_dataframe_structure():
    """
    Demonstrate the expected DataFrame structure
    """
    # Create a sample DataFrame to show the expected format
    sample_data = {
        'Timestamp': [
            pd.Timestamp('2024-01-01 00:00:00'),
            pd.Timestamp('2024-01-01 00:01:00'),
            pd.Timestamp('2024-01-01 00:02:00')
        ],
        'Open': [0.5, 0.51, 0.52],
        'Close': [0.51, 0.52, 0.53],
        'High': [0.52, 0.53, 0.54],
        'Low': [0.49, 0.50, 0.51],
        'Volume': [1000, 1200, 1100],
        'Turnover': [500, 612, 583]
    }
    
    df = pd.DataFrame(sample_data)
    df.set_index('Timestamp', inplace=True)
    
    print("Expected DataFrame Structure:")
    print(df)
    print("\nColumn Types:")
    print(df.dtypes)
    
    return df





def expected_dataframe_structure():
    """
    Demonstrate the expected DataFrame structure
    """
    # Create a sample DataFrame to show the expected format
    sample_data = {
        'Timestamp': [
            pd.Timestamp('2024-01-01 00:00:00'),
            pd.Timestamp('2024-01-01 00:01:00'),
            pd.Timestamp('2024-01-01 00:02:00')
        ],
        'Open': [0.5, 0.51, 0.52],
        'Close': [0.51, 0.52, 0.53],
        'High': [0.52, 0.53, 0.54],
        'Low': [0.49, 0.50, 0.51],
        'Volume': [1000, 1200, 1100],
        'Turnover': [500, 612, 583]
    }
    
    df = pd.DataFrame(sample_data)
    df.set_index('Timestamp', inplace=True)
    
    print("Expected DataFrame Structure:")
    print(df)
    print("\nColumn Types:")
    print(df.dtypes)
    
    return df

def main():

    # Define start and end times
    start_time = datetime(2024, 1, 1, 0, 0, 0)  # January 1, 2024, 00:00:00
    end_time = datetime(2024, 1, 2, 0, 0, 0)    # January 2, 2024, 00:00:00
    
    # Fetch live data with specified start and end times
    df = fetch_kucoin_ohlcv(symbol='XRP-USDT', timeframe='1min', start_time=start_time, end_time=end_time)
    df.to_pickle('XRP-USDT_1min_20240101.pkl')
if __name__ == "__main__":
    main()


