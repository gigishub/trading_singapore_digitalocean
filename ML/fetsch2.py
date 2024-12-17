import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone

def fetch_kucoin_ohlcv(symbol='BTC-USDT', timeframe='1hour', start_time=None, end_time=None):
    """
    Fetch historical OHLCV data from KuCoin API.

    Parameters:
    - symbol (str): Trading pair symbol (e.g., 'BTC-USDT').
    - timeframe (str): Candle timeframe (e.g., '1hour').
    - start_time (datetime): Start time for data fetching (inclusive).
    - end_time (datetime): End time for data fetching (exclusive).

    Returns:
    - pandas.DataFrame: DataFrame containing OHLCV data.
    """
    base_url = 'https://api.kucoin.com'
    endpoint = '/api/v1/market/candles'
    limit = 1500  # Maximum limit per request

    # Set default start and end times
    if start_time is None:
        start_time = datetime(2019, 1, 1, tzinfo=timezone.utc)
    if end_time is None:
        end_time = datetime.utcnow().replace(tzinfo=timezone.utc)

    # Convert timeframe string to minutes
    timeframe_to_minutes = {
        '1min': 1, '3min': 3, '5min': 5, '15min': 15, '30min': 30,
        '1hour': 60, '2hour': 120, '4hour': 240, '6hour': 360,
        '8hour': 480, '12hour': 720, '1day': 1440, '1week': 10080
    }
    if timeframe not in timeframe_to_minutes:
        raise ValueError(f"Invalid timeframe '{timeframe}'.")
    timeframe_minutes = timeframe_to_minutes[timeframe]
    delta_per_request = timedelta(minutes=limit * timeframe_minutes)

    all_candles = []
    current_end_time = end_time

    while True:
        current_start_time = current_end_time - delta_per_request
        if current_start_time < start_time:
            current_start_time = start_time

        params = {
            'symbol': symbol,
            'type': timeframe,
            'startAt': int(current_start_time.timestamp()),
            'endAt': int(current_end_time.timestamp()),
        }

        try:
            response = requests.get(f"{base_url}{endpoint}", params=params)
            response.raise_for_status()
            data = response.json()
            if data['code'] != '200000':
                print(f"API Error: {data}")
                break

            candles = data['data']
            if not candles:
                print("No more data available.")
                break

            all_candles.extend(candles)
            print(f"Fetched {len(candles)} candles from {current_start_time} to {current_end_time}")

            # Update current_end_time for the next iteration
            earliest_timestamp = int(candles[-1][0])
            current_end_time = datetime.fromtimestamp(earliest_timestamp, tz=timezone.utc)

            if current_end_time <= start_time:
                break

            # Delay between requests
            time.sleep(3)

        except Exception as e:
            print(f"Error fetching data: {e}")
            break

    if not all_candles:
        print("No data fetched.")
        return pd.DataFrame()

    # Create DataFrame
    df = pd.DataFrame(all_candles, columns=['time', 'open', 'close', 'high', 'low', 'volume', 'turnover'])
    df['time'] = pd.to_datetime(df['time'].astype(float), unit='s', utc=True)
    numeric_cols = ['open', 'close', 'high', 'low', 'volume', 'turnover']
    df[numeric_cols] = df[numeric_cols].astype(float)
    df.sort_values('time', inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df

# Example usage
if __name__ == "__main__":
    symbol = 'BTC-USDT'
    timeframe = '1hour'
    start_time = datetime(2019, 1, 1, tzinfo=timezone.utc)
    end_time = datetime.utcnow().replace(tzinfo=timezone.utc)
    df = fetch_kucoin_ohlcv(symbol, timeframe, start_time, end_time)
    print(df.head())
    print(df.tail())
    df.to_pickle('kucoin_btc_usdt_1hour.pkl')