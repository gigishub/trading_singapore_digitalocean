import pandas as pd
import numpy as np
import vectorbt as vbt

# Function to calculate Simple Moving Averages
def sma(close, n):
    return close.rolling(n).mean()

# Read the pickle file
try:
    df = pd.read_pickle('/root/trading_systems/kucoin_btc_usdt_1hour_TA.pkl')
    
    # Standardize column names (case-insensitive)
    df.rename(columns={
        col: col.capitalize() 
        for col in ['time', 'open', 'close', 'high', 'low', 'volume', 'turnover']
    }, inplace=True)

    # Convert 'Time' column to datetime and set as index
    df['Time'] = pd.to_datetime(df['Time'])
    df.set_index('Time', inplace=True)
    
    # Ensure 'Close' is a Series
    close_series = df['Close']



    # Define entry and exit signals
    # Define entry and exit signals
    entries = (df['EMA_50'] > df['EMA_100']) & (df['EMA_100'] > df['EMA_150']) & (df['EMA_150'] > df['EMA_500']) & (
        df['EMA_500'] > df['EMA_750']) & (df['EMA_750'] > df['EMA_1000']) & (df['RSI'] > 70) 
    exits = (df['EMA_150'] < df['EMA_500']) 

    # Run the backtest
    portfolio = vbt.Portfolio.from_signals(df['Close'], entries, exits, init_cash=10000, fees=0.002)
    stats = portfolio.stats()
    print(stats)
    
    # Plot results
    portfolio.plot().show()
    
except FileNotFoundError:
    print("Error: Pickle file not found. Please check the file path.")
except Exception as e:
    print(f"An error occurred: {e}")