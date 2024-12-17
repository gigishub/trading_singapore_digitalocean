import gymnasium as gym
import numpy as np
import pandas as pd
import pandas_ta as ta
import matplotlib.pyplot as plt
import yfinance as yf
import time

from gymnasium import spaces
from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv
from sklearn.preprocessing import StandardScaler

import requests
from datetime import datetime, timedelta

class CryptoTradingEnv(gym.Env):
    def __init__(self, df, initial_balance=10000, max_steps=None):
        super().__init__()
        
        # Data preparation
        self.df = df.copy()
        self.prepare_features()
        
        # Trading parameters
        self.initial_balance = initial_balance
        self.max_steps = max_steps or len(self.df)
        
        # Trading state tracking
        self.current_step = 0
        self.current_balance = initial_balance
        self.positions_held = 0
        self.entry_price = 0
        
        # Performance tracking
        self.trades = []
        
        # Define action and observation spaces
        self.action_space = spaces.Discrete(3)  # Buy, Sell, Hold
        self.observation_space = spaces.Box(
            low=-np.inf, 
            high=np.inf, 
            shape=(len(self.features.columns),), 
            dtype=np.float32
        )
        
    def prepare_features(self):
        # Calculate technical indicators
        # Add EMA indicators
        print("Available columns before feature preparation:")
        print(self.df.columns)
        self.df.ta.ema(length=50, append=True)
        self.df.ta.ema(length=100, append=True)
        
        # Add RSI
        self.df.ta.rsi(length=14, append=True)
        
        # Add ATR
        self.df.ta.atr(length=14, append=True)
        
        # Drop NaN rows
        self.df.dropna(inplace=True)
        
        # Print columns after adding indicators
        print("Available columns after adding indicators:")
        print(self.df.columns)
        
        # Rename columns to match expected names
        column_mapping = {
            'EMA_50': 'EMA_50',
            'EMA_100': 'EMA_100',
            'RSI_14': 'RSI_14',
            'ATRr_14': 'ATR_14'  # Rename 'ATRr_14' to 'ATR_14'
        }
        self.df.rename(columns=column_mapping, inplace=True)
        
        # Ensure all required columns exist
        required_columns = ['Close', 'EMA_50', 'EMA_100', 'RSI_14', 'ATR_14']
        
        # Normalize features
        self.scaler = StandardScaler()
        
        # Select and prepare features
        feature_df = self.df[required_columns]
        
        self.features = pd.DataFrame(
            self.scaler.fit_transform(feature_df),
            columns=required_columns,
            index=feature_df.index
        )
        
    def reset(self, seed=None):
        super().reset(seed=seed)
        
        # Reset trading state
        self.current_step = 0
        self.current_balance = self.initial_balance
        self.positions_held = 0
        self.entry_price = 0
        self.trades = []
        
        return self._get_observation(), {}
    
    def step(self, action):
        # Increment step
        self.current_step += 1
        
        # Current price
        current_price = self.df['Close'].iloc[self.current_step]
        atr = self.df['ATR_14'].iloc[self.current_step]
        
        # Trading logic
        reward = 0
        done = self.current_step >= self.max_steps - 1
        
        if action == 0:  # Buy
            if self.positions_held == 0:
                # Calculate position size (simplified)
                position_size = self.current_balance * 0.9 / current_price
                self.positions_held = position_size
                self.entry_price = current_price
        
        elif action == 1:  # Sell
            if self.positions_held > 0:
                # Calculate profit/loss
                sell_price = current_price
                profit = (sell_price - self.entry_price) * self.positions_held
                
                # Update balance
                self.current_balance += profit
                
                # Record trade
                self.trades.append({
                    'entry_price': self.entry_price,
                    'exit_price': sell_price,
                    'profit': profit
                })
                
                # Reset position
                self.positions_held = 0
                self.entry_price = 0
        
        # Stop Loss and Take Profit
        if self.positions_held > 0:
            # Stop Loss: 1 ATR below entry
            stop_loss = self.entry_price - atr
            # Take Profit: 1 ATR above entry
            take_profit = self.entry_price + atr
            
            if current_price <= stop_loss or current_price >= take_profit:
                sell_price = current_price
                profit = (sell_price - self.entry_price) * self.positions_held
                
                self.current_balance += profit
                
                self.trades.append({
                    'entry_price': self.entry_price,
                    'exit_price': sell_price,
                    'profit': profit
                })
                
                self.positions_held = 0
                self.entry_price = 0
        
        # Calculate reward based on balance change
        reward = self.current_balance - self.initial_balance
        
        return (
            self._get_observation(), 
            reward, 
            done, 
            False, 
            {}
        )
    
    def _get_observation(self):
        return self.features.iloc[self.current_step].values
    
    def calculate_performance(self):
        # Calculate win/loss ratio and total trades
        if not self.trades:
            return 0, 0
        
        profitable_trades = [trade for trade in self.trades if trade['profit'] > 0]
        win_loss_ratio = len(profitable_trades) / len(self.trades)
        
        return win_loss_ratio, len(self.trades)


def fetch_kucoin_ohlcv(symbol='XRP-USDT', timeframe='4hour', start_time=None, end_time=None):
    """
    Fetch OHLCV data from KuCoin API with pagination to overcome 1500 candle limit
    
    Parameters:
    - symbol: Trading pair (default XRP-USDT)
    - timeframe: Candle timeframe (1min, 3min, 5min, 15min, 30min, 1hour, 2hour, 4hour, 6hour, 8hour, 12hour, 1day, 1week)
    - start_time: Start time (optional, defaults to 80 days ago)
    - end_time: End time (optional, defaults to current time)
    
    Returns:
    - DataFrame with OHLCV data for entire requested period
    """
    base_url = "https://api.kucoin.com"
    endpoint = "/api/v1/market/candles"
    
    # Set default time range if not provided
    if start_time is None:
        start_time = datetime.now() - timedelta(days=30)
    
    if end_time is None:
        end_time = datetime.now()
    
    # Ensure start_time is before end_time
    if start_time >= end_time:
        raise ValueError("Start time must be before end time")
    
    # List to collect all candle data
    all_candles = []
    
    # Current pagination window
    current_start = start_time
    print(f"Fetching data from {current_start} to {end_time}")
    while current_start < end_time:
        try:
            # Prepare request parameters
            params = {
                'symbol': symbol,
                'type': timeframe,
                'startAt': int(current_start.timestamp()),
                'endAt': int(end_time.timestamp()),
                'limit': 1500  # Maximum allowed by KuCoin
            }
            
            # Make API request
            response = requests.get(base_url + endpoint, params=params)
            response.raise_for_status()
            
            # Parse response
            data = response.json()
            
            if data.get('code') != '200000':
                raise ValueError(f"API Error: {data}")
            
            # Process candles
            candles = data.get('data', [])
            
            # Break if no more data
            if not candles:
                break
            
            # Add to collection
            all_candles.extend(candles)
            
            # Update start time to one second after the latest timestamp in current batch
            latest_timestamp = pd.to_datetime(int(candles[-1][0]), unit='s')
            current_start = latest_timestamp + timedelta(seconds=1)
            time.sleep(0.001)  # Be nice to the API
            print(f"Fetched {len(candles)} candles up to {latest_timestamp}")
            
        except requests.RequestException as e:
            print(f"Request Error: {e}")
            break
        except Exception as e:
            print(f"Unexpected Error: {e}")
            break
    
    # Create DataFrame if we have data
    if not all_candles:
        print("No data fetched.")
        return None
    
    # Create DataFrame with expected structure
    df = pd.DataFrame(all_candles, columns=[
        'Timestamp', 'Open', 'Close', 'High', 'Low', 'Volume', 'Turnover'
    ])
    
    # Convert timestamp and price columns to numeric
    df['Timestamp'] = pd.to_datetime(df['Timestamp'].astype(int), unit='s')
    df[['Open', 'Close', 'High', 'Low', 'Volume', 'Turnover']] = df[['Open', 'Close', 'High', 'Low', 'Volume', 'Turnover']].astype(float)
    
    # Set timestamp as index
    df.set_index('Timestamp', inplace=True)
    
    # Sort index in ascending order
    df.sort_index(inplace=True)
    
    # Remove duplicates (if any)
    df = df[~df.index.duplicated(keep='first')]
    
    print(f"Fetched {len(df)} candles for {symbol}")
    print("\nDataFrame Preview:")
    print(df.head())
    print("\nData Range:")
    print(f"Start: {df.index.min()}")
    print(f"End: {df.index.max()}")
    
    return df

def fetch_kucoin_ohlcv2(symbol='XRP-USDT', timeframe='4hour', start_time=None, end_time=None):
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
        start_time = int((datetime.now() - timedelta(days=80)).timestamp() * 1000)
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



def train_agent(env, total_timesteps=50000):
    # Wrap the environment
    vec_env = DummyVecEnv([lambda: env])
    
    # Initialize and train PPO agent
    model = PPO('MlpPolicy', vec_env, verbose=1)
    model.learn(total_timesteps=total_timesteps)
    
    return model

def evaluate_agent(model, env, episodes=10):
    all_rewards = []
    
    for _ in range(episodes):
        obs, _ = env.reset()
        done = False
        total_reward = 0
        
        while not done:
            action, _ = model.predict(obs)
            obs, reward, done, _, _ = env.step(action)
            total_reward += reward
        
        all_rewards.append(total_reward)
        
        # Performance metrics
        win_ratio, total_trades = env.calculate_performance()
        print(f"Episode Reward: {total_reward}")
        print(f"Win/Loss Ratio: {win_ratio}")
        print(f"Total Trades: {total_trades}")
    
    return all_rewards

def plot_rewards(rewards):
    plt.figure(figsize=(10, 5))
    plt.plot(rewards)
    plt.title('Rewards per Episode')
    plt.xlabel('Episode')
    plt.ylabel('Total Reward')
    plt.show()

def main():
    # Fetch XRP data
    df = fetch_kucoin_ohlcv()
    # df = fetch_kucoin_ohlcv(symbol='XRP-USDT', timeframe='1min', start_time=, end_time=None)

    
    if df is None:
        print("Failed to fetch data. Exiting.")
        return
    
    # Create environment
    env = CryptoTradingEnv(df)
    
    # Train agent
    model = train_agent(env)
    
    # Evaluate agent
    rewards = evaluate_agent(model, env)
    
    # Plot rewards
    plot_rewards(rewards)

if __name__ == "__main__":
    main()