import gymnasium as gym
import numpy as np
import pandas as pd
import torch
from gymnasium import spaces
from stable_baselines3 import PPO
from stable_baselines3.common.monitor import Monitor
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer

class TrendTradingEnv(gym.Env):
    def __init__(self, df, time_window=1):
        super(TrendTradingEnv, self).__init__()
        
        # Preprocess the dataframe
        self.df = df.copy()
        self.df.dropna(inplace=True)
        self.df.reset_index(drop=True, inplace=True)
        
        self.time_window = time_window
        self.current_step = 0
        
        # Prepare features
        self.prepare_data()
        
        # Define action and observation spaces
        self.action_space = spaces.Discrete(3)  # 0: Buy, 1: Sell, 2: Hold
        self.observation_space = spaces.Box(
            low=-np.inf,
            high=np.inf,
            shape=(self.normalized_features.shape[1],),
            dtype=np.float32
        )

    def prepare_data(self):
        # Select features for observation
        feature_columns = [
            'volume','RSI',
            'EMA_50', 'EMA_100', 'EMA_150', 
            'EMA_500', 'EMA_750', 'EMA_1000'
        ]
        
        # Extract features
        features = self.df[feature_columns].copy()
        
        # Handle potential NaN values
        imputer = SimpleImputer(strategy='median')
        features_imputed = imputer.fit_transform(features)
        
        # Normalize features
        self.scaler = StandardScaler()
        self.normalized_features = self.scaler.fit_transform(features_imputed)
        
        # Convert to DataFrame for easier indexing
        self.normalized_features = pd.DataFrame(
            self.normalized_features, 
            columns=feature_columns
        )

    def determine_trend(self, idx):
        row = self.df.loc[idx]
        emas = [
            row['EMA_50'], row['EMA_100'], row['EMA_150'],
            row['EMA_500'], row['EMA_750'], row['EMA_1000']
        ]
        if all(emas[i] > emas[i+1] for i in range(len(emas)-1)):
            return 'Uptrend'
        elif all(emas[i] < emas[i+1] for i in range(len(emas)-1)):
            return 'Downtrend'
        else:
            return 'Sideways'

    def step(self, action):
        trend = self.determine_trend(self.current_step)

        price_at_prediction = self.df.loc[self.current_step, 'Close']
        timestamp = self.df.loc[self.current_step, 'time']

        # Predict based on action
        if action == 0:
            prediction = 'Increase'
        elif action == 1:
            prediction = 'Decrease'
        else:
            prediction = 'No significant movement'


        # Calculate reward
        reward = 0.0

        # Wait for time_window periods to evaluate
        if self.current_step + self.time_window < len(self.df):
            future_price = self.df.loc[self.current_step + self.time_window, 'Close']

            if trend == 'Uptrend':
                reward = 1.0 if prediction == 'Increase' else -1.0
            elif trend == 'Downtrend' :
                reward = 1.0 if prediction == 'Decrease' else -1.0
            elif trend == 'Sideways':
                reward = 1.0 if prediction == 'No significant movement' else -1.0
            else:
                reward = -1.0
        
        # Increment step and check termination
        self.current_step += 1
        terminated = self.current_step >= len(self.df) - self.time_window
        truncated = False

        # Get next observation
        obs = self._get_observation()
        
        # Prepare info dictionary
        info = {
            'timestamp': timestamp,
            'trend': trend,
            'prediction': prediction,
            'price_at_prediction': price_at_prediction
        }

        return obs, reward, terminated, truncated, info




    def reset(self, seed=None, options=None):
        super().reset(seed=seed)
        self.current_step = 0
        obs = self._get_observation()
        return obs, {}

    def _get_observation(self):
        obs = self.normalized_features.loc[self.current_step].values.astype(np.float32)
        if np.isnan(obs).any():
            obs = np.nan_to_num(obs, nan=0.0)
        return obs


def train_and_evaluate(df):
    # Create environment without DummyVecEnv
    env = TrendTradingEnv(df, time_window=1)
    env = Monitor(env)
    
    # Initialize and train the agent
    model = PPO('MlpPolicy', env, verbose=1)
    model.learn(total_timesteps=400000)
    
    # Evaluate the agent
    obs, _ = env.reset()
    total_rewards = []
    collected_info = []
    
    max_steps = len(df) - env.env.time_window
    for _ in range(max_steps):
        action, _ = model.predict(obs)
        obs, reward, terminated, truncated, info = env.step(action)
        
        total_rewards.append(reward)
        collected_info.append(info)
        
        if terminated or truncated:
            break

    # Calculate performance metrics
    positive_rewards = sum(1 for r in total_rewards if r > 0)
    accuracy = positive_rewards / len(total_rewards) if total_rewards else 0
    print(f"Overall Accuracy: {accuracy:.2%}")
    print(f"Total steps: {len(total_rewards)}")
    
    # Optional: analyze collected info
    if collected_info:
        trends = [item['trend'] for item in collected_info]
        predictions = [item['prediction'] for item in collected_info]
        print("Trend distribution:\n", pd.Series(trends).value_counts())
        print("Prediction distribution:\n", pd.Series(predictions).value_counts())

# Load data
df = pd.read_pickle('/root/trading_systems/kucoin_btc_usdt_1hour_TA.pkl')

# Run training and evaluation
train_and_evaluate(df)