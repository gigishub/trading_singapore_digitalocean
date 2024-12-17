import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class TrendAnalyzer:
    def __init__(self, price_data):
        """
        Initialize TrendAnalyzer with price data
        
        :param price_data: pandas DataFrame with 'Close' prices
        """
        # Ensure 'Close' column exists
        if 'Close' not in price_data.columns:
            raise ValueError("Input DataFrame must have a 'Close' column")
        
        # Create a copy of the dataframe to avoid modifying the original
        self.price_data = price_data.copy()
        
        # Ensure Close column is numeric
        self.price_data['Close'] = pd.to_numeric(self.price_data['Close'], errors='coerce')
        
        # Drop any NaN values
        self.price_data.dropna(subset=['Close'], inplace=True)
    
    def determine_trend(self, 
                        slow_ema_period=50, 
                        mid_ema_period=20, 
                        fast_ema_period=10,
                        short_slow_ema_period=5, 
                        short_mid_ema_period=3, 
                        short_fast_ema_period=2):
        """
        Determine market trend using multiple EMA periods
        
        :return: Trend classification (Uptrend/Downtrend/Sideways)
        """
        try:
            # Compute EMAs using pandas built-in ewm method
            ema_slow = self.price_data['Close'].ewm(span=slow_ema_period, adjust=False).mean()
            ema_mid = self.price_data['Close'].ewm(span=mid_ema_period, adjust=False).mean()
            ema_fast = self.price_data['Close'].ewm(span=fast_ema_period, adjust=False).mean()
            
            short_ema_slow = self.price_data['Close'].ewm(span=short_slow_ema_period, adjust=False).mean()
            short_ema_mid = self.price_data['Close'].ewm(span=short_mid_ema_period, adjust=False).mean()
            short_ema_fast = self.price_data['Close'].ewm(span=short_fast_ema_period, adjust=False).mean()
            
            # Last values for trend determination
            last_close = self.price_data['Close'].iloc[-1]
            
            # Trend Logic with more nuanced classification
            uptrend_condition = (
                (short_ema_fast.iloc[-1] > short_ema_mid.iloc[-1] > short_ema_slow.iloc[-1]) and
                (ema_fast.iloc[-1] > ema_mid.iloc[-1] > ema_slow.iloc[-1])
            )
            
            downtrend_condition = (
                (short_ema_fast.iloc[-1] < short_ema_mid.iloc[-1] < short_ema_slow.iloc[-1]) and
                (ema_fast.iloc[-1] < ema_mid.iloc[-1] < ema_slow.iloc[-1])
            )
            
            if uptrend_condition:
                return 'Uptrend'
            elif downtrend_condition:
                return 'Downtrend'
            else:
                return 'Sideways'
        
        except Exception as e:
            print(f"Error in trend determination: {e}")
            return 'Unable to determine'
    
    def predict_trend_accuracy(self, 
                                time_windows=[4, 8, 12], 
                                trend_params=None):
        """
        Analyze trend prediction accuracy across multiple time windows
        
        :param time_windows: List of periods to check trend prediction
        :param trend_params: Dictionary of trend determination parameters
        :return: Detailed accuracy metrics
        """
        if trend_params is None:
            trend_params = {
                'slow_ema_period': 50,
                'mid_ema_period': 20,
                'fast_ema_period': 10,
                'short_slow_ema_period': 5,
                'short_mid_ema_period': 3,
                'short_fast_ema_period': 2
            }
        
        results = {
            'window_accuracies': {},
            'total_predictions': 0,
            'correct_predictions': 0
        }
        
        # Compute predictions for different time windows
        for window in time_windows:
            window_correct = 0
            window_total = 0
            
            # Ensure we have enough data
            if len(self.price_data) <= window:
                print(f"Not enough data for window {window}")
                continue
            
            for i in range(window, len(self.price_data)):
                try:
                    # Set up price slice for trend determination
                    price_slice = self.price_data.iloc[:i].copy()
                    trend_analyzer = TrendAnalyzer(price_slice)
                    
                    # Determine trend
                    predicted_trend = trend_analyzer.determine_trend(**trend_params)
                    
                    # Check prediction accuracy
                    price_at_prediction = self.price_data.iloc[i - window]['Close']
                    current_price = self.price_data.iloc[i]['Close']
                    
                    # Price change percentage
                    price_change_pct = ((current_price - price_at_prediction) / price_at_prediction) * 100
                    
                    # Prediction success criteria
                    success = (
                        (predicted_trend == 'Uptrend' and price_change_pct > 0) or
                        (predicted_trend == 'Downtrend' and price_change_pct < 0) or
                        (predicted_trend == 'Sideways' and abs(price_change_pct) < 1)
                    )
                    
                    if success:
                        window_correct += 1
                    window_total += 1
                
                except Exception as e:
                    print(f"Error in prediction for window {window}: {e}")
                    continue
            
            # Calculate accuracy for this window
            accuracy = window_correct / window_total if window_total > 0 else 0
            results['window_accuracies'][window] = {
                'correct': window_correct,
                'total': window_total,
                'accuracy': accuracy
            }
            
            results['total_predictions'] += window_total
            results['correct_predictions'] += window_correct
        
        # Overall accuracy
        results['overall_accuracy'] = (
            results['correct_predictions'] / results['total_predictions'] 
            if results['total_predictions'] > 0 else 0
        )
        
        return results
    
    def visualize_trend_analysis(self, results):
        """
        Create visualization of trend prediction accuracies
        
        :param results: Results from predict_trend_accuracy method
        """
        plt.figure(figsize=(12, 6))
        
        # Bar plot of accuracies
        windows = list(results['window_accuracies'].keys())
        accuracies = [results['window_accuracies'][w]['accuracy'] for w in windows]
        
        plt.bar(windows, accuracies)
        plt.title('Trend Prediction Accuracy across Different Time Windows')
        plt.xlabel('Time Window (Periods)')
        plt.ylabel('Accuracy')
        plt.ylim(0, 1)
        
        # Add accuracy percentages on top of bars
        for i, acc in enumerate(accuracies):
            plt.text(windows[i], acc, f'{acc*100:.2f}%', 
                     ha='center', va='bottom')
        
        plt.tight_layout()

        plt.show()










import pandas as pd
import os

def diagnose_pickle_file(file_path):
    """
    Comprehensive diagnostic for pickle file
    """
    print("Pickle File Diagnostic Report")
    print("-" * 40)
    
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            print(f"Error: File not found at {file_path}")
            return None
        
        # Check file size
        file_size = os.path.getsize(file_path)
        print(f"File Size: {file_size / (1024*1024):.2f} MB")
        
        # Try to read the pickle file
        try:
            df = pd.read_pickle(file_path)
            
            # Diagnostic information
            print("\nDataFrame Characteristics:")
            print(f"Shape: {df.shape}")
            print("\nColumns:")
            for col in df.columns:
                print(f"- {col}: {df[col].dtype}")
            
            print("\nFirst few rows:")
            print(df.head())
            
            print("\nBasic Statistics:")
            print(df.describe())
            
            return df
        
        except Exception as e:
            print(f"Error reading pickle file: {e}")
            return None
    
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None


def main():
    try:
        # Use pandas read methods instead of read_pickle if the file path is specific
        # If it's a pickle file, use pd.read_pickle
        # If it's a CSV, use pd.read_csv, etc.
        # price_data = pd.read_pickle('/root/trading_systems/XRP-USDT_1min_20240101.pkl')
        
        # Update this with your actual pickle file path
        file_path = '/root/trading_systems/kucoin_btc_usdt_1hour_TA.pkl'
        
        # Diagnose the pickle file
        df = diagnose_pickle_file(file_path)
        
        # Ensure necessary columns exist
        required_columns = ['Close']
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"DataFrame must contain columns: {required_columns}")
        
        # Initialize TrendAnalyzer
        trend_analyzer = TrendAnalyzer(df)
        
        # Determine trend for the entire dataset
        overall_trend = trend_analyzer.determine_trend()
        print(f"Overall Market Trend: {overall_trend}")
        
        # Predict trend accuracy
        accuracy_results = trend_analyzer.predict_trend_accuracy()
        
        # Print detailed results
        print("\nTrend Prediction Accuracy:")
        for window, metrics in accuracy_results['window_accuracies'].items():
            print(f"Window {window} periods:")
            print(f"  Correct Predictions: {metrics['correct']}/{metrics['total']}")
            print(f"  Accuracy: {metrics['accuracy']*100:.2f}%")
        
        print(f"\nOverall Accuracy: {accuracy_results['overall_accuracy']*100:.2f}%")
        
        # Visualize results
        trend_analyzer.visualize_trend_analysis(accuracy_results)
    
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()