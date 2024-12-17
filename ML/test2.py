import pandas as pd
import numpy as np
import vectorbt as vbt
import ta

class CryptoTradingSignals:
    def __init__(self, ema_periods=[10, 20, 30, 50, 100, 200], rsi_period=14, tp_percentage=0.05):
        """
        Initialize the trading strategy with dynamic trailing stop
        
        :param ema_periods: List of periods for Exponential Moving Averages
        :param rsi_period: Period for Relative Strength Index
        :param tp_percentage: Base take-profit percentage
        """
        self.ema_periods = ema_periods
        self.rsi_period = rsi_period
        self.tp_percentage = tp_percentage
    
    def calculate_dynamic_trailing_stop(self, df, position_type):
        """
        Calculate dynamic trailing stop based on EMA positions
        
        :param df: Input price dataframe with EMAs
        :param position_type: 'long' or 'short'
        :return: Boolean mask for trailing stop conditions
        """
        # Prepare EMA columns
        ema_cols = [f'EMA_{period}' for period in self.ema_periods]
        
        # Dynamic trailing stop logic
        if position_type == 'long':
            # Find the first two EMAs crossing
            trailing_stop_condition = (
                (df[ema_cols[0]] <= df[ema_cols[1]]) |  # First two EMAs cross
                (df['RSI'] < 50)  # Additional RSI confirmation
            )
        else:  # short
            trailing_stop_condition = (
                (df[ema_cols[0]] >= df[ema_cols[1]]) |  # First two EMAs cross
                (df['RSI'] > 50)  # Additional RSI confirmation
            )
        
        return trailing_stop_condition
    
    def generate_signals(self, df):
        """
        Generate entry and exit signals with dynamic take-profit
        
        :param df: Input price dataframe 
        :return: Tuple of entry and exit signals
        """
        # Calculate EMAs
        ema_cols = []
        for period in self.ema_periods:
            ema_col = f'EMA_{period}'
            df[ema_col] = ta.trend.ema_indicator(df['close'], window=period)
            ema_cols.append(ema_col)
        
        # Calculate RSI
        df['RSI'] = ta.momentum.rsi(df['close'], window=self.rsi_period)
        
        # Long entry conditions
        long_entries = (
            (df[f'EMA_{self.ema_periods[0]}'] > df[f'EMA_{self.ema_periods[1]}']) &
            (df[f'EMA_{self.ema_periods[1]}'] > df[f'EMA_{self.ema_periods[2]}']) &
            (df[f'EMA_{self.ema_periods[2]}'] > df[f'EMA_{self.ema_periods[3]}']) &
            (df['RSI'] > 70)
        )
        
        # Long exit conditions (dynamic trailing stop)
        long_exits = self.calculate_dynamic_trailing_stop(df, 'long')
        
        # Short entry conditions
        short_entries = (
            (df[f'EMA_{self.ema_periods[0]}'] < df[f'EMA_{self.ema_periods[1]}']) &
            (df[f'EMA_{self.ema_periods[1]}'] < df[f'EMA_{self.ema_periods[2]}']) &
            (df[f'EMA_{self.ema_periods[2]}'] < df[f'EMA_{self.ema_periods[3]}']) &
            (df['RSI'] < 30)
        )
        
        # Short exit conditions (dynamic trailing stop)
        short_exits = self.calculate_dynamic_trailing_stop(df, 'short')
        
        return long_entries, long_exits, short_entries, short_exits

# Example usage function
def backtest_signals(df, init_cash=10000, fee=0.001,initial_stop=0.05):
    """
    Backtest trading signals using Vectorbt
    
    :param df: Input price dataframe
    :param init_cash: Initial portfolio cash
    :param fee: Trading fee percentage
    :return: Vectorbt portfolio
    """
    # Initialize signal generator
    signal_generator = CryptoTradingSignals()
    
    # Generate signals
    long_entries, long_exits, short_entries, short_exits = signal_generator.generate_signals(df)
    
    # Create Vectorbt Portfolio with stop-loss and take-profit
    portfolio = vbt.Portfolio.from_signals(
        df['close'],
        long_entries,
        long_exits,
        short_entries=short_entries,
        short_exits=short_exits,
        init_cash=init_cash,
        fees=fee,
        sl_stop=initial_stop,  # 5% stop loss
        # tp_stop=0.05   # 5% take profit
    )
    
    return portfolio

# Commented example for data preparation

# Example data preparation
import ccxt

# Initialize exchange
exchange = ccxt.binance()

# Fetch historical data
ohlcv = exchange.fetch_ohlcv('BTC/USDT', '1h')
df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
df.set_index('timestamp', inplace=True)

# Backtest
portfolio = backtest_signals(df)
print(portfolio.total_return())
print(portfolio.stats())
