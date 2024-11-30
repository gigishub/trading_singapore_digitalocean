import pandas as pd
from typing import List, Dict
from datetime import datetime, timedelta
import json

def analyze_trade_performance(trades: List[Dict]) -> Dict:
    """
    Analyze trade performance from a list of trade dictionaries.
    
    Args:
        trades (List[Dict]): List of trade dictionaries
    
    Returns:
        Dict: Comprehensive trade performance analysis
    """
    # Convert trades to DataFrame for easier manipulation
    df = pd.DataFrame(trades)
    
    # Group trades by symbol
    performance_by_symbol = {}
    
    for symbol in df['symbol'].unique():
        # Filter trades for this symbol
        symbol_trades = df[df['symbol'] == symbol].sort_values('createdAt')
        
        # Track open positions
        positions = []
        trade_log = []
        
        for _, trade in symbol_trades.iterrows():
            # Convert timestamp to datetime
            trade_time = datetime.fromtimestamp(trade['createdAt'] / 1000)
            
            if trade['side'] == 'buy':
                # Open or add to position
                positions.append({
                    'size': float(trade['size']),
                    'price': float(trade['price']),
                    'entry_time': trade_time
                })
            
            elif trade['side'] == 'sell':
                # Close positions
                sell_size = float(trade['size'])
                sell_price = float(trade['price'])
                
                while sell_size > 0 and positions:
                    current_position = positions[0]
                    
                    # Determine how much of the position to close
                    close_size = min(current_position['size'], sell_size)
                    
                    # Calculate profit/loss for this portion
                    profit = (sell_price - current_position['price']) * close_size
                    
                    # Log the trade
                    trade_log.append({
                        'symbol': symbol,
                        'entry_price': current_position['price'],
                        'exit_price': sell_price,
                        'size': close_size,
                        'profit': profit,
                        'entry_time': current_position['entry_time'],
                        'exit_time': trade_time
                    })
                    
                    # Update positions and sell size
                    current_position['size'] -= close_size
                    sell_size -= close_size
                    
                    # Remove fully closed positions
                    if current_position['size'] == 0:
                        positions.pop(0)
        
        # Analyze trade log
        if trade_log:
            trade_df = pd.DataFrame(trade_log)
            performance = {
                'total_trades': len(trade_log),
                'total_profit': trade_df['profit'].sum(),
                'average_profit_per_trade': trade_df['profit'].mean(),
                'winning_trades': (trade_df['profit'] > 0).sum(),
                'losing_trades': (trade_df['profit'] <= 0).sum(),
                'win_rate': (trade_df['profit'] > 0).mean(),
                'total_volume': trade_df['size'].sum(),
                'trade_details': trade_log
            }
        else:
            performance = {
                'total_trades': 0,
                'total_profit': 0,
                'average_profit_per_trade': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0,
                'total_volume': 0,
                'trade_details': []
            }
        
        performance_by_symbol[symbol] = performance
    
    return performance_by_symbol




# Example usage
def main():
    trades = [

    {
        "symbol": "XRP-USDT",
        "tradeId": "10925524728039425",
        "orderId": "674891dfb6d508000796a3b5",
        "counterOrderId": "674891dfb26bfb0007c19e61",
        "side": "buy",
        "liquidity": "taker",
        "forceTaker": "false",
        "price": "1.43634",
        "size": "11",
        "funds": "15.79974",
        "fee": "0.01579974",
        "feeRate": "0.001",
        "feeCurrency": "USDT",
        "stop": "",
        "tradeType": "TRADE",
        "type": "limit",
        "createdAt": 1732809183094
    },
    {
        "symbol": "XRP-USDT",
        "tradeId": "10925218880833543",
        "orderId": "67488e1f5a55500007d9574d",
        "counterOrderId": "67488e1f754b1e000782dccc",
        "side": "buy",
        "liquidity": "taker",
        "forceTaker": "false",
        "price": "1.44249",
        "size": "5.0022",
        "funds": "7.215623478",
        "fee": "0.007215623478",
        "feeRate": "0.001",
        "feeCurrency": "USDT",
        "stop": "",
        "tradeType": "TRADE",
        "type": "limit",
        "createdAt": 1732808223812
    },
    {
        "symbol": "XRP-USDT",
        "tradeId": "10925218880833540",
        "orderId": "67488e1f5a55500007d9574d",
        "counterOrderId": "67488dcfb38d12000743e6b9",
        "side": "buy",
        "liquidity": "taker",
        "forceTaker": "false",
        "price": "1.4424",
        "size": "3.2758",
        "funds": "4.72501392",
        "fee": "0.00472501392",
        "feeRate": "0.001",
        "feeCurrency": "USDT",
        "stop": "",
        "tradeType": "TRADE",
        "type": "limit",
        "createdAt": 1732808223812
    },
    {
        "symbol": "XRP-USDT",
        "tradeId": "10925218880833537",
        "orderId": "67488e1f5a55500007d9574d",
        "counterOrderId": "67488dcfb38d12000743e6a5",
        "side": "buy",
        "liquidity": "taker",
        "forceTaker": "false",
        "price": "1.4422",
        "size": "2.722",
        "funds": "3.9256684",
        "fee": "0.0039256684",
        "feeRate": "0.001",
        "feeCurrency": "USDT",
        "stop": "",
        "tradeType": "TRADE",
        "type": "limit",
        "createdAt": 1732808223812
    },
    {
        "symbol": "XRP-USDT",
        "tradeId": "10925048907450543",
        "orderId": "67488c0b754b1e0007736ac4",
        "counterOrderId": "67488c25f4c90600075edcc5",
        "side": "sell",
        "liquidity": "maker",
        "forceTaker": "false",
        "price": "1.45",
        "size": "11",
        "funds": "15.95",
        "fee": "0.01595",
        "feeRate": "0.001",
        "feeCurrency": "USDT",
        "stop": "",
        "tradeType": "TRADE",
        "type": "limit",
        "createdAt": 1732807718011
    },
    {
        "symbol": "XRP-USDT",
        "tradeId": "10925014908815361",
        "orderId": "67488bcc5e5d4c0007dd74f3",
        "counterOrderId": "67488bccb26bfb000795a38e",
        "side": "buy",
        "liquidity": "taker",
        "forceTaker": 'false',
        "price": "1.44334",
        "size": "11",
        "funds": "15.87674",
        "fee": "0.01587674",
        "feeRate": "0.001",
        "feeCurrency": "USDT",
        "stop": "",
        "tradeType": "TRADE",
        "type": "limit",
        "createdAt": 1732807628763
    },
    {
        "symbol": "XRP-USDT",
        "tradeId": "10925014906980353",
        "orderId": "67488bcc8b053b0007a3ca94",
        "counterOrderId": "67488bcc754b1e0007717eb9",
        "side": "sell",
        "liquidity": "taker",
        "price": "1.44291",
        "size": "11",
        "funds": "15.87201",
        "fee": "0.01587201",
        "feeRate": "0.001",
        "feeCurrency": "USDT",
        "stop": "",
        "tradeType": "TRADE",
        "type": "limit",
        "createdAt": 1732807628753
    },
    {
        "symbol": "XRP-USDT",
        "tradeId": "10924970427566081",
        "orderId": "67488b66b6d508000767135e",
        "counterOrderId": "67488b66b6d5080007671355",
        "side": "buy",
        "liquidity": "taker",
        "price": "1.44663",
        "size": "11",
        "funds": "15.91293",
        "fee": "0.01591293",
        "feeRate": "0.001",
        "feeCurrency": "USDT",
        "stop": "",
        "tradeType": "TRADE",
        "type": "limit",
        "createdAt": 1732807526236
    },
    {
        "symbol": "MGT-USDT",
        "tradeId": "5774725952059408",
        "orderId": "67485b5b725fc600077b2948",
        "counterOrderId": "67485b5ef61fa00007ddcb55",
        "side": "sell",
        "liquidity": "maker",
        "price": "0.07387",
        "size": "55",
        "funds": "4.06285",
        "fee": "0.01218855",
        "feeRate": "0.003",
        "feeCurrency": "USDT",
        "stop": "",
        "tradeType": "TRADE",
        "type": "limit",
        "createdAt": 1732795230827
    },
    {
        "symbol": "MGT-USDT",
        "tradeId": "5774725952059405",
        "orderId": "67485b5b1adbea000723f5cf",
        "counterOrderId": "67485b5ef61fa00007ddcb55",
        "side": "sell",
        "liquidity": "maker",
        "price": "0.07387",
        "size": "55",
        "funds": "4.06285",
        "fee": "0.01218855",
        "feeRate": "0.003",
        "feeCurrency": "USDT",
        "stop": "",
        "tradeType": "TRADE",
        "type": "limit",
        "createdAt": 1732795230827
    },
    {
        "symbol": "MGT-USDT",
        "tradeId": "5774712897026049",
        "orderId": "67485b40754b1e00072ab8e3",
        "counterOrderId": "67483de7b26bfb000793cba9",
        "side": "buy",
        "liquidity": "taker",
        "price": "0.07053",
        "size": "55",
        "funds": "3.87915",
        "fee": "0.01163745",
        "feeRate": "0.003",
        "feeCurrency": "USDT",
        "stop": "",
        "tradeType": "TRADE",
        "type": "limit",
        "createdAt": 1732795200436
    },
    {
        "symbol": "MGT-USDT",
        "tradeId": "5774712892700673",
        "orderId": "67485b40dcdd7b00078a369d",
        "counterOrderId": "67483de7b26bfb000793cba9",
        "side": "buy",
        "liquidity": "taker",
        "price": "0.07053",
        "size": "55",
        "funds": "3.87915",
        "fee": "0.01163745",
        "feeRate": "0.003",
        "feeCurrency": "USDT",
        "stop": "",
        "tradeType": "TRADE",
        "type": "limit",
        "createdAt": 1732795200430
    }
]
        

    
    performance = analyze_trade_performance(trades)
    print(performance )
    df = pd.DataFrame(performance)
    print(df)
    print(type(performance))
    
    # Pretty print the results
    for symbol, stats in performance.items():
        print(f"\nPerformance for {symbol}:")
        print(f"Total Profit: ${stats['total_profit']:.2f}")
        print(f"Total Trades: {stats['total_trades']}")
        print(f"Win Rate: {stats['win_rate'] * 100:.2f}%")
        print(f"Average Profit per Trade: ${stats['average_profit_per_trade']:.2f}")

if __name__ == "__main__":
    main()