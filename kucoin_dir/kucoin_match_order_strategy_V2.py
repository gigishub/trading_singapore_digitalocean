import asyncio
import json
from datetime import datetime
import logging
from typing import Dict, List
from kucoin_order_manager import kucoinHForderManager
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

class KucoinStrategyTrader:
    def __init__(self, symbol: str, api_key: str, api_secret: str, api_passphrase: str):
        self.trading_client = kucoinHForderManager(api_key, api_secret, api_passphrase)
        self.symbol = symbol + "-USDT"
        
        # Essential state tracking
        self.price_first_match = None
        self.second_order_placed = False
        self.first_sell_price = None
        self.second_sell_order_placed = False
        
        # Trade data storage
        self.trade_data = {
            "buy_orders": {"first_group": None, "second_group": None},
            "sell_orders": {"first_group": None, "second_group": None},
            "trading_session": {
                "start_time": datetime.now().isoformat(),
                "symbol": self.symbol
            }
        }

    async def multiple_buy_orders_percent_dif(self, base_price: float, num_orders: int, 
                                            percentage_difference: float, time_in_force: str = "GTC") -> List[Dict]:
        size, decimal_to_round = self.order_size_and_rounding(base_price)
        
        prices = [
            str(float(base_price) * (1 + ((i + 1) * percentage_difference / 100)))
            for i in range(num_orders)
        ]
        
        orders = [
            {
                "symbol": self.symbol,
                "side": "buy",
                "price": str(round(float(price), decimal_to_round)),
                "size": str(size),
                "time_in_force": time_in_force
            }
            for price in prices
        ]

        return await self.trading_client.place_multiple_orders(orders)

    async def multiple_sell_orders_percent_dif(self, base_price: float, num_orders: int, 
                                             percentage_difference: float, time_in_force: str = "GTC") -> List[Dict]:
        size, decimal_to_round = self.order_size_and_rounding(base_price)
        
        prices = [
            str(float(base_price) * (1 + ((i + 1) * percentage_difference / 100)))
            for i in range(num_orders)
        ]

        orders = [
            {
                "symbol": self.symbol,
                "side": "sell",
                "price": str(round(float(price), decimal_to_round)),
                "size": str(size),
                "time_in_force": time_in_force
            }
            for price in prices
        ]

        return await self.trading_client.place_multiple_orders(orders)


    async def buy_first_and_second_match(self, num_orders: int, market_data: dict, percentage_difference: float):
        current_price = float(market_data['price'])
        tasks = []
        
        if self.price_first_match is None:
            self.price_first_match = current_price
            first_task = asyncio.create_task(
                self.multiple_buy_orders_percent_dif(current_price, num_orders, percentage_difference)
            )
            tasks.append(('first_group', first_task))
            self.trade_data["buy_orders"]["first_group"] = {
                "trigger_data": {k: market_data[k] for k in ['price', 'side', 'time_received']}
                # "orders" will be added after the task completes
            }
            
        elif not self.second_order_placed and current_price != self.price_first_match:
            second_task = asyncio.create_task(
                self.multiple_buy_orders_percent_dif(current_price, num_orders, percentage_difference)
            )
            tasks.append(('second_group', second_task))
            self.trade_data["buy_orders"]["second_group"] = {
                "trigger_data": {k: market_data[k] for k in ['price', 'side', 'time_received']}
                # "orders" will be added after the task completes
            }
            self.second_order_placed = True

        if tasks:
            labels, task_objects = zip(*tasks)
            results = await asyncio.gather(*task_objects)
            for label, result in zip(labels, results):
                self.trade_data["buy_orders"][label]["orders"] = result

    async def sell_on_first_sell_order(self, num_orders: int, market_data: dict, percentage_difference: float):
        current_price = float(market_data['price'])
        tasks = []
        
        if self.first_sell_price is None:
            self.first_sell_price = current_price
            first_sell_task = asyncio.create_task(
                self.multiple_sell_orders_percent_dif(current_price, num_orders, percentage_difference)
            )
            tasks.append(('first_group', first_sell_task))
            self.trade_data["sell_orders"]["first_group"] = {
                "trigger_data": {k: market_data[k] for k in ['price', 'side', 'time_received']}
                # "orders" will be added after the task completes
            }
            
        elif not self.second_sell_order_placed and current_price != self.first_sell_price:
            second_sell_task = asyncio.create_task(
                self.multiple_sell_orders_percent_dif(current_price, num_orders, percentage_difference)
            )
            tasks.append(('second_group', second_sell_task))
            self.trade_data["sell_orders"]["second_group"] = {
                "trigger_data": {k: market_data[k] for k in ['price', 'side', 'time_received']}
                # "orders" will be added after the task completes
            }
            self.second_sell_order_placed = True

        if tasks:
            labels, task_objects = zip(*tasks)
            results = await asyncio.gather(*task_objects)
            for label, result in zip(labels, results):
                self.trade_data["sell_orders"][label]["orders"] = result





    async def buy_first_and_second_match1(self, num_orders: int, market_data: dict, percentage_difference: float):
        current_price = float(market_data['price'])
        
        if self.price_first_match is None:
            self.price_first_match = current_price
            result = await self.multiple_buy_orders_percent_dif(current_price, num_orders, percentage_difference)
            self.trade_data["buy_orders"]["first_group"] = {
                "trigger_data": {k: market_data[k] for k in ['price', 'side', 'time_received']},
                "orders": result
            }
            
        elif not self.second_order_placed and current_price != self.price_first_match:
            result = await self.multiple_buy_orders_percent_dif(current_price, num_orders, percentage_difference)
            self.trade_data["buy_orders"]["second_group"] = {
                "trigger_data": {k: market_data[k] for k in ['price', 'side', 'time_received']},
                "orders": result
            }
            self.second_order_placed = True

    async def sell_on_first_sell_order1(self, num_orders: int, market_data: dict, percentage_difference: float):
        current_price = float(market_data['price'])
        
        if self.first_sell_price is None:
            self.first_sell_price = current_price
            result = await self.multiple_sell_orders_percent_dif(current_price, num_orders, percentage_difference)
            self.trade_data["sell_orders"]["first_group"] = {
                "trigger_data": {k: market_data[k] for k in ['price', 'side', 'time_received']},
                "orders": result
            }
            
        elif not self.second_sell_order_placed and current_price != self.first_sell_price:
            result = await self.multiple_sell_orders_percent_dif(current_price, num_orders, percentage_difference)
            self.trade_data["sell_orders"]["second_group"] = {
                "trigger_data": {k: market_data[k] for k in ['price', 'side', 'time_received']},
                "orders": result
            }
            self.second_sell_order_placed = True

    def save_trading_data(self,path_to_save:str = None):
        if not path_to_save:
            path_to_save = '/root/trading_systems/kucoin_dir/kucoin_trading_data'
        os.makedirs(path_to_save, exist_ok=True)

        token = self.symbol.split('-')[0]
        filename = f"{(datetime.now().strftime('%Y-%m-%d_%H-%M'))}_{token}.json"

        full_path = os.path.join(path_to_save, filename)
        self.trade_data["trading_session"]["end_time"] = datetime.now().isoformat()
        
        with open(full_path, 'w') as f:
            json.dump(self.trade_data, f, indent=4)

    async def close_client(self):
        await self.trading_client.close()

    def order_size_and_rounding(self, token_price):
        # Keeping your existing implementation
        token_price = float(token_price)
        ranges = [
            (0.0000045, 9, '1000100'),
            (0.000009, 9, '500050'),
            (0.000045, 8, '100100'),
            (0.00009, 8, '50050'),
            (0.00045, 7, '10100'),
            (0.0009, 7, '5050'),
            (0.0045, 6, '1010'),
            (0.009, 6, '505'),
            (0.045, 5, '110'),
            (0.09, 5, '55'),
            (0.45, 4, '11'),
            (0.5, 4, '6'),
            (0.75, 4, '3'),
            (0.9, 4, '2'),
            (4.5, 2, '2'),
            (9, 2, '2')
        ]
        
        for upper_bound, decimal_to_round, size in ranges:
            if token_price < upper_bound:
                return size, decimal_to_round






async def main():
    from kucoin_websocket_listen_DEV import KucoinWebsocketListen
    import json
    from datetime import datetime, timedelta

    # Load API credentials
    with open('/root/trading_systems/kucoin_dir/config_api.json', 'r') as file:
        api_creds = json.load(file)

    # Initialize trading components
    symbol = "XRP"
    strategy = KucoinStrategyTrader(symbol, api_creds['api_key'], 
                                  api_creds['api_secret'], 
                                  api_creds['api_passphrase'])
    
    ws_match = KucoinWebsocketListen(symbol, channel='match')
    run_match = asyncio.create_task(ws_match.start())

    try:
        end_time = datetime.now() + timedelta(minutes=1)
        
        while datetime.now() < end_time:
            market_data = ws_match.queue
            
            if market_data:
                print(market_data)
                # Handle buy orders
                if not strategy.second_order_placed:
                    await strategy.buy_first_and_second_match(2, market_data, -5)
                

                # Handle sell orders when market data shows sell side
                if market_data['side'] == 'sell' and not strategy.second_sell_order_placed:
                    await strategy.sell_on_first_sell_order(2, market_data, 5)
                
                # Break if both buy and sell cycles are complete
                if strategy.second_order_placed and strategy.second_sell_order_placed:
                    break
            else:
                print('no data')
            await asyncio.sleep(0.0001)
            
            


    finally:
        # Save trading data and cleanup
        strategy.save_trading_data()
        await ws_match.cleanup()
        await strategy.close_client()

if __name__ == "__main__":
    asyncio.run(main())