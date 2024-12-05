import asyncio
import json
from datetime import datetime
import logging
from typing import Dict, List
from kucoin_order_managerV2 import KucoinHFOrderManager
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

class Level2StrategyTrader:
    def __init__(self, symbol: str, api_key: str, api_secret: str, api_passphrase: str):
        self.trading_client = KucoinHFOrderManager(api_key, api_secret, api_passphrase)
        self.symbol = symbol + "-USDT"
        
        # Essential state tracking
        self.price_first_match = None
        self.second_order_placed = False
        self.first_sell_price = None
        self.second_sell_order_placed = False

        # ask trading storing values
        self.ask_trade_entry_price = None
        self.ask_trade_entry_price_found = False
        
        # Trade data storage
        self.trade_data = {
            "buy_orders": {},
            "sell_orders": {},
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





    def save_trading_data(self,path_to_save:str = None):
        if not path_to_save:
            path_to_save = '/root/trading_systems/kucoin_dir'
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
            (0.09, 5, '100'),
            (0.45, 4, '11'),
            (0.5, 4, '10'),
            (0.75, 4, '5'),
            (0.9, 4, '5'),
            (4.5, 2, '2'),
            (9, 2, '2')
        ]
        
        for upper_bound, decimal_to_round, size in ranges:
            if token_price < upper_bound:
                return size, decimal_to_round



            
    async def buy_first_ask_found(self, num_orders: int, market_data: dict, percentage_difference: float,wait_to_delete_orders:int = 5):
        
        for ask in market_data['changes']['asks']:
            if float(ask[1]) > 0:
                self.ask_trade_entry_price = ask[0]
    
        if self.ask_trade_entry_price:
            buy_result_ask = await self.multiple_buy_orders_percent_dif(self.ask_trade_entry_price, num_orders, percentage_difference)
            logger.info(f"{len(buy_result_ask)} buy orders sent for {self.symbol}")
            await self.delete_unfilled_orders(wait_to_delete_orders,buy_result_ask)
            self.trade_data['buy_orders']['trigger_data'] = market_data
            self.trade_data['buy_orders']["orders_sent"]= buy_result_ask
            return buy_result_ask


                # "orders" will be added after the task completes

    async def sell_bid(self, num_orders: int, market_data: dict, percentage_difference: float):
        if self.ask_trade_entry_price_found:
            bid_price = None
            if market_data['changes']['bids']:
                bid_price = market_data['changes']['bids'][0][0]
            if bid_price:
                if self.ask_trade_entry_price_found < bid_price:
                    sell_result_bid = await self.multiple_sell_orders_percent_dif(bid_price, num_orders, percentage_difference)
                    self.trade_data['sell_orders']['trigger_data'] = market_data
                    self.trade_data['sell_orders']["orders_sent"]= sell_result_bid
                # "orders" will be added after the task completes



                
            
    async def delete_unfilled_orders(self, seconds_delay:int,buy_result:List[Dict]):
        '''delete unfilled ordered with time delay from creation'''
        await asyncio.sleep(seconds_delay)
        deleted_orders = []
        for result in buy_result:
            if result['success'] == True:
                deleted_orders.append(result['orderId'])
                await self.trading_client.cancel_order_by_id(result['orderId'])
        logger.info(f"attempt to delete {len(deleted_orders)} orders for {self.symbol} if not filled else nothing to deleted")





async def main():
    from kucoin_websocket_listen_DEV import KucoinWebsocketListen
    import json
    from datetime import datetime, timedelta

    # Load API credentials
    with open('/root/trading_systems/kucoin_dir/config_api.json', 'r') as file:
        api_creds = json.load(file)

    # Initialize trading components
    symbol = "KIMA"
    strategy = Level2StrategyTrader(symbol, api_creds['api_key'], 
                                  api_creds['api_secret'], 
                                  api_creds['api_passphrase'])
    
    ws_levels2 = KucoinWebsocketListen(symbol, channel='level2')
    run_level2 = asyncio.create_task(ws_levels2.start())

    try:
        end_time = datetime.now() + timedelta(minutes=1)
        trade_entry_price = None
        while datetime.now() < end_time:
            market_data = await ws_levels2.queue.get()
            
            if market_data:
                print(json.dumps(market_data,indent =4))


                buy_result = await strategy.buy_first_ask_found(3, market_data, -7)
                if buy_result:
                    print(json.dumps(strategy.trade_data,indent=4))
                    break

            await asyncio.sleep(0.00001)
            
            


    finally:
        # Save trading data and cleanup
        strategy.save_trading_data('/root/trading_systems/kucoin_dir')
        await ws_levels2.cleanup()
        await strategy.close_client()

if __name__ == "__main__":
    asyncio.run(main())