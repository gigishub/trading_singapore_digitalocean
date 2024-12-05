import asyncio
import json
from datetime import datetime
import logging
from typing import Dict, List
from kucoin_order_managerV2 import KucoinHFOrderManager
import os
import traceback
from datetime import timedelta


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

class MatchStrategyTrader:
    def __init__(self, basecoin: str, api_key: str, api_secret: str, api_passphrase: str):
        self.trading_client = KucoinHFOrderManager(api_key, api_secret, api_passphrase)
        self.symbol = basecoin + "-USDT"
        self.basecoin = basecoin
        
        # Essential state tracking
        self.price_first_match = None
        self.second_order_placed = False
        self.first_sell_price = None
        self.second_sell_order_placed = False

        self.first_buy_match_price = None
        self.enable_performance_snapshot = False

        self.sellcounter = 0
        
        # Trade data storage
        self.trade_data = {
            "trading_session": {
                "start_time": datetime.now().isoformat(),
                "symbol": self.symbol
            },
            "trade_performance": {},
            "buy_orders": {},
            "sell_orders": {},
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
            self.trade_data["buy_orders"]= {
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




    def save_trading_data(self,path_to_save:str = None):
        if not path_to_save:
            path_to_save = '/root/trading_systems/kucoin_dir'
        else:
            path_to_save = path_to_save
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
            (0.5, 4, '8'),
            (0.75, 4, '7'),
            (0.9, 4, '6'),
            (1.5, 3, '4'),
            (2, 3, '3'),
            (4.5, 2, '1'),
            (9, 2, '0.5')
        ]
        
        for upper_bound, decimal_to_round, size in ranges:
            if token_price < upper_bound:
                return size, decimal_to_round



    async def multiple_sell_percentdiff_same_price(self, base_price: float, num_orders: int, 
                                            percentage_difference: float, time_in_force: str = "GTC") -> List[Dict]:
        size, decimal_to_round = self.order_size_and_rounding(base_price)
        
        # Calculate the price that is n% different from the base price
        adjusted_price = str(round(float(base_price) * (1 + (percentage_difference / 100)), decimal_to_round))

        # Create multiple orders at the same adjusted price
        orders = [
            {
                "symbol": self.symbol,
                "side": "sell",
                "price": adjusted_price,
                "size": str(size),
                "time_in_force": time_in_force
            }
            for _ in range(num_orders)
        ]

        return await self.trading_client.place_multiple_orders(orders)



    async def buy_if_makerOrderId(self, num_orders_buy: int, market_data: dict, percentage_difference: float):
        if market_data['makerOrderId']:
            self.first_buy_match_price = float(market_data['price'])
            first_buyprice = await self.multiple_buy_orders_percent_dif(self.first_buy_match_price, num_orders_buy, percentage_difference)
            


            self.trade_data["buy_orders"]['trigger_data']= {
                k: market_data[k] for k in ['price', 'side', 'time_received','makerOrderId']
                }
            self.trade_data["buy_orders"]["orders_sent"] = first_buyprice

            return first_buyprice


    async def sell_on_5_sell_in_a_row(self,num_orders_sell:int, market_data: dict, percentage_difference: float):
        if market_data['side'] == 'sell':
            self.sellcounter += 1
        if market_data['side'] == 'buy':
            self.sellcounter = 0
        
        if self.sellcounter == 5:

            sell_task = await self.multiple_sell_percentdiff_same_price(market_data['price'], num_orders_sell, percentage_difference)

            self.trade_data["sell_orders"]['trigger_data']= {
                k: market_data[k] for k in ['price', 'side', 'time_received','makerOrderId']
                }
            self.trade_data["sell_orders"]["orders_sent"] = sell_task

            return True

    async def delete_unfilled_orders_old(self, seconds_delay:int,buy_result:List[Dict]):
        '''delete unfilled ordered with time delay from creation'''
        await asyncio.sleep(seconds_delay)
        for result in buy_result:
            if result['success'] == True:
                logger.info(f"Attempt to delete orderId: {result['orderId']} delted")
                await self.trading_client.cancel_order_by_id(result['orderId'])

    async def delete_unfilled_orders(self, seconds_delay:int,buy_result:List[Dict]):
        '''delete unfilled ordered with time delay from creation'''
        await asyncio.sleep(seconds_delay)
        deleted_orders = []
        for result in buy_result:
            if result['success'] == True:
                deleted_orders.append(result['orderId'])
                await self.trading_client.cancel_order_by_id(result['orderId'])
        logger.info(f"attempt to delete {len(deleted_orders)} orders for {self.symbol} if not filled else nothing to deleted")





    async def strategy(self,num_orders_buy:int,
                       num_orders_sell:int ,
                       market_data: dict, 
                       percentage_diff_sell: float,
                       percentage_diff_buy: float):
        
        try:
            delete_buy_order_task = None
            if not self.first_buy_match_price:
                result_buy = await self.buy_if_makerOrderId(num_orders_buy, market_data, percentage_diff_buy)
                if result_buy:
                    #logger.info(json.dumps(self.trade_data,indent=4))
                    delete_buy_order_task =asyncio.create_task(self.delete_unfilled_orders(4,result_buy))


                    # if buy rult orders are not filled in 5 seconds, cancel them
                    

            if self.first_buy_match_price:
                result_sell = await self.sell_on_5_sell_in_a_row(num_orders_sell,market_data, percentage_diff_sell)
                if result_sell:
                    #logger.info(json.dumps(self.trade_data,indent=4))
                    if delete_buy_order_task:
                        try:
                            await asyncio.wait_for(delete_buy_order_task,timeout=5)
                        except asyncio.TimeoutError:
                            logger.info("Timeout reached, buy orders not deleted")
                            pass
                    self.enable_performance_snapshot = True
            if self.enable_performance_snapshot:
                await self.trading_client.check_if_order_filled()
                self.trade_data['trade_performance'] = await self.trading_client.performance_snapshot(self.basecoin)
                self.trade_data['buy_orders']['filled_orders'] = self.trading_client.filled_buy_orders
                self.trade_data['sell_orders']['filled_orders'] = self.trading_client.filled_sell_orders
                return True

        
        except Exception as e:
            self.trade_data['error'] = str(e)
            logger.error(f"Error in strategy: {e}")
            traceback.print_exc()


async def main():
    from kucoin_websocket_listen_DEV import KucoinWebsocketListen
    import json
    from datetime import datetime, timedelta

    # Load API credentials
    with open('/root/trading_systems/kucoin_dir/config_api.json', 'r') as file:
        api_creds = json.load(file)

    # Initialize trading components
    basecoin = "FET"
    strategy = MatchStrategyTrader(basecoin, api_creds['api_key'], 
                                  api_creds['api_secret'], 
                                  api_creds['api_passphrase'])
    
    ws_match = KucoinWebsocketListen(basecoin, channel='match')
    run_match = asyncio.create_task(ws_match.start())

    try:
        end_time = datetime.now() + timedelta(minutes=10)
        
        while datetime.now() < end_time:
            market_data = await ws_match.get_data()
            
            if market_data:
                #print(json.dumps(market_data,indent=4))
                strategy_result = await strategy.strategy(3,3,market_data,10,-10)
                if strategy_result:
                    strategy.save_trading_data('/root/trading_systems/kucoin_dir/taibdabidbnai')
                    break
                    

            await asyncio.sleep(0.0001)


    finally:
        await ws_match.cleanup()
        await strategy.close_client()

if __name__ == "__main__":
    asyncio.run(main())