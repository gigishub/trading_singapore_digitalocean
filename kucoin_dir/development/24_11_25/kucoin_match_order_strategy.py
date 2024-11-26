import asyncio
import logging
from typing import Dict, List
from kucoin_order_manager import kucoinHForderManager
import math

# Create a dedicated logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

class KucoinStrategyTrader:
    def __init__(self,symbol:str , api_key: str, api_secret: str, api_passphrase: str):
        self.trading_client = kucoinHForderManager(api_key, api_secret, api_passphrase)
        self.buy_order_tasks = []
        self.should_stop = False
        self.all_executed_buy_order_results = []
        self.first_successful_order = None
        self.all_sell_order_results = []
        self.all_buy_orders = []
        self.symbol = symbol + "-USDT"
        
        # buying first and second match prices variables
        self.price_first_match = None
        self.price_second_match = None
        self.first_order_task = None
        self.second_order_task = None
        self.second_order_placed = None

        # selling first sell order variables
        self.first_sell_price = None
        self.first_sell_order_task = None
        self.second_sell_price = None
        self.second_sell_order_task = None
        self.second_sell_order_placed = None


    async def close_client(self):
        """Cleanup resources"""
        await self.trading_client.close()


    async def multiple_buy_orders_percent_dif(self,  base_price: float, num_orders: int, percentage_difference: float, size_for_testing: int = 0, time_in_force: str = "GTC") -> List[Dict]:
        """
        Place multiple limit buy orders concurrently with different prices based on a percentage difference.

        Args:
            symbol: Trading symbol (e.g., "BTC-USDT").
            base_price: The base price for the limit buy orders.
            num_orders: Number of orders to place.
            percentage_difference: Percentage difference between each order price.
            size_for_testing: Override size for testing purposes.
            time_in_force: Time in force for the orders (default is "GTC").

        Returns:
            List of order results.
        """
        size, decimal_to_round = self.order_size_and_rounding(base_price)
        if size_for_testing != 0:
            size = size_for_testing

        prices = [
            str(float(base_price) * (1 + ((i + 1) * percentage_difference / 100)))
            for i in range(num_orders)
        ]
        
        orders = [
            {
                "symbol": self.symbol,  # KuCoin uses "-" instead of "" for symbol pairs
                "side": "buy",
                "price": str(round(float(price), decimal_to_round)),
                "size": str(size),
                "time_in_force": time_in_force
            }
            for price in prices
        ]

        multi_buy_order_response = await self.trading_client.place_multiple_orders(orders)
        self.all_buy_orders.extend(multi_buy_order_response)
        for order in multi_buy_order_response:
            if order["success"]:
                self.all_executed_buy_order_results.append(order)
        
        return multi_buy_order_response
    

    async def multiple_sell_orders_percent_dif(self, base_price: float, num_orders: int, percentage_difference: float, time_in_force: str = "GTC") -> List[Dict]:
        """
        Place multiple limit sell orders concurrently with different prices based on a percentage difference.

        Args:
            symbol: Trading symbol (e.g., "BTC-USDT").
            base_price: The base price for the limit sell orders.
            num_orders: Number of orders to place.
            percentage_difference: Percentage difference between each order price.
            time_in_force: Time in force for the orders (default is "GTC").

        Returns:
            List of order results.
        """
        size, decimal_to_round = self.order_size_and_rounding(base_price)

        prices = [
            str(float(base_price) * (1 + ((i + 1) * percentage_difference / 100)))
            for i in range(num_orders)
        ]

        orders = [
            {
                "symbol": self.symbol,  # KuCoin uses "-" instead of "" for symbol pairs
                "side": "sell",
                "price": str(round(float(price), decimal_to_round)),
                "size": size,   # str(self.adjust_size_for_fees(float(size), 0.1, 1)),
                "time_in_force": time_in_force
            }
            for price in prices
        ]

        multi_sell_order_response = await self.trading_client.place_multiple_orders(orders)
        for order in multi_sell_order_response:
            if order["success"]:
                self.all_sell_order_results.append(order)
        
        return multi_sell_order_response

    def adjust_size_for_fees(self, order_size: float, fee_in_percent: float, decimal_places: int = 2) -> float:
        """Adjust the original value by subtracting a percentage of it and round down to the specified decimal places."""
        adjusted_value = order_size * (1 - (fee_in_percent / 100))
        
        if order_size > 100:
            return int(adjusted_value)
        
        factor = 10 ** decimal_places
        return math.floor(adjusted_value * factor) / factor



    def order_size_and_rounding(self, token_price):
        """Determine order size and decimal rounding based on token price."""
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



        
    async def buy_first_and_second_match(self, num_orders: int, current_match_price: float, percentage_difference: float):
        try:
            # First match handling
            if self.price_first_match is None:
                logger.info(f"Setting first match price to {current_match_price}")
                self.price_first_match = current_match_price
                self.first_order_task = asyncio.create_task(
                    self.multiple_buy_orders_percent_dif(current_match_price, num_orders, percentage_difference)
                )
                logger.info(f"Created first order task at price: {self.price_first_match}")
            
            # Second match handling
            elif not self.second_order_placed and current_match_price != self.price_first_match:
                logger.info(f"Current price {current_match_price} differs from first match {self.price_first_match}")
                self.price_second_match = current_match_price
                self.second_order_task = asyncio.create_task(
                    self.multiple_buy_orders_percent_dif(current_match_price, num_orders, percentage_difference)
                )
                self.second_order_placed = True
                logger.info(f"Created second order task at price: {self.price_second_match}")
            
            # Gather and return results if we have any tasks
            tasks = [task for task in [self.first_order_task, self.second_order_task] if task is not None]
            if tasks:
                logger.info(f"Gathering {len(tasks)} tasks")
                return await asyncio.gather(*tasks)
            
            return None

        except Exception as e:
            logger.error(f"Error in buy_first_and_second_match: {str(e)}", exc_info=True)
            return None


    async def sell_on_first_sell_order(self, num_orders: int, current_sell_price: float, percentage_difference: float):
        try:
            # First match handling
            if self.first_sell_price is None:
                logger.info(f"Setting first sell price to {current_sell_price}")
                self.first_sell_price = current_sell_price
                self.first_sell_order_task = asyncio.create_task(
                    self.multiple_sell_orders_percent_dif(current_sell_price, num_orders, percentage_difference)
                )
                logger.info(f"Created first sell order task at price: {self.price_first_match}")
            
            elif self.first_sell_price and not self.second_sell_order_placed and  (current_sell_price != self.price_first_match):
                logger.info(f"Current price {current_sell_price} differs from first sell price {self.first_sell_price}")
                self.second_sell_price = current_sell_price
                self.second_sell_order_task = asyncio.create_task(
                    self.multiple_sell_orders_percent_dif(current_sell_price, num_orders, percentage_difference)
                )
                self.second_sell_order_placed = True
                logger.info(f"Created second sell order task at price: {self.second_sell_price}")
                
            # Gather and return results if we have any tasks
            tasks = [task for task in [self.first_order_task] if task is not None]
            if tasks:
                logger.info(f"Gathering {len(tasks)} tasks")
                return await asyncio.gather(*tasks)
            
            return None

        except Exception as e:
            logger.error(f"Error in sell_on_first_sell_order: {str(e)}", exc_info=True)
            return None



##########################################################

# testing

##########################################################



# Example usage
async def main():
    import datetime
    from datetime import datetime, timedelta

    from kucoin_websocket_listen_DEV import KucoinWebsocketListen
    import json
    logger.info("Starting strategy")
    with open('/root/trading_systems/kucoin_dir/config_api.json', 'r') as file:
        api_creds = json.load(file)

    api_key = api_creds['api_key']
    api_secret = api_creds['api_secret']
    api_passphrase = api_creds['api_passphrase']
    symbol = "XRP"  

    strategy = KucoinStrategyTrader(symbol,api_key, api_secret, api_passphrase)
    ws_match = KucoinWebsocketListen(symbol,channel='match')
    run_match = asyncio.create_task(ws_match.start())
    buy_first_second_output = None
    sell_output = None
    try:
        end_time = datetime.now() + timedelta(minutes=0.1)
        logger.debug(f'{end_time}')
        while datetime.now() < end_time:
            data = ws_match.queue
            if data:
                buy_first_second_output = await strategy.buy_first_and_second_match(2, data['price'], -5)

            if data:    
                if data['side'] == 'sell':
                    sell_output = await strategy.sell_on_first_sell_order(2, data['price'], 5)
                    
                
            if data:
                print(data)
            else:
                print("No data")
            await asyncio.sleep(1)
        logger.info(f'buy output: {json.dumps(buy_first_second_output,indent=4)}')
        logger.info(f'sell output: {json.dumps(sell_output,indent=4)}')
    finally:
        await ws_match.cleanup()
        await strategy.close_client()




if __name__ == "__main__":
    asyncio.run(main())