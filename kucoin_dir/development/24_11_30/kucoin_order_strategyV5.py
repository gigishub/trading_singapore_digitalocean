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
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str):
        self.trading_client = kucoinHForderManager(api_key, api_secret, api_passphrase)
        self.buy_order_tasks = []
        self.should_stop = False
        self.all_executed_buy_order_results = []
        self.first_successful_order = None
        self.all_sell_order_results = []
        self.all_buy_orders = []

    async def cleanup_remaining_orders(self, symbol: str, size: str, limit_sell_price: str):
        """Place sell orders for any successful buy orders that weren't the fastest"""
        for order_response in self.all_executed_buy_order_results:
            if order_response != self.first_successful_order and order_response.get('success'):
                result = await self.trading_client.place_limit_sell(
                    symbol=symbol,
                    price=limit_sell_price,
                    size=size
                )
                sell_order_response = {
                    "success": result.get("success", False),
                    "price": limit_sell_price,
                    "order_id": result.get("order_id", "unknown"),
                    "order_sent_time": result.get("order_sent_time"),
                    "requestTime": result.get("requestTime"),
                    "execution_time": result.get("execution_time", 0),
                    "message": result.get("message", "No response"),
                    "sell_buy_order_num": order_response.get('order_num')
                }
                logger.info(f"Cleanup: Placed sell order for buy order {order_response.get('order_num')}")
                self.all_sell_order_results.append(sell_order_response)

    async def close_client(self):
        """Cleanup resources"""
        await self.trading_client.close()

    async def multiple_buy_orders_percent_dif(self, symbol: str, base_price: float, num_orders: int, percentage_difference: float, size_for_testing: int = 0, time_in_force: str = "GTC") -> List[Dict]:
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
                "symbol": symbol,  # KuCoin uses "-" instead of "" for symbol pairs
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

    async def multiple_sell_orders_percent_dif(self, symbol: str, base_price: float, num_orders: int, percentage_difference: float, size_for_testing: int = 0, time_in_force: str = "GTC") -> List[Dict]:
        """
        Place multiple limit sell orders concurrently with different prices based on a percentage difference.

        Args:
            symbol: Trading symbol (e.g., "BTC-USDT").
            base_price: The base price for the limit sell orders.
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
                "symbol": symbol,  # KuCoin uses "-" instead of "" for symbol pairs
                "side": "sell",
                "price": str(round(float(price), decimal_to_round)),
                "size": str(self.adjust_size_for_fees(float(size), 0.1, 1)),
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

    def order_size_and_rounding_ORG(self, token_price):
        """Determine order size and decimal rounding based on token price."""
        size = ''
        decimal_to_round = 0
        token_price = float(token_price)
        
        if token_price < 0.000009:
            decimal_to_round = 9
            size = '1000100'
        elif token_price < 0.00009:
            decimal_to_round = 8
            size = '100100'
        elif token_price < 0.0009:
            decimal_to_round = 7
            size = '10100'
        elif token_price < 0.009:
            decimal_to_round = 6
            size = '1010'
        elif token_price < 0.09:
            decimal_to_round = 5
            size = '110'
        elif token_price < 0.5:
            decimal_to_round = 4
            size = '11'
        elif token_price < 0.9:
            decimal_to_round = 4
            size = '3'
        elif token_price < 9:
            decimal_to_round = 2
            size = '3'
        else:
            decimal_to_round = 1
            size = '1'
        return size, decimal_to_round

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


# Example usage
async def main():
    import json
    logger.info("Starting strategy")
    with open('/root/trading_systems/kucoin_dir/config_api.json', 'r') as file:
        api_creds = json.load(file)

    api_key = api_creds['api_key']
    api_secret = api_creds['api_secret']
    api_passphrase = api_creds['api_passphrase']

    strategy = KucoinStrategyTrader(api_key, api_secret, api_passphrase)
    symbol = "XRP-USDT"  # Note the different symbol format for KuCoin
    base_price = 0.432
    
    # Place multiple buy orders
    base_buy_price = base_price
    percentage_dif_buy = -7.80
    percent_diff_sell = -7.60  
    num_buy_orders = 3
    
    buy_results = await strategy.multiple_buy_orders_percent_dif(
        symbol=symbol,
        base_price=base_buy_price,
        num_orders=num_buy_orders,
        percentage_difference=percentage_dif_buy
    )
    logger.info(f"Buy Order Results:{json.dumps(buy_results, indent=4)}")

    # Check how many buy orders were successful
    successful_buy_orders = [order for order in buy_results if order["success"]]
    num_successful_buy_orders = len(successful_buy_orders)
    logger.info(f"Number of successful buy orders: {num_successful_buy_orders}")

    if num_successful_buy_orders > 0:
        # Place multiple sell orders based on the number of successful buy orders
        base_sell_price = base_price
        sell_results = await strategy.multiple_sell_orders_percent_dif(
            symbol=symbol,
            base_price=base_sell_price,
            num_orders=num_successful_buy_orders,
            percentage_difference=percent_diff_sell
        )
        logger.info(f"Sell Order Results:{json.dumps(sell_results, indent=4)}")
    else:
        logger.info("No successful buy orders to place sell orders.")

    # Close the client
    await strategy.close_client()

if __name__ == "__main__":
    asyncio.run(main())