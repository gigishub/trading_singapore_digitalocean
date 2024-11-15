import asyncio
import logging
from typing import Dict, List
from bitget_order_manager import BitgetOrderManager

# Create a dedicated logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

class BitgetStrategyTrader:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str):
        self.trading_client = BitgetOrderManager(api_key, api_secret, api_passphrase)
        self.buy_order_tasks = []
        self.should_stop = False
        self.all_executed_buy_order_results = []
        self.first_successful_order = None
        self.all_sell_order_results = []
        self.all_buy_orders = []
        self.all_executed_sell_order_results = []
        self.all_sell_orders = []
        


    async def execute_staged_orders(self, 
                                    symbol: str,
                                    order_price: str,
                                    size: str,
                                    num_orders: int = 10,
                                    time_offset_ms: int = 30):
        """
        Execute multiple buy orders with time offsets, immediately proceeding after first success
        """
        try:
            async def place_single_order(order_num: int):
                try:
                    if order_num > 0:
                        await asyncio.sleep(time_offset_ms / 1000 * order_num)

                    if self.should_stop:
                        return None

                    logger.info(f"Placing order {order_num + 1}/{num_orders}")

                    result = await self.trading_client.place_limit_buy(
                        symbol=symbol,
                        price=order_price,
                        size=size
                    )

                    order_info = {
                        "success": result.get("success", False),
                        "order_num": order_num + 1,
                        "price": order_price,
                        "order_size": size,
                        "order_sent_time": result.get("order_sent_time"),
                        "requestTime": result.get("requestTime"),
                        "execution_time": result.get("execution_time", 0),
                        "order_id": result.get("order_id", "unknown"),
                        "message": result.get("message", "No response")
                    }

                    self.all_executed_buy_order_results.append(order_info)

                    if result.get("success"):
                        logger.info(f"Buy Order {order_num + 1} succeeded! Execution time: {result.get('execution_time', 0):.3f}s")
                        self.should_stop = True
                        return order_info
                    else:
                        logger.info(f"Order {order_num + 1} failed: {result.get('message', 'Unknown error')}")
                        return order_info

                except Exception as e:
                    logger.error(f"Error in order {order_num + 1}: {str(e)}")
                    return None

            tasks = [
                asyncio.create_task(place_single_order(i))
                for i in range(num_orders)
            ]
            self.buy_order_tasks = tasks

            successful_result = None
            while True:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    result = await task
                    if result and result.get("success"):
                        successful_result = result
                        self.first_successful_order = result
                        self.should_stop = True
                        break
                if successful_result or not pending:
                    break

            return successful_result

        except Exception as e:
            logger.error(f"Error in staged order execution: {str(e)}")
            return None



    async def execute_concurrent_orders(self,
                                        symbol: str,
                                        order_price: str,
                                        size: str,
                                        num_orders: int = 10) -> Dict:
        """
        Execute multiple buy orders concurrently using place_multiple_orders functionality
        """
        try:
            orders = [
                {
                    "symbol": symbol,
                    "side": "buy",
                    "price": order_price,
                    "size": size
                }
                for _ in range(num_orders)
            ]

            logger.info(f"Placing {num_orders} concurrent orders")
            results = await self.trading_client.place_multiple_orders(orders)

            # Process results
            successful_orders = []
            for idx, result in enumerate(results):
                order_info = {
                    "success": result.get("success", False),
                    "order_num": idx + 1,
                    "price": order_price,
                    "order_size": size,
                    "order_sent_time": result.get("order_sent_time"),
                    "requestTime": result.get("requestTime"),
                    "execution_time": result.get("execution_time", 0),
                    "order_id": result.get("order_id", "unknown"),
                    "message": result.get("message", "No response")
                }
                self.all_executed_buy_order_results.append(order_info)
                
                if result.get("success"):
                    successful_orders.append(order_info)

            if successful_orders:
                fastest_order = min(successful_orders, key=lambda x: x["execution_time"])
                self.first_successful_order = fastest_order
                logger.info(f"Fastest successful order: {fastest_order['order_num']} ({fastest_order['execution_time']:.3f}s)")
                return fastest_order
            
            return None

        except Exception as e:
            logger.error(f"Error in concurrent order execution: {str(e)}")
            return None

    async def multiple_buy_order_offset_time(self, 
                                             symbol: str, 
                                             limit_buy_price: str, 
                                             limit_sell_price: str, 
                                             size: str, 
                                             num_orders: int, 
                                             time_offset_ms: int):
        """Run the trading strategy with time offsets between orders"""
        try:
            buy_result = await self.execute_staged_orders(
                symbol=symbol,
                order_price=limit_buy_price,
                size=size,
                num_orders=num_orders,
                time_offset_ms=time_offset_ms,
            )

            if buy_result:
                logger.info(f"Fastest buy order: {buy_result['order_num']} at price {buy_result['price']}")
                result = await self.trading_client.place_limit_sell(
                    symbol=symbol,
                    size=size,
                    price=limit_sell_price
                )
                
                sell_result = {
                    "success": result.get("success", False),
                    "price": limit_sell_price,
                    "order_id": result.get("order_id", "unknown"),
                    "order_sent_time": result.get("order_sent_time"),
                    "requestTime": result.get("requestTime"),
                    "execution_time": result.get("execution_time", 0),
                    "message": result.get("message", "No response"),
                    "sell_buy_order_num": buy_result['order_num']
                }
                
                if sell_result.get('success'):
                    logger.info(f"Sell order succeeded at price: {sell_result['price']}")
                self.all_sell_order_results.append(sell_result)
            
            await self.cleanup_remaining_orders(symbol, size, limit_sell_price)
            
            return {
                "all_executed_buy_orders": self.all_executed_buy_order_results,
                "all_executed_sell_orders": self.all_sell_order_results
            }

        except Exception as e:
            logger.error(f"Error in strategy execution: {str(e)}")
        finally:
            await self.close_client()

    async def execute_concurrent_strategy(self,
                                          symbol: str,
                                          limit_buy_price: str,
                                          limit_sell_price: str,
                                          size: str,
                                          num_orders: int):
        """Run the trading strategy using concurrent order execution"""
        try:
            buy_result = await self.execute_concurrent_orders(
                symbol=symbol,
                order_price=limit_buy_price,
                size=size,
                num_orders=num_orders
            )

            if buy_result:
                logger.info(f"Fastest buy order: {buy_result['order_num']} at price {buy_result['price']}")
                result = await self.trading_client.place_limit_sell(
                    symbol=symbol,
                    size=size,
                    price=limit_sell_price
                )
                
                sell_result = {
                    "success": result.get("success", False),
                    "price": limit_sell_price,
                    "order_id": result.get("order_id", "unknown"),
                    "order_sent_time": result.get("order_sent_time"),
                    "requestTime": result.get("requestTime"),
                    "execution_time": result.get("execution_time", 0),
                    "message": result.get("message", "No response"),
                    "sell_buy_order_num": buy_result['order_num']
                }
                
                if sell_result.get('success'):
                    logger.info(f"Sell order succeeded at price: {sell_result['price']}")
                self.all_sell_order_results.append(sell_result)
            
            await self.cleanup_remaining_orders(symbol, size, limit_sell_price)
            
            return {
                "all_executed_buy_orders": self.all_executed_buy_order_results,
                "all_executed_sell_orders": self.all_sell_order_results
            }

        except Exception as e:
            logger.error(f"Error in strategy execution: {str(e)}")
        finally:
            await self.close_client()

    async def cleanup_remaining_orders(self, symbol: str, size: str, limit_sell_price: str):
        """Place sell orders for any successful buy orders that weren't the fastest"""
        for order_response in self.all_executed_buy_order_results:
            if order_response != self.first_successful_order and order_response.get('success'):
                result = await self.trading_client.place_limit_sell(
                    symbol=symbol,
                    size=size,
                    price=limit_sell_price
                )
                sell_order_response = {
                    "success": result.get("success", False),
                    "price": limit_sell_price,
                    "order_id": result.get("order_id", "unknown"),
                    "order_sent_time": result.get("order_sent_time"),
                    "requestTime": result.get("requestTime"),
                    "execution_time": result.get("execution_time", 0),
                    "message": result.get("message", "No response"),
                    "sell_buy_order_num": order_response['order_num']
                }
                logger.info(f"Cleanup: Placed sell order for buy order {order_response['order_num']}")
                self.all_sell_order_results.append(sell_order_response)

    async def close_client(self):
        """Cleanup resources"""
        await self.trading_client.close()
    


    async def multiple_buy_orders_percent_dif(self, symbol: str, base_price: float, size: str, num_orders: int, percentage_difference: float, time_in_force: str = "gtc") -> List[Dict]:
        """
        Place multiple limit buy orders concurrently with different prices based on a percentage difference.

        Args:
            symbol: Trading symbol (e.g., "BTCUSDT").
            base_price: The base price for the limit buy orders.
            size: Size of each order.
            num_orders: Number of orders to place.
            percentage_difference: Percentage difference between each order price.
            time_in_force: Time in force for the orders (default is "gtc").

        Returns:
            List of order results.
        """
        prices = [
            str(base_price * (1 + (i * percentage_difference / 100)))
            for i in range(num_orders)
        ]

        orders = [
            {
                "symbol": symbol,
                "side": "buy",
                "price": price,
                "size": size,
                "time_in_force": time_in_force
            }
            for price in prices
        ]
        multi_buy_order_resposne = await self.trading_client.place_multiple_orders(orders)
        self.all_buy_orders.extend(multi_buy_order_resposne) 
        for order in multi_buy_order_resposne:
            if order["success"]:
                self.all_executed_buy_order_results.append(order)
        
        return multi_buy_order_resposne




    async def multiple_sell_orders_percent_dif(self, symbol: str, base_price: float, size: str, num_orders: int, percentage_difference: float, time_in_force: str = "gtc") -> List[Dict]:
        """
        Place multiple limit sell orders concurrently with different prices based on a percentage difference.

        Args:
            symbol: Trading symbol (e.g., "BTCUSDT").
            base_price: The base price for the limit sell orders.
            size: Size of each order.
            num_orders: Number of orders to place.
            percentage_difference: Percentage difference between each order price.
            time_in_force: Time in force for the orders (default is "gtc").

        Returns:
            List of order results.
        """
        prices = [
            str(base_price * (1 + (i * percentage_difference / 100)))
            for i in range(num_orders)
        ]

        orders = [
            {
                "symbol": symbol,
                "side": "sell",
                "price": price,
                "size": size,
                "time_in_force": time_in_force
            }
            for price in prices
        ]
        multi_sell_order_response = await self.trading_client.place_multiple_orders(orders)
        self.all_sell_orders.extend(multi_sell_order_response)
        for order in multi_sell_order_response:
            if order["success"]:
                self.all_executed_sell_order_results.append(order)
        
        return multi_sell_order_response

async def main():
    import json
    logger.info("Starting strategy")
    with open('/root/trading_systems/bitget/api_creds.json', 'r') as file:
        api_creds = json.load(file)

    api_key = api_creds['api_key']
    api_secret = api_creds['api_secret']
    api_passphrase = api_creds['api_passphrase']

    strategy = BitgetStrategyTrader(api_key, api_secret, api_passphrase)

    # Place multiple buy orders
    base_buy_price = 30000
    percentage_difference = 1  # 1%
    num_buy_orders = 5
    buy_results = await strategy.multiple_buy_orders_percent_dif(
        symbol="BTCUSDT",
        base_price=base_buy_price,
        size="0.0001",
        num_orders=num_buy_orders,
        percentage_difference=percentage_difference
    )
    logger.info(f"Buy Order Results:{json.dumps(buy_results, indent=4)}")

    # Check how many buy orders were successful
    successful_buy_orders = [order for order in buy_results if order["success"]]
    num_successful_buy_orders = len(successful_buy_orders)
    logger.info(f"Number of successful buy orders: {num_successful_buy_orders}")

    if num_successful_buy_orders > 0:
        # Place multiple sell orders based on the number of successful buy orders
        base_sell_price = 31000
        sell_results = await strategy.multiple_sell_orders_percent_dif(
            symbol="BTCUSDT",
            base_price=base_sell_price,
            size="0.0001",
            num_orders=num_successful_buy_orders,
            percentage_difference=percentage_difference
        )
        logger.info(f"Sell Order Results:{json.dumps(sell_results, indent=4)}")
    else:
        logger.info("No successful buy orders to place sell orders.")

    # Close the client
    await strategy.close_client()

if __name__ == "__main__":
    asyncio.run(main())

# async def main():
#     import json
#     logger.info("Starting strategy")
#     with open('/root/trading_systems/bitget/api_creds.json', 'r') as file:
#         api_creds = json.load(file)

#     api_key = api_creds['api_key']
#     api_secret = api_creds['api_secret']
#     api_passphrase = api_creds['api_passphrase']

#     strategy = BitgetStrategyTrader(api_key, api_secret, api_passphrase)
#     results = await strategy.execute_concurrent_strategy(
#         symbol="BTCUSDT",
#         limit_buy_price="30000",
#         limit_sell_price="31000",
#         size="0.0001",
#         num_orders=2
#     )
#     logger.info(f"Order Results:{json.dumps(results, indent=4)}")

# if __name__ == "__main__":
#     asyncio.run(main())