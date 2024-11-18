import asyncio
import aiohttp
import time
import uuid
import hmac
import base64
import hashlib
from typing import Optional, Dict, Union, List
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from kucoin_order_manager import kucoinHForderManager
import traceback

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s ',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class KucoinStrategyTrader:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str):
        self.trading_client = kucoinHForderManager(api_key, api_secret, api_passphrase)
        self.buy_order_tasks = []
        self.should_stop = False
        self.all_executed_buy_order_results = []
        self.first_succesful_order = None  
        self.all_sell_order_results = []


    async def execute_staged_orders(self, 
                                    symbol: str,
                                    order_price: str,
                                    size: str,
                                    num_orders: int = 10,
                                    time_offset_ms: int = 30,
                                    price_increment: float = None):
        """
        Execute multiple buy orders with time offsets, immediately proceeding after first success
        return example: 
        {'success': True, 'order_num': 1, 'price': '0.2', 'order_sent_time': '14:39:06.658', 'execution_time': 0.16934753803070635, 'order_id': '6734ba0a858fce00075bae52'}
        """
        try:
            logger.info(f"Starting staged order execution: {num_orders} orders, {time_offset_ms}ms offset")

            async def place_single_order(order_num: int):
                try:

                    # if i want to adjust the price with increment
                    #price_str = str(float(base_price) + price_increment * order_num) if price_increment else base_price
                    price_str = str(order_price)
                    # Wait for the appropriate time offset
                            # Apply time offset only for orders after the first one
                    if order_num > 0:
                        await asyncio.sleep(time_offset_ms / 1000 * order_num)

                    #await asyncio.sleep(time_offset_ms / 1000 * order_num)

                    if self.should_stop:
                        return None

                    order_sent_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    logger.info(f"Placing order {order_num + 1}/{num_orders} at {order_sent_time} - Price: {price_str}")

                    try:
                        # Protect the order placement from cancellation
                        result = await asyncio.shield(self.trading_client.place_limit_buy(
                            symbol=symbol,
                            price=price_str,
                            size=size
                        ))

                        order_info = {
                            "success": result.get("success", False) if result else False,
                            "order_num": order_num + 1,
                            "price": price_str,
                            "order_size": size,
                            "order_sent_time": order_sent_time,
                            "execution_time": result.get("execution_time", 0) if result else 0,
                            "order_id": result.get("order_id", "unknown") if result else "unknown",
                            "message": result.get("message", "No response") if result else "No response"
                        }


                        self.all_executed_buy_order_results.append(order_info)

                        if result and result.get("success"):
                            exec_time = result.get("execution_time", 0)
                            logger.info(f"Buy Order {order_num + 1} succeeded! Price: {price_str}, Execution time: {exec_time:.3f}s")
                            self.should_stop = True  # Signal other tasks to stop placing orders
                            return order_info
                        
                        # when order fails 
                        # section that executes when order key success from trading_client object (kucoin_order_manager) returns False
                        else:
                            error_msg = result.get('message') if result else 'No response'
                            logger.info(f"Order {order_num + 1} failed: {error_msg}")
                            return order_info


                    except Exception as e:
                        logger.error(f"Error in order {order_num + 1}: {str(e)}")
                        order_info = {
                            "success": False,
                            "order_num": order_num + 1,
                            "price": price_str,
                            "order_sent_time": order_sent_time,
                            "execution_time": 0,
                            "order_id": "unknown",
                            "message": str(e)
                        }
                        self.all_executed_buy_order_results.append(order_info)
                        return order_info

                except asyncio.CancelledError:
                    # If the task is cancelled before placing the order
                    logger.info(f"Order {order_num + 1} was cancelled before placement")
                    order_info = {
                        "order_num": order_num + 1,
                        "price": None,
                        "order_sent_time": None,
                        "execution_time": 0,
                        "order_id": None,
                        "success": False,
                        "message": "Cancelled before placement"
                    }
                    self.all_executed_buy_order_results.append(order_info)
                    return order_info

            # Create all order tasks
            tasks = [
                asyncio.create_task(place_single_order(i))
                for i in range(num_orders)
            ]

            self.buy_order_tasks = tasks  # Save tasks for later reference

            successful_result = None

            # Wait for the first successful order
            while True:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    result = await task
                    if result and result.get("success"):
                        successful_result = result
                        self.first_succesful_order = result

                        self.should_stop = True  # Signal other tasks to stop placing orders
                        break
                if successful_result or not pending:
                    break

            # Return the successful result immediately
            return successful_result

        except Exception as e:
            logger.error(f"Error in staged order execution: {str(e)}")
            return None
        
    async def execute_sell_limit_order(self, symbol: str, price: str, size: str):
        """
        Execute simple sell order 
        """
        try:
            order_sent_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            result = await self.trading_client.place_limit_sell(
                symbol=symbol,
                price=price,
                size=size
            )
            if result and result.get("success"):
                return {
                    "success": result.get("success", False) if result else False,
                    "price": price,
                    "order_id": result.get("order_id", "unknown") if result else "unknown",
                    "order_sent_time": order_sent_time,
                    "execution_time": result.get("execution_time", 0) if result else 0,
                }
            else:
                # message is already correctly prepared in in order manager class
                return result

        except Exception as e:
            logger.error(f"Error in sell order execution: {str(e)}")
            return result

    
    async def close_client(self):
        """Cleanup resources"""
        await self.trading_client.close()


    async def multiple_buy_order_offset_time(self, symbol: str, limit_buy_price: str, limit_sell_price: str, size: str, num_orders: int, time_offset_ms: int):
        """
        Run the trading strategy with the given parameters
        """
        try:
            # Execute buy orders
            logger.info("Starting buy strategy...")
            buy_result = await self.execute_staged_orders(
                symbol=symbol,
                order_price=limit_buy_price,
                size=size,
                num_orders=num_orders,
                time_offset_ms=time_offset_ms,
            )

            # If buy was successful, execute sell
            if buy_result:
                logger.info(f"limit_buy_order successfully placed at price: {buy_result['price']}, fastest: Order {buy_result['order_num']}")
                logger.info("Starting sell strategy...")
                sell_result = await self.execute_sell_limit_order(
                    symbol=symbol,
                    size=size,
                    price=limit_sell_price)
                if sell_result.get('success'):
                    logger.info(f"First sell order succeeded! at price: {sell_result.get('price', 'unknown')} execution time: {sell_result.get('execution_time', 0):.3f}s")
                sell_result['sell_buy_order_num'] = buy_result['order_num']
                self.all_sell_order_results.append(sell_result)
            else:
                logger.info("No successful buy orders, strategy complete")

            # Wait for remaining tasks to complete
            if self.buy_order_tasks:
                await asyncio.gather(*self.buy_order_tasks, return_exceptions=True)
            
            # Execute sell orders for all buy order that are not fastes one
            for order_resposne in self.all_executed_buy_order_results:
                if order_resposne != self.first_succesful_order:
                    sell_order_response =await self.execute_sell_limit_order(symbol=symbol, size=size, price=limit_sell_price)
                    sell_order_response['sell_buy_order_num'] = order_resposne['order_num']
                    logger.info(f"for placed buy Order {order_resposne['order_num']} sell Order placed ")
                    self.all_sell_order_results.append(sell_order_response)

            # Log all order results
            logger.info("All buy order results:")
            for order_result in self.all_executed_buy_order_results:
                logger.info(order_result)
            
            logger.info("All sell order results:")
            for order_result in self.all_sell_order_results:
                logger.info(order_result)

            return {
                "all_executed_buy_orders": self.all_executed_buy_order_results, 
                "all_executed_sell_orders": self.all_sell_order_results
                }
            # Cleanup
        except Exception as e:
            logger.error(f"Error in strategy execution: {str(e)}")
            traceback.print_exc()
        finally:
            await self.close_client()

async def main():
    import json
    try:
        # Load credentials
        with open('/root/trading_systems/kucoin_dir/config_api.json', 'r') as file:
            api_creds = json.load(file)

        # Initialize trader
        strategy = KucoinStrategyTrader(
            api_key=api_creds['api_key'],
            api_secret=api_creds['api_secret'],
            api_passphrase=api_creds['api_passphrase']
        )

        # Trading parameters
        symbol = "XRP-USDT"
        limit_buy_price = "0.2"
        limit_sell_price = "0.2"
        size = "1"
        num_orders = 4
        time_offset_ms = 10


        # Run the trading strategy
        await strategy.multiple_buy_order_offset_time(
            symbol=symbol,
            limit_buy_price=limit_buy_price,
            limit_sell_price=limit_sell_price,
            size=size,
            num_orders=num_orders,
            time_offset_ms=time_offset_ms,
        )

    except Exception as e:
        logger.error(f"Strategy execution error: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())