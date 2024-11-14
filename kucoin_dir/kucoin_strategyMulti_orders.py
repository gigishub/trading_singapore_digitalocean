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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class KucoinStrategyTrader:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str):
        self.trading_client = kucoinHForderManager(api_key, api_secret, api_passphrase)
        self.order_tasks = []
        self.should_stop = False
        self.successful_orders = []  # Track all successful orders
        self.all_order_results = []  # Track all order results

    async def execute_staged_orders(self, 
                                    symbol: str,
                                    base_price: str,
                                    size: str,
                                    num_orders: int = 10,
                                    time_offset_ms: int = 30,
                                    price_increment: float = 0.0001):
        """
        Execute multiple buy orders with time offsets, immediately proceeding after first success
        """
        try:
            logger.info(f"Starting staged order execution: {num_orders} orders, {time_offset_ms}ms offset")
            self.should_stop = False
            self.successful_orders = []
            self.all_order_results = []

            async def place_single_order(order_num: int):
                try:
                    if self.should_stop:
                        logger.info(f"Order {order_num + 1} skipped due to stop signal")
                        order_info = {
                            "order_num": order_num + 1,
                            "price": None,
                            "send_time": None,
                            "execution_time": 0,
                            "order_id": None,
                            "success": False,
                            "message": "Skipped due to stop signal"
                        }
                        self.all_order_results.append(order_info)
                        return order_info

                    # Calculate adjusted price for this order
                    adjusted_price = float(base_price) + (price_increment * order_num)
                    price_str = f"{adjusted_price:.4f}"

                    # Wait for the appropriate time offset
                    await asyncio.sleep(time_offset_ms / 1000 * order_num)

                    if self.should_stop:
                        logger.info(f"Order {order_num + 1} skipped due to stop signal")
                        order_info = {
                            "order_num": order_num + 1,
                            "price": None,
                            "send_time": None,
                            "execution_time": 0,
                            "order_id": None,
                            "success": False,
                            "message": "Skipped due to stop signal"
                        }
                        self.all_order_results.append(order_info)
                        return order_info

                    send_time = datetime.now().strftime("%H:%M:%S.%f"[:-3])
                    logger.info(f"Placing order {order_num + 1}/{num_orders} at {send_time} - Price: {price_str}")

                    try:
                        result = await self.trading_client.place_limit_buy(
                            symbol=symbol,
                            price=price_str,
                            size=size
                        )

                        order_info = {
                            "order_num": order_num + 1,
                            "price": price_str,
                            "send_time": send_time,
                            "execution_time": result.get("execution_time", 0) if result else 0,
                            "order_id": result.get("order_id", "unknown") if result else "unknown",
                            "success": result.get("success", False) if result else False,
                            "message": result.get("message", "No response") if result else "No response"
                        }

                        self.all_order_results.append(order_info)

                        if result and result.get("success"):
                            exec_time = result.get("execution_time", 0)
                            logger.info(f"Order {order_num + 1} succeeded! Price: {price_str}, Execution time: {exec_time:.3f}ms")
                            self.should_stop = True
                            self.successful_orders.append(order_info)
                            return order_info
                        else:
                            error_msg = result.get('message') if result else 'No response'
                            logger.info(f"Order {order_num + 1} failed: {error_msg}")
                            return order_info

                    except Exception as e:
                        logger.error(f"Error in order {order_num + 1}: {str(e)}")
                        order_info = {
                            "order_num": order_num + 1,
                            "price": price_str,
                            "send_time": send_time,
                            "execution_time": 0,
                            "order_id": "unknown",
                            "success": False,
                            "message": str(e)
                        }
                        self.all_order_results.append(order_info)
                        return order_info

                except asyncio.CancelledError:
                    logger.info(f"Order {order_num + 1} was cancelled")
                    order_info = {
                        "order_num": order_num + 1,
                        "price": None,
                        "send_time": None,
                        "execution_time": 0,
                        "order_id": None,
                        "success": False,
                        "message": "Cancelled"
                    }
                    self.all_order_results.append(order_info)
                    return order_info

            # Create all order tasks
            tasks = [
                asyncio.create_task(place_single_order(i))
                for i in range(num_orders)
            ]
            
            successful_result = None

            # Use as_completed to process orders as they finish
            for coro in asyncio.as_completed(tasks):
                try:
                    result = await coro
                    if result is not None and result.get("success"):
                        successful_result = result
                        # Cancel remaining tasks
                        for task in tasks:
                            if not task.done():
                                task.cancel()
                        break
                except asyncio.CancelledError:
                    continue
                except Exception as e:
                    logger.error(f"Error processing task: {str(e)}")

            # Wait for all tasks to complete, including the ones that were cancelled
            await asyncio.gather(*[asyncio.shield(task) for task in tasks], return_exceptions=True)

            # Log all order results
            logger.info("All order results:")
            for order_result in self.all_order_results:
                logger.info(order_result)

            if successful_result:
                logger.info(f"Successfully executed order: {successful_result}")
                return successful_result
                
            logger.info("No orders were successful")
            return None

        except Exception as e:
            logger.error(f"Error in staged order execution: {str(e)}")
            return None

    async def execute_sell_after_success(self,
                                         symbol: str,
                                         buy_result: Dict,
                                         size: str,
                                         profit_margin: float = 0.01):
        """
        Execute a sell order after a successful buy and log execution summary
        """
        if not buy_result:
            logger.error("No buy result provided for sell execution")
            return None

        try:
            # Calculate sell price with profit margin
            buy_price = float(buy_result["price"])
            sell_price = buy_price * (1 + profit_margin)
            sell_price_str = f"{sell_price:.4f}"

            logger.info(f"Placing sell order at {sell_price_str} (Buy price: {buy_price})")
            
            result = await self.trading_client.place_limit_sell(
                symbol=symbol,
                price=sell_price_str,
                size=size
            )

            # Log execution summary after sell attempt
            logger.info("\n=== Execution Summary ===")
            logger.info(f"Total successful buy orders: {len(self.successful_orders)}")
            for order in self.successful_orders:
                logger.info(
                    f"Order #{order['order_num']}: "
                    f"Price: {order['price']}, "
                    f"Send Time: {order['send_time']}, "
                    f"Execution Time: {order['execution_time']}ms, "
                    f"Order ID: {order['order_id']}"
                )
            logger.info(f"Sell order result: {result}")
            logger.info("=====================\n")

            return result

        except Exception as e:
            logger.error(f"Error in sell execution: {str(e)}")
            return None

    async def close(self):
        """Cleanup resources"""
        await self.trading_client.close()


async def main():
    import json
    try:
        # Load credentials
        with open('/root/trading_systems/kucoin_dir/config_api.json', 'r') as file:
            api_creds = json.load(file)

        # Initialize trader
        trader = KucoinStrategyTrader(
            api_key=api_creds['api_key'],
            api_secret=api_creds['api_secret'],
            api_passphrase=api_creds['api_passphrase']
        )

        # Trading parameters
        symbol = "XRP-USDT"
        base_price = "0.2"
        size = "11"
        num_orders = 10
        time_offset_ms = 30
        price_increment = 0.0001
        profit_margin = 0.01

        # Execute buy orders
        logger.info("Starting buy strategy...")
        buy_result = await trader.execute_staged_orders(
            symbol=symbol,
            base_price=base_price,
            size=size,
            num_orders=num_orders,
            time_offset_ms=time_offset_ms,
            price_increment=price_increment
        )

        # If buy was successful, execute sell
        if buy_result:
            logger.info(f"Buy successful at price {buy_result['price']}, executing sell...")
            sell_result = await trader.execute_sell_after_success(
                symbol=symbol,
                buy_result=buy_result,
                size=size,
                profit_margin=profit_margin
            )
            logger.info(f"Sell order result: {sell_result}")
        else:
            logger.info("No successful buy orders, strategy complete")

        # Cleanup
        await trader.close()

    except Exception as e:
        logger.error(f"Strategy execution error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())