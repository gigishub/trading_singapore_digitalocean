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
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s [%(instance_id)s]',
    datefmt='%Y-%m-%d %H:%M:%S',
    extra={
        'instance_id': None
    }
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
        self.instance_id = str(uuid.uuid4())[:8]
        self.logger = logging.getLogger(__name__ + f"[{self.instance_id}]")
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s')
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)


    async def execute_staged_orders(self, symbol: str, order_price: str, size: str, num_orders: int = 10, time_offset_ms: int = 30, price_increment: float = None):
        try:
            self.logger.info(f"Starting staged order execution: {num_orders} orders, {time_offset_ms}ms offset")

            async def place_single_order(order_num: int):
                try:
                    # if i want to adjust the price with increment
                    #price_str = str(float(base_price) + price_increment * order_num) if price_increment else base_price
                    price_str = str(order_price)
                    # Wait for the appropriate time offset
                    # Apply time offset only for orders after the first one
                    if order_num > 0:
                        await asyncio.sleep(time_offset_ms / 1000 * order_num)

                    if self.should_stop:
                        return None

                    order_sent_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    self.logger.info(f"Placing order {order_num + 1}/{num_orders} at {order_sent_time} - Price: {price_str}")

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
                            self.logger.info(f"Buy Order {order_num + 1} succeeded! Price: {price_str}, Execution time: {exec_time:.3f}s")
                            self.should_stop = True  # Signal other tasks to stop placing orders
                            return order_info
                        else:
                            error_msg = result.get('message') if result else 'No response'
                            self.logger.info(f"Order {order_num + 1} failed: {error_msg}")
                            return order_info

                    except Exception as e:
                        self.logger.error(f"Error in order {order_num + 1}: {str(e)}")
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
                    self.logger.info(f"Order {order_num + 1} was cancelled before placement")
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
                        self.first_succesful_order = result
                        self.should_stop = True
                        break
                if successful_result or not pending:
                    break

            return successful_result

        except Exception as e:
            self.logger.error(f"Error in staged order execution: {str(e)}")
            return None

    async def execute_sell_limit_order(self, symbol: str, price: str, size: str):
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
                return result

        except Exception as e:
            self.logger.error(f"Error in sell order execution: {str(e)}")
            return result

    async def close_client(self):
        await self.trading_client.close()

    async def multiple_buy_order_offset_time(self, symbol: str, limit_buy_price: str, limit_sell_price: str, size: str, num_orders: int, time_offset_ms: int):
        try:
            self.logger.info("Starting buy strategy...")
            buy_result = await self.execute_staged_orders(
                symbol=symbol,
                order_price=limit_buy_price,
                size=size,
                num_orders=num_orders,
                time_offset_ms=time_offset_ms,
            )

            if buy_result:
                self.logger.info(f"limit_buy_order successfully placed at price: {buy_result['price']}, fastest: Order {buy_result['order_num']}")
                self.logger.info("Starting sell strategy...")
                sell_result = await self.execute_sell_limit_order(
                    symbol=symbol,
                    size=size,
                    price=limit_sell_price)
                if sell_result.get('success'):
                    self.logger.info(f"First sell order succeeded! at price: {sell_result.get('price', 'unknown')} execution time: {sell_result.get('execution_time', 0):.3f}s")
                sell_result['sell_buy_order_num'] = buy_result['order_num']
                self.all_sell_order_results.append(sell_result)
            else:
                self.logger.info("No successful buy orders, strategy complete")

            if self.buy_order_tasks:
                await asyncio.gather(*self.buy_order_tasks, return_exceptions=True)

            for order_resposne in self.all_executed_buy_order_results:
                if order_resposne != self.first_succesful_order:
                    sell_order_response =await self.execute_sell_limit_order(symbol=symbol, size=size, price=limit_sell_price)
                    sell_order_response['sell_buy_order_num'] = order_resposne['order_num']
                    self.logger.info(f"for placed buy Order {order_resposne['order_num']} sell Order placed ")
                    self.all_sell_order_results.append(sell_order_response)

            self.logger.info("All buy order results:")
            for order_result in self.all_executed_buy_order_results:
                self.logger.info(order_result)

            self.logger.info("All sell order results:")
            for order_result in self.all_sell_order_results:
                self.logger.info(order_result)

        except Exception as e:
            self.logger.error(f"Error in strategy execution: {str(e)}")
            traceback.print_exc()
        finally:
            await self.close_client()

async def main():
    import json
    try:
        # Load credentials
        with open('/root/trading_systems/kucoin_dir/config_api.json', 'r') as file:
            api_creds = json.load(file)

        # Create and run multiple instances of the trading strategy
        strategies = [
            KucoinStrategyTrader(
                api_key=api_creds['api_key'],
                api_secret=api_creds['api_secret'],
                api_passphrase=api_creds['api_passphrase']
            ) for _ in range(3)
        ]

        await asyncio.gather(
            *[strategy.multiple_buy_order_offset_time(
                symbol="XRP-USDT",
                limit_buy_price="0.2",
                limit_sell_price="0.2",
                size="1",
                num_orders=4,
                time_offset_ms=10
            ) for strategy in strategies]
        )

    except Exception as e:
        logging.error(f"Strategy execution error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())