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
# Set the logger level
logger.setLevel(logging.INFO) 

class KucoinStrategyTrader:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str):
        self.trading_client = kucoinHForderManager(api_key, api_secret, api_passphrase)
        self.order_tasks = []
        self.should_stop = False
    
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
            
            async def place_single_order(order_num: int):
                if self.should_stop:
                    logger.info(f"Order {order_num} skipped due to stop signal")
                    return None
                    
                # Calculate adjusted price for this order
                adjusted_price = float(base_price) + (price_increment * order_num)
                price_str = f"{adjusted_price:.4f}"
                
                # Wait for the appropriate time offset
                await asyncio.sleep(time_offset_ms / 1000 * order_num)
                
                if self.should_stop:
                    return None
                    
                order_time = time.time()
                logger.info(f"Placing order {order_num + 1}/{num_orders} at {order_time:.3f} - Price: {price_str}")
                
                try:
                    result = await self.trading_client.place_limit_buy(
                        symbol=symbol,
                        price=price_str,
                        size=size
                    )
                    
                    if result and result.get("success"):
                        logger.info(f"Order {order_num + 1} succeeded! Price: {price_str}")
                        self.should_stop = True  # Signal other tasks to stop
                        return {
                            "order_num": order_num + 1,
                            "price": price_str,
                            "time": order_time
                        }
                    else:
                        error_msg = result.get('message') if result else 'No response'
                        logger.info(f"Order {order_num + 1} failed: {error_msg}")
                        return None
                        
                except Exception as e:
                    logger.error(f"Error in order {order_num + 1}: {str(e)}")
                    return None

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
                    if result is not None:
                        successful_result = result
                        # Cancel remaining tasks
                        for task in tasks:
                            if not task.done():
                                task.cancel()
                        break
                        
                except asyncio.CancelledError:
                    # This is expected for cancelled tasks
                    continue
                except Exception as e:
                    logger.error(f"Error processing task: {str(e)}")

            # Clean up remaining tasks without waiting for them
            for task in tasks:
                if not task.done():
                    task.cancel()

            if successful_result:
                logger.info(f"Successfully executed order: {successful_result}")
                return successful_result
                
            logger.info("No orders were successful")
            return None

        except Exception as e:
            logger.error(f"Error in staged order execution: {str(e)}")
            return None
        

        
    async def execute_staged_order_working_3(self, 
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
            
            async def place_single_order(order_num: int):
                if self.should_stop:
                    logger.info(f"Order {order_num} skipped due to stop signal")
                    return None
                    
                # Calculate adjusted price for this order
                adjusted_price = float(base_price) + (price_increment * order_num)
                price_str = f"{adjusted_price:.4f}"
                
                # Wait for the appropriate time offset
                await asyncio.sleep(time_offset_ms / 1000 * order_num)
                
                if self.should_stop:
                    return None
                    
                order_time = time.time()
                logger.info(f"Placing order {order_num + 1}/{num_orders} at {order_time:.3f} - Price: {price_str}")
                
                try:
                    result = await self.trading_client.place_limit_buy(
                        symbol=symbol,
                        price=price_str,
                        size=size
                    )
                    
                    if result and result.get("success"):
                        logger.info(f"Order {order_num + 1} succeeded! Price: {price_str}")
                        self.should_stop = True  # Signal other tasks to stop
                        return {
                            "order_num": order_num + 1,
                            "price": price_str,
                            "time": order_time
                        }
                    else:
                        error_msg = result.get('message') if result else 'No response'
                        logger.info(f"Order {order_num + 1} failed: {error_msg}")
                        return None
                        
                except Exception as e:
                    logger.error(f"Error in order {order_num + 1}: {str(e)}")
                    return None

            # Create all order tasks
            tasks = [
                asyncio.create_task(place_single_order(i))
                for i in range(num_orders)
            ]
            
            # Use as_completed to process orders as they finish
            for next_completed in asyncio.as_completed(tasks):
                try:
                    result = await next_completed
                    if result is not None:
                        # We got a successful order! Cancel all pending tasks
                        for task in tasks:
                            if not task.done():
                                task.cancel()
                        
                        # Wait for cancellations without blocking on their results
                        asyncio.create_task(asyncio.gather(*tasks, return_exceptions=True))
                        
                        logger.info(f"Successfully executed order: {result}")
                        return result
                        
                except asyncio.CancelledError:
                    # This is expected for cancelled tasks
                    continue
                except Exception as e:
                    logger.error(f"Error processing completed task: {str(e)}")

            logger.info("No orders were successful")
            return None

        except Exception as e:
            logger.error(f"Error in staged order execution: {str(e)}")
            return None

    async def execute_staged_orders_2working(self, 
                                symbol: str,
                                base_price: str,
                                size: str,
                                num_orders: int = 10,
                                time_offset_ms: int = 30,
                                price_increment: float = 0.0001):
        """
        Execute multiple buy orders with time offsets, stopping when one succeeds
        """
        try:
            logger.info(f"Starting staged order execution: {num_orders} orders, {time_offset_ms}ms offset")
            self.should_stop = False
            success_event = asyncio.Event()
            
            async def place_single_order(order_num: int):
                try:
                    if self.should_stop:
                        logger.info(f"Order {order_num} skipped due to stop signal")
                        return None
                        
                    # Calculate adjusted price for this order
                    adjusted_price = float(base_price) + (price_increment * order_num)
                    price_str = f"{adjusted_price:.4f}"
                    
                    # Wait for the appropriate time offset
                    await asyncio.sleep(time_offset_ms / 1000 * order_num)
                    
                    if self.should_stop:
                        return None
                        
                    order_time = time.time()
                    logger.info(f"Placing order {order_num + 1}/{num_orders} at {order_time:.3f} - Price: {price_str}")
                    
                    result = await self.trading_client.place_limit_buy(
                        symbol=symbol,
                        price=price_str,
                        size=size
                    )
                    
                    if result and result.get("success"):  # Add null check here
                        logger.info(f"Order {order_num + 1} succeeded! Price: {price_str}")
                        self.should_stop = True
                        success_event.set()
                        return {
                            "order_num": order_num + 1,
                            "price": price_str,
                            "time": order_time
                        }
                    else:
                        error_msg = result.get('message') if result else 'No response'
                        logger.info(f"Order {order_num + 1} failed: {error_msg}")
                        return None
                        
                except Exception as e:
                    logger.error(f"Error in order {order_num + 1}: {str(e)}")
                    return None

            # Create and store all order tasks
            self.order_tasks = [
                asyncio.create_task(place_single_order(i))
                for i in range(num_orders)
            ]

            # Wait for either all orders to complete or a success
            done, pending = await asyncio.wait(
                self.order_tasks,
                return_when=asyncio.FIRST_COMPLETED if success_event.is_set() else asyncio.ALL_COMPLETED
            )

            # If we got a success, cancel remaining orders
            if success_event.is_set():
                logger.info("Success detected - canceling remaining orders")
                for task in pending:
                    task.cancel()
                
                # Wait for cancellations to complete
                await asyncio.gather(*pending, return_exceptions=True)
            
            # Process completed tasks
            successful_orders = []
            for task in done:
                try:
                    result = task.result()
                    if result is not None:
                        successful_orders.append(result)
                except Exception as e:
                    logger.error(f"Error processing task result: {str(e)}")

            if successful_orders:
                logger.info(f"Found successful order: {successful_orders[0]}")
                return successful_orders[0]
            
            logger.info("No orders were successful")
            return None

        except Exception as e:
            logger.error(f"Error in staged order execution: {str(e)}")
            return None


    async def execute_staged_orders1(self, 
                                  symbol: str,
                                  base_price: str,
                                  size: str,
                                  num_orders: int = 10,
                                  time_offset_ms: int = 30,
                                  price_increment: float = 0.0001):
        """
        Execute multiple buy orders with time offsets, stopping when one succeeds
        
        Args:
            symbol: Trading pair symbol
            base_price: Starting price for orders
            size: Order size
            num_orders: Number of orders to place
            time_offset_ms: Time offset between orders in milliseconds
            price_increment: Price increment between orders
        """
        try:
            logger.info(f"Starting staged order execution: {num_orders} orders, {time_offset_ms}ms offset")
            self.should_stop = False
            success_event = asyncio.Event()
            
            async def place_single_order(order_num: int):
                try:
                    if self.should_stop:
                        logger.info(f"Order {order_num} skipped due to stop signal")
                        return None
                        
                    # Calculate adjusted price for this order
                    adjusted_price = float(base_price) + (price_increment * order_num)
                    price_str = f"{adjusted_price:.4f}"
                    
                    # Wait for the appropriate time offset
                    await asyncio.sleep(time_offset_ms / 1000 * order_num)
                    
                    if self.should_stop:
                        return None
                        
                    order_time = time.time()
                    logger.info(f"Placing order {order_num + 1}/{num_orders} at {order_time:.3f} - Price: {price_str}")
                    
                    result = await self.trading_client.place_limit_buy(
                        symbol=symbol,
                        price=price_str,
                        size=size
                    )
                    
                    if result["success"]:
                        logger.info(f"Order {order_num + 1} succeeded! Price: {price_str}")
                        self.should_stop = True
                        success_event.set()
                        return {
                            "order_num": order_num + 1,
                            "price": price_str,
                            "time": order_time
                        }
                    else:
                        logger.info(f"Order {order_num + 1} failed: {result.get('message')}")
                        return None
                        
                except Exception as e:
                    logger.error(f"Error in order {order_num + 1}: {str(e)}")
                    return None

            # Create and store all order tasks
            self.order_tasks = [
                asyncio.create_task(place_single_order(i))
                for i in range(num_orders)
            ]

            # Wait for either a successful order or all orders to complete
            done, pending = await asyncio.wait(
                self.order_tasks,
                return_when=asyncio.FIRST_COMPLETED
            )

            # If we got a success, cancel remaining orders
            if success_event.is_set():
                logger.info("Success detected - canceling remaining orders")
                for task in pending:
                    task.cancel()
                
                # Wait for cancellations to complete
                await asyncio.gather(*pending, return_exceptions=True)
                
                # Find the successful order
                successful_orders = [task.result() for task in done if task.result() is not None]
                if successful_orders:
                    return successful_orders[0]
            
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
        Execute a sell order after a successful buy
        
        Args:
            symbol: Trading pair symbol
            buy_result: Successful buy order result
            size: Order size
            profit_margin: Desired profit margin (e.g., 0.01 for 1%)
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

            if result["success"]:
                logger.info(f"Sell order placed successfully at {sell_price_str}")
            else:
                logger.error(f"Sell order failed: {result.get('message')}")
            
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