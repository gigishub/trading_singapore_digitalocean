import sys
import os
import fcntl  # For file locking
import logging
import asyncio
import aiohttp
import time
import orjson
from datetime import datetime, timedelta
import traceback
import json
import re
from kucoin_websocket_collectionV4 import KucoinWebsocketlisten

from kucoin_order_strategyV5 import KucoinStrategyTrader
from kucoin.exceptions import KucoinAPIException

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False


LOCK_FILE = '/tmp/kucoin.lock'

async def main():
    lock_file = acquire_lock()
    try:
        logger.debug("Starting script")
        testing = True  
        directory = '/root/trading_systems/kucoin_dir/new_pair_data_kucoin'
        testing_time_offset = 2  # Time offset for testing
        
        api_creds_dict = load_credetials()

        if not testing:
            for filename in os.listdir(directory):
                if filename.endswith(".json"):
                    with open(os.path.join(directory, filename)) as f:
                        new_pair_dict = json.load(f)
                    parse_result = parse_new_pair_dict(new_pair_dict)
                    if not parse_result:
                        continue
                    basecoin, release_date_time, datetime_to_listing_seconds = parse_result

                    if 0 < datetime_to_listing_seconds < 1200:
                        logger.info(f'Detected new pair {new_pair_dict["pair"]} at {new_pair_dict["date_time_string"]}')
                        while datetime.now() < release_date_time - timedelta(seconds=0.1):
                            logger.info('not yet')
                            await asyncio.sleep(0.09)
                            logger.info('now')
                        try:

                            ...


                        except Exception as e:
                            logger.error(f"Strategy execution error: {str(e)}")
                            traceback.print_exc()
                            continue
                        finally:
                                ...

                        logger.debug('Break after detecting pair')
                        break

        else:
            # Testing mode
            basecoin = 'XRP'  # Test symbol
            price = 0.5
            number_of_orders = 2
            price_increase_buy =-3
            price_increase_sell = 2


            release_date_time = datetime.now() + timedelta(seconds=testing_time_offset)
            release_date_time = release_date_time.replace( microsecond=0)
            api_creds_dict = load_credetials()
            logger.info(f'Testing mode: {basecoin} at {release_date_time}')

            strategy_object = KucoinStrategyTrader(
                                api_creds_dict['api_key'],
                                api_creds_dict['api_secret'],
                                api_creds_dict['api_passphrase']
                                )
            sleep_time = (release_date_time - timedelta(seconds=1)).timestamp() - time.time()
            logger.info(f'sleeping for {sleep_time}')
            await asyncio.sleep(sleep_time)


            # # Assuming release_date_time is already defined
            # order_times = [
            #     release_date_time - timedelta(seconds=0.15),
            #     release_date_time - timedelta(seconds=0.10),
            #     release_date_time - timedelta(seconds=0.05),
            #     release_date_time
            # ]

            # for i, order_time in enumerate(order_times, start=1):
            #     sleep_time = (order_time - datetime.now()).total_seconds()
            #     if sleep_time > 0:
            #         time.sleep(sleep_time)
            #     task_trade2 = asyncio.create_task(trade_match(
            #                         strategy_object,
            #                         basecoin,
            #                         price,
            #                         number_of_orders,
            #                         price_increase_buy,
            #                         price_increase_sell
            #                     ))

                
            #     logger.info(f'Order {i} order sent')


                # Prepare the order times
            order_times = [
                release_date_time - timedelta(seconds=0.15),
                release_date_time - timedelta(seconds=0.10),
                release_date_time - timedelta(seconds=0.05),
                release_date_time
            ]

            # List to hold the tasks
            tasks = []

            # Schedule each trade
            for i, order_time in enumerate(order_times, start=1):
                task = asyncio.create_task(schedule_trade(
                    i,
                    order_time,
                    strategy_object,
                    basecoin,
                    price,
                    number_of_orders,
                    price_increase_buy,
                    price_increase_sell
                ))
                tasks.append(task)

            # Await all tasks to ensure they complete
            await asyncio.gather(*tasks)
            logger.info('All orders have been sent')
            try:

                ...

            except Exception as e:
                logger.error(f"Strategy execution error: {str(e)}")
                traceback.print_exc()

    finally:
        await strategy_object.close_client()
        release_lock(lock_file)
        print(f'{datetime.now()} script finished V5')



# Don't forget to include the schedule_trade function outside of main()
async def schedule_trade(i, order_time, strategy_object, basecoin, price, number_of_orders, price_increase_buy, price_increase_sell):
    sleep_time = (order_time - datetime.now()).total_seconds()
    if sleep_time > 0:
        await asyncio.sleep(sleep_time)
    else:
        # If the sleep time is negative, the order_time is in the past
        pass

    logger.info(f'Order {i} sending at {datetime.now()}')
    await trade_match(
        strategy_object,
        basecoin,
        price,
        number_of_orders,
        price_increase_buy,
        price_increase_sell
    )
    logger.info(f'Order {i} sent at {datetime.now()}')

async def trade_match(strategy_object, symbol, price, number_of_orders, price_increase_buy, price_increase_sell):
    buy_results = await strategy_object.multiple_buy_orders_percent_dif(
        symbol=symbol+ '-USDT',
        base_price=price,
        num_orders=number_of_orders,
        percentage_difference= price_increase_buy
    )
    sell_results = await strategy_object.multiple_sell_orders_percent_dif(
        symbol=symbol+ '-USDT',
        base_price=price,
        num_orders=number_of_orders,
        percentage_difference= price_increase_sell
    )
    logger.info(f'Buy results: {json.dumps(buy_results,indent=4)}')
    logger.info(f'Sell results: {json.dumps(sell_results,indent=4)}')
    return buy_results, sell_results

def acquire_lock():
    """Acquire a file lock to prevent multiple instances from running."""
    try:
        lock_file = open(LOCK_FILE, 'w')
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_file.write(str(os.getpid()))
        lock_file.flush()
        logger.debug("Lock acquired.")
        return lock_file
    except BlockingIOError:
        logger.warning("Another instance is running. Exiting.")
        sys.exit(0)
    except Exception as e:
        logger.warning(f"Unexpected error acquiring lock: {e}")
        sys.exit(1)

def release_lock(lock_file):
    """Release the file lock."""
    try:
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()
        logger.debug("Lock released.")
    except Exception as e:
        logger.error(f"Unexpected error releasing lock: {e}")


def parse_new_pair_dict(new_pair_dict):
    """Parse new pair data to extract basecoin and release time."""
    try:
        basecoin = new_pair_dict['pair'].split('USDT')[0]
        date_time_dict = parse_date_time_string(new_pair_dict['date_time_string'])
        if 'error' in date_time_dict:
            logger.error(date_time_dict['error'])
            return None
        release_date_time_str = date_time_dict['formatted_string']
        release_date_time = datetime.strptime(release_date_time_str, '%Y-%m-%d %H:%M:%S')
        datetime_to_listing_seconds = (release_date_time - datetime.now()).total_seconds()
        return basecoin, release_date_time, datetime_to_listing_seconds
    except Exception as e:
        logger.error(f"Error parsing date time string:\n {e}")
        traceback.print_exc()
        return None


def load_credetials():
    """Initialize the KuCoin client."""
    try:
        with open('/root/trading_systems/kucoin_dir/config_api.json') as config_file:
            config = json.load(config_file)

        return config
    except Exception as e:
        logger.error(f"loding credentials:\n {e}")
        return None


def parse_date_time_string(date_time_string):
    """Parse a date-time string into a dictionary."""
    import re
    date_time_string = date_time_string.replace(',', '')
    date_time_string = re.sub(r'\s+', ' ', date_time_string.strip())
    formats = [
        "%b %d %Y %I:%M:%S%p",
        "%b %d %Y %I:%M%p",
        "%b %d %Y %I%p",
        "%B %d %Y %I:%M:%S%p",
        "%B %d %Y %I:%M%p",
        "%B %d %Y %I%p",
    ]

    for fmt in formats:
        try:
            target_datetime = datetime.strptime(date_time_string, fmt)
            result = {
                'year': target_datetime.year,
                'month': target_datetime.month,
                'day': target_datetime.day,
                'hour': target_datetime.hour,
                'minute': target_datetime.minute,
                'second': target_datetime.second,
                'day_of_week': target_datetime.strftime('%a').lower(),
                'formatted_string': target_datetime.strftime('%Y-%m-%d %H:%M:%S')
            }
            return result
        except ValueError:
            continue
    return {'error': f"Date time string '{date_time_string}' does not match any known formats"}

if __name__ == '__main__':
    asyncio.run(main())