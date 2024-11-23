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
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

LOCK_FILE = '/tmp/kucoin_trading_level2_ask_ws.lock'

# kucoin trading channel level2 ask
#1,16,31,46 * * * * /root/trading_systems/tradingvenv/bin/python /root/trading_systems/kucoin_dir/kucoin_trading_level2_ask.py >> /root/trading_systems/kucoin_dir/cronlogs/kucoin_trading_level2_ask.log 2>&1

async def main():
    lock_file = acquire_lock()
    try:
        testing = False  
        directory = '/root/trading_systems/kucoin_dir/new_pair_data_kucoin'
        price_increase_buy = 7
        price_increase_sell = 4

        number_of_orders = 2
        testing_time_offset= 2
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
                        logger.info(f"{datetime_to_listing_seconds} until listing sleep {datetime_to_listing_seconds-30}")
                        await asyncio.sleep(datetime_to_listing_seconds-30)
                        try:

                            symbol = f'{basecoin}-USDT'
                            strategy = KucoinStrategyTrader(api_creds_dict['api_key'], api_creds_dict['api_secret'], api_creds_dict['api_passphrase'])
                            ws_level2 = KucoinWebsocketlisten(basecoin, channel= 'level2')

                            ask_price = 0
                            ask_price_order_placed = False

                            bid_price = 0
                            bid_price_order_placed = False

                            async for data in ws_level2.continuous_data_stream():

                                ask_bid_dict= data.get('changes', [])
                                if ask_bid_dict.get('asks'):
                                    try:
                                        ask_price = float(ask_bid_dict.get('asks')[0][0])
                                        logger.info(f"Asks: {ask_bid_dict.get('asks')}")
                                    except Exception as e:
                                        logger.error(f"Error getting ask price: {e}")
                                        traceback.print_exc()
                                        continue
                                if ask_price != 0 and not ask_price_order_placed and datetime.now() < release_date_time+timedelta(seconds=0.1):
                                    ask_price_order_placed = True
                                    logger.info(f"Ask used for buying : {ask_price}")
                                    trade_ask_task = asyncio.create_task(trade_ask(strategy, symbol, ask_price, number_of_orders, price_increase_buy, price_increase_sell))

                                if ask_bid_dict.get('bids'):
                                    try:
                                        bid_price = float(ask_bid_dict.get('bids')[0][0])
                                        logger.info(f"bids: {ask_bid_dict.get('bids')}")
                                    except Exception as e: 
                                        logger.error(f"Error getting bid price: {e}")
                                        traceback.print_exc()
                                        continue

                                if bid_price != 0 and not bid_price_order_placed and datetime.now() < release_date_time+timedelta(seconds=0.1):
                                    try:
                                        bid_price_order_placed = True
                                        logger.info(f"Bid used for selling : {bid_price}")
                                        trade_bid_task = asyncio.create_task(trade_ask(strategy, symbol, bid_price, number_of_orders, price_increase_buy, price_increase_sell))

                                    except Exception as e:
                                        logger.error(f"Error getting bid price: {e}")
                                        traceback.print_exc()
                                        continue

                                if (ask_price_order_placed and bid_price_order_placed) or datetime.now() > release_date_time+timedelta(seconds=0.3):
                                    logger.info(f"waiting for orders to finish")
                                    placed_order_tasks = [trade_ask_task, trade_bid_task]
                                    await asyncio.wait(placed_order_tasks, return_when=asyncio.ALL_COMPLETED)
                                    logger.info(f"Orders finished move to cleanup")
                                    await strategy.close_client()
                                    logger.info(f"Break after placing orders or timed out")
                                    break





                        except Exception as e:
                            logger.error(f"Strategy execution error: {str(e)}")
                            traceback.print_exc()
                            continue
                        finally:
                            await strategy.close_client()

                        logger.debug('Break after detecting pair')
                        break

        else:
            # Testing mode
            basecoin = 'XRP'  # Test symbol
            release_date_time = datetime.now() + timedelta(seconds=testing_time_offset)
            release_date_time = release_date_time.replace( microsecond=0)

            try:
                symbol = f'{basecoin}-USDT'
                strategy = KucoinStrategyTrader(api_creds_dict['api_key'], api_creds_dict['api_secret'], api_creds_dict['api_passphrase'])
                ws_level2 = KucoinWebsocketlisten(basecoin, channel= 'level2')

                ask_price = 0

                async for data in ws_level2.continuous_data_stream():

                    ask_bid_dict= data.get('changes', [])
                    if ask_bid_dict.get('asks'):
                        try:
                            ask_price = float(ask_bid_dict.get('asks')[0][0])
                            logger.info(f"Ask Price retrived : {ask_price}")
                        except Exception as e:
                            logger.error(f"Error getting ask price: {e}")
                            traceback.print_exc()
                            continue

                    if ask_price != 0:
                        buy_results = await strategy.multiple_buy_orders_percent_dif(
                            symbol=symbol,
                            base_price=ask_price,
                            num_orders=number_of_orders,
                            percentage_difference= price_increase_buy
                        )
                        sell_results = await strategy.multiple_sell_orders_percent_dif(
                            symbol=symbol,
                            base_price=ask_price,
                            num_orders=number_of_orders,
                            percentage_difference= price_increase_sell
                        )
                        logger.info(f"Buy Order Results:{json.dumps(buy_results, indent=4)}")
                        logger.info(f"Sell Order Results:{json.dumps(sell_results, indent=4)}")

                        await strategy.close_client()
                        break

                    #print('ask',ask_bid_dict.get('asks'))
                    if ask_bid_dict.get('bids'):
                        logger.info(f"bids: {ask_bid_dict.get('bids')}")




            except Exception as e:
                logger.error(f"Strategy execution error: {str(e)}")
                traceback.print_exc()

    finally:
        release_lock(lock_file)
        print(f'{datetime.now()} script finished V5')


async def trade_ask(strategy_object, symbol, ask_price, number_of_orders, price_increase_buy, price_increase_sell):
    buy_results = await strategy_object.multiple_buy_orders_percent_dif(
        symbol=symbol,
        base_price=ask_price,
        num_orders=number_of_orders,
        percentage_difference= price_increase_buy
    )
    sell_results = await strategy_object.multiple_sell_orders_percent_dif(
        symbol=symbol,
        base_price=ask_price,
        num_orders=number_of_orders,
        percentage_difference= price_increase_sell
    )
    logger.info(f"Buy Order Results:{json.dumps(buy_results, indent=4)}")
    logger.info(f"Sell Order Results:{json.dumps(sell_results, indent=4)}")




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