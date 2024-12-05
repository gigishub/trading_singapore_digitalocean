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
from kucoin_websocket_listen_DEV import KucoinWebsocketListen
from kucoin_bid_ask_order_strategy import Level2StrategyTrader
from kucoin.exceptions import KucoinAPIException

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False



LOCK_FILE = '/tmp/kucoin_trading_bid_ask_ws.lock'

# kucoin trading channel match

# 1,16,31,46 * * * * /root/trading_systems/tradingvenv/bin/python /root/trading_systems/kucoin_dir/kucoin_trading_bisd_ask.py >> /root/trading_systems/kucoin_dir/cronlogs/kucoin_trading_bisd_ask.log 2>&1

async def main():
    lock_file = acquire_lock()
    directory = '/root/trading_systems/kucoin_dir/new_pair_data_kucoin'
    path_to_save = '/root/trading_systems/kucoin_dir/kucoin_trading_data_LEVEL2'

    testing = False
    testing_pair = 'XRPUSDT'
    testing_relesease_time = datetime.now() + timedelta(seconds= 60) #'Sep 15 2021  3:00PM'
    testing_relesease_time = testing_relesease_time.replace(second=0)

    try:  

        async def execution_level2(new_pair_dict):
            logger.debug("Starting script")
            price_increase_buy = 1
            price_increase_sell = 2
            number_of_orders_buy = 8
            number_of_orders_sell = 5
            
            path_to_save_level2 = '/root/trading_systems/kucoin_dir/kucoin_trading_data_LEVEL2'
    
            api_creds = load_credetials()
            basecoin, release_date_time, datetime_to_listing_seconds = parse_new_pair_dict(new_pair_dict)
            logger.info(f'Detected new pair {new_pair_dict["pair"]} at {new_pair_dict["date_time_string"]}')
            logger.info(f"{datetime_to_listing_seconds} until listing sleep {datetime_to_listing_seconds-30}")
            await asyncio.sleep(datetime_to_listing_seconds-30)

            try:
                symbol = f'{basecoin}'
                strategy = Level2StrategyTrader(symbol, api_creds['api_key'], 
                        api_creds['api_secret'], 
                        api_creds['api_passphrase'])

                ws_levels2 = KucoinWebsocketListen(symbol, KucoinWebsocketListen.CHANNEL_LEVEL2)
                run_match = asyncio.create_task(ws_levels2.start())

                try:
                    end_time = datetime.now() + timedelta(minutes=2)
                    
                    while datetime.now() < end_time:
                        market_data = await ws_levels2.get_data()
                        
                        if market_data:
                            print(json.dumps(market_data,indent =4))
                            buy_result = await strategy.buy_first_ask_found(number_of_orders_buy, market_data,price_increase_buy )
                            if buy_result:
                                print(json.dumps(strategy.trade_data,indent=4))
                                break

                        await asyncio.sleep(0.00001)
                except asyncio.TimeoutError:
                    logger.info('No data in queue')
                                    

                finally:
                    # Save trading data and cleanup
                    strategy.save_trading_data(path_to_save_level2)
                    await ws_levels2.cleanup()
                    await strategy.close_client()

            except Exception as e:
                logger.error(f"executing strategy: {str(e)}")
                traceback.print_exc()


        if testing:
            new_pair_dict = {
                "pair": testing_pair,
                "date_time_string": testing_relesease_time.strftime('%b %d %Y %I:%M%p'),


            }
            await execution_level2(new_pair_dict)
        
        else:    
            pairs_close_to_release = check_if_for_releases(directory)
            
            tasks_to_execute = []
            for new_pair_dict in pairs_close_to_release:

                tasks_to_execute.append(asyncio.create_task(execution_level2(new_pair_dict)))
            
            await asyncio.gather(*tasks_to_execute)




    finally:
        # Synchronous operations after all async tasks are completed
        release_lock(lock_file)
        print(f'{datetime.now()} script finished V5')

def check_if_for_releases(directory):
    pairs_to_trade = []
    try:
        for filename in os.listdir(directory):
            if filename.endswith(".json"):
                with open(os.path.join(directory, filename)) as f:
                    new_pair_dict = json.load(f)
                parse_result = parse_new_pair_dict(new_pair_dict)
                if not parse_result:
                    continue
                basecoin, release_date_time, datetime_to_listing_seconds = parse_result

                if 0 < datetime_to_listing_seconds < 1200:
                    if new_pair_dict['tag'] == 'initial_listing':
                        pairs_to_trade.append(new_pair_dict)

    except Exception as e:
        logger.error(f"parsings directory for new pairs: {e}")
        traceback.print_exc()
    return pairs_to_trade


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