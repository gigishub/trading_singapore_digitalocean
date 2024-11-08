import os
import sys
import time
import logging
import asyncio
from datetime import datetime, timedelta
import json
import traceback
from kucoin.client import Client
from kucoin.exceptions import KucoinAPIException
from Kucoin_websocket_speed_update import KucoinWebSocketScraper
from hf_kucoin_order import KucoinHFTrading

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Explicitly set logger level

LOCK_FILE = '/tmp/buying_kucoin.lock'

def is_running():
    """Check if the script is already running by checking the lock file."""
    if os.path.exists(LOCK_FILE):
        with open(LOCK_FILE, 'r') as lock_file:
            pid = int(lock_file.read())
            if psutil.pid_exists(pid):
                # The process is still running
                return True
            else:
                # The process is not running; remove stale lock file
                os.remove(LOCK_FILE)
                return False
    return False

def create_lock():
    """Create a lock file to indicate the script is running."""
    with open(LOCK_FILE, 'w') as lock_file:
        lock_file.write(str(os.getpid()))
        logger.debug(f"Lock file created: {LOCK_FILE}")

def remove_lock():
    """Remove the lock file to indicate the script has finished running."""
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)

def main():
    if is_running():
        logger.info("Script is already running. Exiting.")
        sys.exit(0)

    create_lock()

    try:
        directory = '/root/trading_systems/kucoin_dir/new_pair_data_kucoin'
        percent_of_price_buy = 0.8  # setting limit order to buy n% above the retrieved price
        percent_of_price_sell = 1.2  # setting limit order to sell n% above the ACTUAL buy price NOT the retrieved price
        max_wait_time_for_execution = 0.5  # time to wait for price if passed no order execution but continues to retrieve price

        # Loop through announced pairs
        for filename in os.listdir(directory):
            if filename.endswith(".json"):
                with open(os.path.join(directory, filename)) as f:
                    new_pair_dict = json.load(f)

            try:
                # Create URL to scrape price data
                basecoin = new_pair_dict['pair'].split('USDT')[0]

                # Get the time remaining to listing in seconds
                date_time_dict = parse_date_time_string(new_pair_dict['date_time_string'])
                release_date_time_str = date_time_dict['formatted_string']
                release_date_time = datetime.strptime(release_date_time_str, '%Y-%m-%d %H:%M:%S')
                datetime_to_listing_seconds = (release_date_time - datetime.now()).total_seconds()

            except Exception as e:
                logger.error(f"Error when parsing date time string:\n {e}")
                traceback.print_exc()
                continue

            # Check if listing is close to start
            if 0 < datetime_to_listing_seconds < 1200:
                try:
                    if datetime.now() > release_date_time:
                        logger.info('Release time has passed')
                        break
                except Exception as e:
                    logger.error(f"Checking release time:\n {e}")

                logger.info(f'Detected new pair {new_pair_dict["pair"]} at {new_pair_dict["date_time_string"]}')

                try:
                    with open('/root/trading_systems/kucoin_dir/config_api.json') as config_file:
                        config = json.load(config_file)

                    client = KucoinHFTrading(
                        api_key=config['api_key'],
                        api_secret=config['api_secret'],
                        api_passphrase=config['api_passphrase'],
                        debug=False  # Set to False for production
                    )

                except Exception as e:
                    logger.error(f"Error initializing KuCoin HF trading class:\n {e}")

                # Example usage
                async def scraper(client=client, basecoin=basecoin, percent_of_price_buy=percent_of_price_buy, percent_of_price_sell=percent_of_price_sell):
                    symbol = basecoin
                    scraper = KucoinWebSocketScraper()
                    try:
                        websocket_release_price = await scraper.get_price_websocket(symbol, max_wait_time=2, release_time=release_date_time)

                        if websocket_release_price and datetime.now() < release_date_time + timedelta(seconds=max_wait_time_for_execution):
                            # Buying execution
                            try:
                                size, decimal_to_round = order_size_and_rounding(websocket_release_price)
                                response_buy_order = await place_buy_limit_order(websocket_release_price, decimal_to_round, size, client, basecoin, percent_of_price_buy)
                                logger.debug(f'Buy order response: {response_buy_order[0]}')
                                limit_order_buy_price = response_buy_order[1]
                                logger.debug(f'Buy order price: {limit_order_buy_price}')
                            except Exception as e:
                                logger.error(f'Error in buying execution: {e}')

                            try:
                                # Using limit order buy price as reference to sell
                                response_sell_order = await place_sell_limit_order(limit_order_buy_price, size, client, basecoin, percent_of_price_sell)
                                logger.debug(f'Sell order response: {response_sell_order}')
                            except Exception as e:
                                logger.error(f'Error in selling execution: {e}')

                        else:
                            logger.warning('Price not found or timed out')

                    finally:
                        await scraper.cleanup()

                # Run the asynchronous scraper
                asyncio.run(scraper())

        logger.info(f'{datetime.now().strftime("%H:%M:%S")} loop finished sleeping 1 second before exit')
        logger.info('Branch update No. 5')
        time.sleep(1)

    finally:
        remove_lock()

if __name__ == "__main__":
    main()