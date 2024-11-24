import aiohttp
import asyncio
import logging

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

async def get_price_change(symbol: str):
    api_url = f"https://api.kucoin.com/api/v1/market/stats?symbol={symbol}-USDT"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(api_url) as response:
            if response.status == 200:
                data = await response.json()
                price_change_1h = data['data']
                logger.info(f"Price change in the last 1 hour for {symbol}: {price_change_1h}")
                return price_change_1h
            else:
                logger.error(f"Failed to get price change for {symbol}: {response.status}")
                return None

async def main():
    symbol = "BTC"  # Replace with the desired symbol
    await get_price_change(symbol)

if __name__ == "__main__":
    asyncio.run(main())