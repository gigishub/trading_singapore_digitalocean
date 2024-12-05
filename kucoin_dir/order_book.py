import asyncio
import aiohttp
import orjson
import logging
from datetime import datetime, timedelta
import time
from kucoin_websocket_listen_DEV import KucoinWebsocketListen
# Your existing websocket listener code (KucoinWebsocketListen class)...

# OrderBook class to maintain the order book state
class OrderBook:
    def __init__(self):
        self.bids = {}
        self.asks = {}
        self.sequence = None

    def update(self, data):
        """
        Update the order book with new data.
        """
        changes = data.get('changes', {})

        # Update bids
        for update in changes.get('bids', []):
            price = float(update[0])
            size = float(update[1])
            if size == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = size

        # Update asks
        for update in changes.get('asks', []):
            price = float(update[0])
            size = float(update[1])
            if size == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = size

    def get_top_of_book(self):
        """
        Get the best bid and best ask prices.
        """
        best_bid = max(self.bids.keys()) if self.bids else None
        best_ask = min(self.asks.keys()) if self.asks else None
        return best_bid, best_ask

async def main():
    ws = KucoinWebsocketListen(symbol="BTC", channel=KucoinWebsocketListen.CHANNEL_LEVEL2)

    # Create an instance of OrderBook
    order_book = OrderBook()

    try:
        asyncio.create_task(ws.start())

        # Continuously process incoming data and update the order book
        while True:
            data = await ws.get_data()
            print(data)
            if data:
                # Update the order book with the new data
                order_book.update(data)

                # Get the best bid and ask prices
                bid, ask = order_book.get_top_of_book()
                print(f"Best Bid: {bid}, Best Ask: {ask}")

            await asyncio.sleep(1)
    finally:
        await ws.cleanup()

if __name__ == "__main__":
    asyncio.run(main())