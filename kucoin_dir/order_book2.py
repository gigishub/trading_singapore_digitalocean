import asyncio
import aiohttp
import orjson
import logging
from datetime import datetime, timedelta
import time
from kucoin_websocket_listen_DEV import KucoinWebsocketListen
# Assume existing imports and KucoinWebsocketListen class...

# OrderBook class to maintain the order book state
class OrderBook:
    def __init__(self):
        self.bids = {}  # Price -> Size
        self.asks = {}  # Price -> Size

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

    def get_top_levels(self, depth=5):
        """
        Get the top N levels of bids and asks.
        """
        # Get top N bids (sorted descending by price)
        top_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:depth]
        # Get top N asks (sorted ascending by price)
        top_asks = sorted(self.asks.items(), key=lambda x: x[0])[:depth]
        return top_bids, top_asks

async def main():
    ws = KucoinWebsocketListen(symbol="BTC", channel=KucoinWebsocketListen.CHANNEL_LEVEL2)

    # Create an instance of OrderBook
    order_book = OrderBook()

    depth = 5  # Adjustable depth variable

    try:
        asyncio.create_task(ws.start())

        # Continuously process incoming data and update the order book
        while True:
            data = await ws.get_data()
            if data:
                # Update the order book with the new data
                order_book.update(data)
                # print(data)

                # Get the top N bids and asks
                top_bids, top_asks = order_book.get_top_levels(depth=depth)

                print(f"\nTop {depth} Bids:")
                for price, size in top_bids:
                    print(f"Price: {price}, Size: {size}")

                print(f"\nTop {depth} Asks:")
                for price, size in top_asks:
                    print(f"Price: {price}, Size: {size}")

            await asyncio.sleep(1)
    finally:
        await ws.cleanup()

if __name__ == "__main__":
    asyncio.run(main())