import asyncio
import aiohttp
import orjson
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import os
import json
import traceback

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

class Bitget_save_ws_data:
    CHANNEL_TICKER = "ticker"
    CHANNEL_TRADE = "trade"
    CHANNEL_DEPTH5 = "books5"
    
    def __init__(self, 
                 symbol: str, 
                 release_time: datetime, 
                 duration_minutes: float,
                 saving_path: str,
                 channel: str = "ticker",
                 pre_release_seconds: int = 30):
        
        # Basic configuration
        self.symbol = symbol
        self.channel = channel
        self.ws_url = "wss://ws.bitget.com/v2/ws/public"
        
        # WebSocket state
        self.ws_connection = None
        self.ws_session = None
        self.is_running = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        
        # Data management
        self.queue = asyncio.Queue()
        self.stored_data = []
        self.messages_processed = 0
        self.saving_path = saving_path
        
        # Timing control
        self.release_time = release_time
        self.start_time = release_time - timedelta(seconds=pre_release_seconds)
        self.end_time = release_time + timedelta(minutes=duration_minutes)
        self.collection_started = False
        self.collection_ended = False
        
        # Heartbeat management
        self.last_message_time = datetime.now()
        self.heartbeat_interval = 25  # Send ping slightly before 30s timeout
        self.max_no_message_time = 60  # Reconnect if no messages for 60 seconds

        logger.info(f"Initialized collector for {symbol} - Start: {self.get_formatted_time(self.start_time)}, "
                   f"Release: {self.get_formatted_time(release_time)}, "
                   f"End: {self.get_formatted_time(self.end_time)}")

    @staticmethod
    def get_formatted_time(dt: datetime = None) -> str:
        """Return formatted time string with microseconds"""
        if dt is None:
            dt = datetime.now()
        return dt.strftime('%H:%M:%S.%f')[:-2]

    def get_subscription_data(self) -> dict:
        """Generate subscription data based on channel and symbol"""
        formatted_symbol = f"{self.symbol}USDT"
        
        channel_map = {
            self.CHANNEL_TICKER: "ticker",
            self.CHANNEL_TRADE: "trade",
            self.CHANNEL_DEPTH5: "books5"
        }
        
        return {
            "op": "subscribe",
            "args": [{
                "instType": "SPOT",
                "channel": channel_map.get(self.channel, self.channel),
                "instId": formatted_symbol
            }]
        }

    async def start_websocket(self):
        """Start the WebSocket connection with timing control"""
        # Wait for start time if needed
        time_to_start = (self.start_time - datetime.now()).total_seconds()
        if time_to_start > 0:
            logger.info(f"Waiting {time_to_start:.2f}s until collection starts at {self.get_formatted_time(self.start_time)}")
            await asyncio.sleep(time_to_start)

        try:
            self.ws_session = aiohttp.ClientSession()
            self.ws_connection = await self.ws_session.ws_connect(
                self.ws_url,
                heartbeat=20,
                autoping=True,
                receive_timeout=40
            )

            # Add ping and message monitoring tasks
            asyncio.create_task(self._maintain_connection())

            subscribe_data = orjson.dumps(self.get_subscription_data()).decode('utf-8')
            await self.ws_connection.send_str(subscribe_data)
            
            self.is_running = True
            self.collection_started = True
            self.last_message_time = datetime.now()
            logger.info("WebSocket connection established and collection started")
            
            await self._process_messages()

        except Exception as e:
            logger.error(f"Error in WebSocket connection: {e}")
            if self.reconnect_attempts < self.max_reconnect_attempts:
                self.reconnect_attempts += 1
                logger.info(f"Attempting reconnection ({self.reconnect_attempts}/{self.max_reconnect_attempts})")
                await asyncio.sleep(2 ** self.reconnect_attempts)  # Exponential backoff
                await self.start_websocket()
            else:
                logger.error("Max reconnection attempts reached")

    async def _maintain_connection(self):
        """Maintain WebSocket connection with BitGet-specific heartbeat"""
        while self.is_running:
            try:
                # Check time since last message
                time_since_last_message = (datetime.now() - self.last_message_time).total_seconds()
                
                # Send ping every 25 seconds
                if time_since_last_message >= self.heartbeat_interval:
                    await self.ws_connection.send_str("ping")
                    logger.debug("Sent ping to maintain connection")
                
                # Check for prolonged message inactivity
                if time_since_last_message >= self.max_no_message_time:
                    logger.warning(f"No messages received for {time_since_last_message:.2f} seconds. Reconnecting...")
                    await self.cleanup()
                    await self.start_websocket()
                    break

                await asyncio.sleep(5)  # Check every 5 seconds
            except Exception as e:
                logger.error(f"Error in connection maintenance: {e}")
                break

    async def _process_messages(self):
        """Process incoming WebSocket messages with optimized speed and timing"""
        while self.is_running and datetime.now() <= self.end_time:
            try:
                msg = await self.ws_connection.receive(timeout=0.1)
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    # Update last message time
                    self.last_message_time = datetime.now()
                    
                    # Handle ping/pong
                    if msg.data == "pong":
                        logger.debug("Received pong from server")
                        continue

                    try:
                        data = orjson.loads(msg.data)
                    except Exception:
                        # Skip messages that aren't JSON or pong
                        continue
                    
                    if 'data' in data:
                        precise_time = datetime.now()
                        processed_data = data['data'][0]  # BitGet sends array of data
                        processed_data['time_received'] = self.get_formatted_time(precise_time)
                        
                        await self.queue.put(processed_data)
                        self.messages_processed += 1
                        
                        if self.messages_processed % 100 == 0:
                            time_remaining = (self.end_time - datetime.now()).total_seconds()
                            logger.debug(f"Processed {self.messages_processed} messages. "
                                      f"Time remaining: {time_remaining:.2f}s. "
                                      f"Queue size: {self.queue.qsize()}")

                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    logger.warning("WebSocket connection closed")
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error("WebSocket error")
                    break

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                if datetime.now() <= self.end_time:
                    await self.cleanup()
                    await self.start_websocket()
                break

        self.collection_ended = True
        logger.info("Message processing completed")



    async def process_for_saving(self):
        """Process and save data to a list with optimized batch processing"""
        batch = []
        batch_size = 100

        try:
            while datetime.now() <= self.end_time:
                try:
                    data = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                    batch.append(data)

                    if len(batch) >= batch_size:
                        self.stored_data.extend(batch)
                        batch.clear()
                        
                except asyncio.TimeoutError:
                    if self.collection_ended and self.queue.empty():
                        break
                    continue
                
            # Process remaining batch
            if batch:
                self.stored_data.extend(batch)
                
            logger.info(f"Total messages processed and stored: {len(self.stored_data)}")
            
        except Exception as e:
            logger.error(f"Error in data processing: {e}")
        finally:
            await self.save_data(self.saving_path)
            await self.cleanup()

    async def get_final_snapshot(self, symbol: str):
        """Get final market snapshot from BitGet REST API"""
        api_url = f"https://api.bitget.com/api/v2/spot/market/tickers?symbol={symbol}USDT"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'data' in data and data['data']:
                        market_data = data['data'][0]
                        final_snapshot = {
                            'time': self.get_formatted_time(),
                            'symbol': market_data['symbol'],
                            'price_high': market_data['high24h'],
                            'price_last': market_data['lastPr'],
                            'price_low': market_data['low24h'],
                            'openUtc': market_data['openUtc'],
                        }
                        return final_snapshot
                else:
                    logger.error(f"Failed to get snapshot for {symbol}: {response.status}")
                    return None

    async def save_data(self, saving_path: str):
        """Save the stored data with timing information"""
        try:
            saving_dir = f"{self.release_time.strftime('%Y-%m-%d_%H-%M')}_{self.symbol}"
            full_path = os.path.join(saving_path, saving_dir)
            os.makedirs(full_path, exist_ok=True)

            filename = os.path.join(full_path, f"{self.symbol}_{self.channel}_data.json")
            
            metadata = {
                "symbol": self.symbol,
                "channel": self.channel,
                "start_time": self.get_formatted_time(self.start_time),
                "release_time": self.get_formatted_time(self.release_time),
                "end_time": self.get_formatted_time(self.end_time),
                "total_messages": len(self.stored_data),
                "final_snapshot": await self.get_final_snapshot(self.symbol)
            }
            
            save_data = {
                "metadata": metadata,
                "data": self.stored_data
            }

            with open(filename, "w") as f:
                json.dump(save_data, f, indent=2)
            
            logger.info(f"Data saved successfully to: {filename}")
            logger.info(f"Total messages saved: {len(self.stored_data)}")
            
        except Exception as e:
            logger.error(f"Error saving data: {e}")
            traceback.print_exc()


    async def cleanup(self):
        """Clean up resources"""
        self.is_running = False
        
        if self.ws_connection and not self.ws_connection.closed:
            await self.ws_connection.close()
        
        if self.ws_session and not self.ws_session.closed:
            await self.ws_session.close()
        
        logger.info("Cleanup completed")

# Helper functions for running collections
async def run_collection(symbol: str, release_time: datetime, duration_minutes: float, 
                        channel: str, save_path: str, start_collect_before_release_sec: int = 30):
    """Run a single channel collection"""
    ws = Bitget_save_ws_data(
        symbol=symbol,
        release_time=release_time,
        duration_minutes=duration_minutes,
        saving_path=save_path,
        channel=channel,
        pre_release_seconds=start_collect_before_release_sec
    )
    
    try:
        websocket_task = asyncio.create_task(ws.start_websocket())
        data_task = asyncio.create_task(ws.process_for_saving())
        await asyncio.gather(websocket_task, data_task)
    except Exception as e:
        logger.error(f"Error in collection process: {e}")

async def run_all_collections(symbol: str, release_time: datetime, duration_minutes: float, 
                            save_path: str, start_collect_before_release_sec: int = 30):
    """Run collection for all channels concurrently"""
    channels = [
        Bitget_save_ws_data.CHANNEL_TICKER,
        Bitget_save_ws_data.CHANNEL_TRADE,
        Bitget_save_ws_data.CHANNEL_DEPTH5
    ]

    tasks = []
    ws_instances = []
    for channel in channels:
        ws = Bitget_save_ws_data(
            symbol=symbol,
            release_time=release_time,
            duration_minutes=duration_minutes,
            saving_path=save_path,
            channel=channel,
            pre_release_seconds=start_collect_before_release_sec
        )
        ws_instances.append(ws)
        try:
            websocket_task = asyncio.create_task(ws.start_websocket())
            data_task = asyncio.create_task(ws.process_for_saving())
            tasks.append(asyncio.gather(websocket_task, data_task))
        except Exception as e:
            logger.error(f"Error in collection process for channel {channel}: {e}")
    
    await asyncio.gather(*tasks)

# Example usage
async def main():
    # Example: Collect data for a token
    release_time = datetime.now() + timedelta(seconds=5)

    await run_all_collections(
        symbol="BTC",
        release_time=release_time,
        duration_minutes=2,
        save_path="/root/trading_systems/bitget/bitget_test_data",
        start_collect_before_release_sec=3
    )

if __name__ == "__main__":
    asyncio.run(main())