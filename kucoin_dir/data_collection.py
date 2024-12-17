import asyncio
import aiohttp
import orjson
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import os
import json
from typing import Dict, Any, Optional, List

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False



class Kucoin_save_ws_data:
    CHANNEL_TICKER = "ticker"
    CHANNEL_LEVEL2 = "level2"
    CHANNEL_MATCH = "match"
    CHANNEL_DEPTH5 = "level2Depth5"
    CHANNEL_SNAPSHOT = "snapshot"
    CHANNEL_LEVEL1 = "level1"

    def __init__(self, 
                 symbol: str, 
                 release_time: datetime, 
                 duration_minutes: float,
                 saving_path: str,
                 channel: str = "ticker" ,
                 pre_release_seconds: int = 30):
        
        # Basic configuration
        self.symbol = symbol
        self.channel = channel
        self.api_url = "https://api.kucoin.com"
        
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
    


        logger.info(f"Initialized collector for {symbol} - Start: {self.get_formatted_time(self.start_time)}, "
                   f"Release: {self.get_formatted_time(release_time)}, "
                   f"End: {self.get_formatted_time(self.end_time)}")



    @staticmethod
    def get_formatted_time(dt: datetime = None) -> str:
        """Return formatted time string with microseconds"""
        if dt is None:
            dt = datetime.now()
        return dt.strftime('%H:%M:%S.%f')[:-2]
    
    # logging code end
    #########################################










    ###########################################
    # establishing and maintaining websocket connection

    async def get_token(self) -> Optional[str]:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.api_url}/api/v1/bullet-public") as response:
                    if response.status == 200:
                        data = await response.json(loads=orjson.loads)
                        return data['data']['token']
        except Exception as e:
            logger.error(f"Failed to get WebSocket token: {e}")
            return None

    async def get_websocket_url(self) -> Optional[str]:
        token = await self.get_token()
        return f"wss://ws-api-spot.kucoin.com/?token={token}" if token else None

    def get_subscription_data(self) -> dict:
        if self.channel == self.CHANNEL_DEPTH5:
            topic = f"/spotMarket/{self.channel}:{self.symbol}-USDT"
        elif self.channel == self.CHANNEL_SNAPSHOT:
            topic = f"/market/{self.channel}:{self.symbol}-USDT"
        elif self.channel == self.CHANNEL_LEVEL1:
            topic = f"/spotMarket/{self.channel}:{self.symbol}-USDT"
        else:
            topic = f"/market/{self.channel}:{self.symbol}-USDT"
        
        return {
            "id": int(datetime.now().timestamp() * 1000),
            "type": "subscribe",
            "topic": topic,
            "privateChannel": False,
            "response": True
        }

    async def start_websocket(self):
        """Start the WebSocket connection with timing control"""
        # Wait for start time if needed
        time_to_start = (self.start_time - datetime.now()).total_seconds()
        if time_to_start > 0:
            logger.info(f"Waiting {time_to_start:.2f}s until collection data at {self.get_formatted_time(self.start_time)}")
            await asyncio.sleep(time_to_start)

        try:
            ws_url = await self.get_websocket_url()
            if not ws_url:
                logger.error("Could not obtain WebSocket URL")
                return

            self.ws_session = aiohttp.ClientSession()
            self.ws_connection = await self.ws_session.ws_connect(
                ws_url,
                heartbeat=20,
                autoping=True,
                receive_timeout=30
            )

            # Add ping message handling
            asyncio.create_task(self._keep_alive())
            

            subscribe_data = orjson.dumps(self.get_subscription_data()).decode('utf-8')
            await self.ws_connection.send_str(subscribe_data)
            
            self.is_running = True
            self.collection_started = True
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
    
    async def _keep_alive(self):
        """Maintain connection with explicit ping/pong"""
        while self.is_running:
            try:
                await self.ws_connection.ping()
                await asyncio.sleep(15)  # Send ping every 15 seconds
            except Exception as e:
                logger.error(f"Error in keep_alive: {e}")
                break


# process initial messsages from websocket 
    async def _process_messages(self):
        """Process incoming WebSocket messages with optimized speed and timing"""

        while self.is_running and datetime.now() <= self.end_time:
            try:
                msg = await self.ws_connection.receive(timeout=0.1)
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = orjson.loads(msg.data)
                    
                    # Update heartbeat time for any valid message
                    last_heartbeat = datetime.now()
                    
                    if data.get('type') == 'pong':
                        continue


                    if data.get('type') == 'message' and 'data' in data:
                        precise_time = datetime.now()
                        processed_data = data['data']
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

                # Check heartbeat
                if (datetime.now() - last_heartbeat).total_seconds() > 30:
                    logger.warning("No heartbeat received, reconnecting...")
                    await self.cleanup()
                    await self.start_websocket()
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

# end of websocket connect and maintain code 
##############################################












# [... previous logging configuration remains the same ...]

    async def process_for_saving(self):
        """Process and save data in batches with periodic saving"""
        batch = []
        batch_size = 1000  # Increased batch size
        batch_save_interval = 900  # 15 minutes in seconds
        last_batch_save_time = datetime.now()

        try:
            while datetime.now() <= self.end_time or not self.queue.empty():
                try:
                    # Wait for data with a timeout
                    data = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                    batch.append(data)

                    # Check if it's time to save a batch
                    current_time = datetime.now()
                    time_since_last_save = (current_time - last_batch_save_time).total_seconds()

                    if (len(batch) >= batch_size or 
                        time_since_last_save >= batch_save_interval or 
                        (self.collection_ended and self.queue.empty())):
                        
                        # Save the current batch
                        await self.save_batch_data(batch, last_batch_save_time)
                        
                        # Reset batch and update last save time
                        batch.clear()
                        last_batch_save_time = current_time

                except asyncio.TimeoutError:
                    if self.collection_ended and self.queue.empty():
                        break
                    continue
                
            # Process any remaining batch
            if batch:
                await self.save_batch_data(batch, last_batch_save_time)
            
            logger.info(f"Total messages processed: {len(self.stored_data)}")
            
        except Exception as e:
            logger.error(f"Error in data processing: {e}")
        finally:
            await self.cleanup()

    async def save_batch_data(self, batch: list, batch_save_time: datetime):
        """Save a batch of data to a separate file"""
        try:
            saving_dir = f"{self.release_time.strftime('%Y-%m-%d_%H-%M')}_{self.symbol}"
            full_path = os.path.join(self.saving_path, saving_dir)
            os.makedirs(full_path, exist_ok=True)

            # Create a unique filename based on the batch save time
            batch_filename = os.path.join(
                full_path, 
                f"{self.symbol}_{self.channel}_batch_{batch_save_time.strftime('%H-%M-%S')}.json"
            )
            
            # Prepare metadata for this batch
            metadata = {
                "symbol": self.symbol,
                "channel": self.channel,
                "batch_save_time": self.get_formatted_time(batch_save_time),
                "total_messages_in_batch": len(batch),
                "collection_start_time": self.get_formatted_time(self.start_time),
                "collection_end_time": self.get_formatted_time(self.end_time)
            }
            
            save_data = {
                "metadata": metadata,
                "data": batch
            }

            # Save the batch
            with open(batch_filename, "w") as f:
                json.dump(save_data, f)
            
            logger.info(f"Batch data saved successfully to: {batch_filename}")
            logger.info(f"Batch messages saved: {len(batch)}")

            # Optionally, clear the stored data to free up memory
            self.stored_data.clear()
            
        except Exception as e:
            logger.error(f"Error saving batch data: {e}")



    # async def process_for_saving(self):
    #     """Process and save data to a list with optimized batch processing"""
    #     batch = []
    #     batch_size = 100  # Increased batch size for better performance

    #     try:
    #         while datetime.now() <= self.end_time or not self.queue.empty():
    #             try:
    #                 data = await asyncio.wait_for(self.queue.get(), timeout=1.0)
    #                 batch.append(data)

    #                 if len(batch) >= batch_size:
    #                     self.stored_data.extend(batch)
    #                     batch.clear()
                        
    #             except asyncio.TimeoutError:
    #                 if self.collection_ended and self.queue.empty():
    #                     break
    #                 continue
                
    #         # Process remaining batch
    #         if batch:
    #             self.stored_data.extend(batch)
                
    #         logger.info(f"Total messages processed and stored: {len(self.stored_data)}")
            
    #     except Exception as e:
    #         logger.error(f"Error in data processing: {e}")
    #     finally:
    #         await self.save_data(self.saving_path)
    #         await self.cleanup()



    def convert_timestamp_final_snapshot(self,timestamp: int) -> str:
        # Convert milliseconds to seconds
        timestamp_in_seconds = timestamp / 1000
        # Convert to datetime object
        dt_object = datetime.fromtimestamp(timestamp_in_seconds)
        # Format the datetime object to a human-readable string
        human_readable_time = dt_object.strftime('%H:%M:%S.%f')[:-2]
        return human_readable_time


    async def get_final_snapshot(self,symbol: str):
        api_url = f"https://api.kucoin.com/api/v1/market/stats?symbol={symbol}-USDT"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as response:
                if response.status == 200:
                    data_raw = await response.json()
                    data = data_raw['data']
                    final_snapshot ={
                        'time': self.convert_timestamp_final_snapshot(data['time']),
                        'symbol': data['symbol'],
                        'price_high': data['high'],
                        'price_last': data['last'],
                        'price_low': data['low'],
                        'price_avg': data['averagePrice'],
                        'volValue_USDT': data['volValue'],

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
            
            # Add collection metadata
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
                json.dump(save_data, f)
            
            logger.info(f"Data saved successfully to: {filename}")
            logger.info(f"Total messages saved: {len(self.stored_data)}")
            
        except Exception as e:
            logger.error(f"Error saving data: {e}")

    async def cleanup(self):
        """Clean up resources"""
        self.is_running = False
        
        if self.ws_connection and not self.ws_connection.closed:
            await self.ws_connection.close()
        
        if self.ws_session and not self.ws_session.closed:
            await self.ws_session.close()
        
        logger.info("Cleanup completed")







################################
# testing code



async def main():
    # List of top cryptocurrencies by market cap
    top_coins = ["BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE"]
    
    # Set release time slightly in the future
    release_time = datetime.now() + timedelta(seconds=10)  # 10 seconds from now
    
    # Run collections for all top coins concurrently
    await run_all_collections(
        symbol_list=top_coins,  # Pass list of coins
        release_time=release_time,
        duration_minutes=500,  # 30 seconds of data collection
        save_path="./data",
        start_collect_before_release_sec=5  # Start collecting 5 seconds before release time
    )
    # await run_collection(
    #     symbol="BTC",
    #     release_time=release_time,
    #     duration_minutes=0.1,
    #     channel="level2Depth5",
    #     save_path="./data",
    #     start_collect_before_release_sec = 3
    #     )





#########################
# multi channel use
async def run_all_collections(symbol_list: List[str], release_time: datetime, duration_minutes: float, 
                              save_path: str, start_collect_before_release_sec: int = 30):
    """Run collection for all channels and symbols concurrently"""
    # List all channel attributes from the class
    channels = [
        Kucoin_save_ws_data.CHANNEL_TICKER,
        # Kucoin_save_ws_data.CHANNEL_LEVEL2,
        Kucoin_save_ws_data.CHANNEL_MATCH,
        Kucoin_save_ws_data.CHANNEL_DEPTH5,
        # Kucoin_save_ws_data.CHANNEL_SNAPSHOT,
        # Kucoin_save_ws_data.CHANNEL_LEVEL1
    ]

    tasks = []
    ws_instances = []
    
    # Nested loops to create instances for each symbol and channel
    for symbol in symbol_list:
        for channel in channels:
            ws = Kucoin_save_ws_data(
                symbol=symbol,
                release_time=release_time,
                duration_minutes=duration_minutes,
                saving_path=save_path,
                channel=channel,
                pre_release_seconds=start_collect_before_release_sec
            )
            ws_instances.append(ws)
            try:
                # Start the websocket connection
                websocket_task = asyncio.create_task(ws.start_websocket())
                # Process and save data
                data_task = asyncio.create_task(ws.process_for_saving())
                # Wait for both tasks to complete
                tasks.append(asyncio.gather(websocket_task, data_task))
            except Exception as e:
                logger.error(f"Error in collection process for symbol {symbol}, channel {channel}: {e}")
    
    # Run all tasks concurrently
    await asyncio.gather(*tasks)


#########################
# single channel use 
# 1,16,31,46 * * * * /root/trading_systems/tradingvenv/bin/python /root/trading_systems/kucoin_dir/data_collection.py >> /root/trading_systems/kucoin_dir/cronlogs/data_collection.log 2>&1

async def run_collection(symbol: str, release_time: datetime, duration_minutes: float, 
                        channel: str, save_path: str,start_collect_before_release_sec: int = 30):
    
    """Main function to run the collection process"""
    ws = Kucoin_save_ws_data(
        symbol=symbol,
        release_time=release_time,
        duration_minutes=duration_minutes,
        saving_path=save_path,
        channel=channel,
        pre_release_seconds=start_collect_before_release_sec
    )
    
    try:
        # Start the websocket connection
        websocket_task = asyncio.create_task(ws.start_websocket())
        # Process and save data
        data_task = asyncio.create_task(ws.process_for_saving())
        
        # Wait for both tasks to complete
        await asyncio.gather(websocket_task, data_task)
        
        
    except Exception as e:
        logger.error(f"Error in collection process: {e}")


if __name__ == "__main__":
    asyncio.run(main())
