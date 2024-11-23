import asyncio
import aiohttp
import orjson
import logging
import os
import json
from datetime import datetime
from typing import Dict, Any, Optional, List, Callable

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

class KucoinWebsocketDataManager:
    # Supported Channels
    CHANNEL_TICKER = "ticker"
    CHANNEL_LEVEL2 = "level2"
    CHANNEL_MATCH = "match"
    CHANNEL_DEPTH5 = "level2Depth5"
    CHANNEL_SNAPSHOT = "snapshot"
    CHANNEL_LEVEL1 = "level1"
    CHANNEL_ORDER_BOOK = "orderBook"
    CHANNEL_INDEX = "indexPrice"
    CHANNEL_MARKET_PRICES = "marketPrice"

    def __init__(self, 
                 symbols: List[str], 
                 channels: List[str] = None, 
                 base_save_dir: str = "/root/trading_systems/kucoin_data_collection"):
        """
        Initialize WebSocket data manager with optional custom save directory
        
        :param symbols: List of trading symbols
        :param channels: List of channels to subscribe
        :param base_save_dir: Base directory for saving collected data
        """
        self.symbols = symbols
        self.channels = channels or [self.CHANNEL_TICKER]
        self.api_url = "https://api.kucoin.com"
        
        # WebSocket connections per symbol and channel
        self.websockets = {}
        
        # Data queues per symbol and channel
        self.queues = {}
        
        # Store received data for potential saving
        self.received_data = {}
        
        # Tracking collection start time
        self.release_time = datetime.now()
        
        # Base save directory
        self.base_save_dir = base_save_dir
        
        # Event to control overall WebSocket management
        self.is_running = False
        
        # Optional custom data processors
        self.custom_processors = {}

    async def _get_token(self) -> Optional[str]:
        """Retrieve WebSocket authentication token"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.api_url}/api/v1/bullet-public") as response:
                    if response.status == 200:
                        data = await response.json(loads=orjson.loads)
                        return data['data']['token']
        except Exception as e:
            logger.error(f"Failed to get WebSocket token: {e}")
            return None

    def _get_subscription_topic(self, symbol: str, channel: str) -> str:
        """Generate WebSocket subscription topic based on channel"""
        symbol_pair = f"{symbol}-USDT"
        channel_map = {
            self.CHANNEL_DEPTH5: f"/spotMarket/{channel}:{symbol_pair}",
            self.CHANNEL_SNAPSHOT: f"/market/{channel}:{symbol_pair}",
            self.CHANNEL_LEVEL1: f"/spotMarket/{channel}:{symbol_pair}",
            self.CHANNEL_TICKER: f"/market/{channel}:{symbol_pair}",
            self.CHANNEL_MATCH: f"/market/{channel}:{symbol_pair}",
            self.CHANNEL_LEVEL2: f"/spotMarket/{channel}:{symbol_pair}",
            self.CHANNEL_ORDER_BOOK: f"/spotMarket/{channel}:{symbol_pair}",
            self.CHANNEL_INDEX: f"/index/market/{channel}:{symbol_pair}",
            self.CHANNEL_MARKET_PRICES: f"/market/{channel}:{symbol_pair}"
        }
        return channel_map.get(channel, f"/market/{channel}:{symbol_pair}")

    def register_custom_processor(self, symbol: str, channel: str, processor: Callable):
        """
        Register a custom data processor for a specific symbol and channel
        
        :param symbol: Trading symbol
        :param channel: WebSocket channel
        :param processor: Async function to process incoming data
        """
        key = f"{symbol}_{channel}"
        self.custom_processors[key] = processor

    async def start_websocket(self, symbol: str, channel: str):
        """
        Start WebSocket connection for a specific symbol and channel
        
        :param symbol: Trading symbol
        :param channel: WebSocket channel
        """
        try:
            # Generate unique identifier
            key = f"{symbol}_{channel}"
            
            # Create queue if not exists
            if key not in self.queues:
                self.queues[key] = asyncio.Queue()
            
            # Get WebSocket URL
            token = await self._get_token()
            if not token:
                logger.error(f"Could not obtain token for {key}")
                return
            
            ws_url = f"wss://ws-api-spot.kucoin.com/?token={token}"
            
            # Establish WebSocket session
            session = aiohttp.ClientSession()
            connection = await session.ws_connect(ws_url, heartbeat=20, receive_timeout=0.1)
            
            # Store connection
            self.websockets[key] = {
                'session': session,
                'connection': connection
            }
            
            # Prepare subscription
            subscribe_data = {
                "id": int(datetime.now().timestamp() * 1000),
                "type": "subscribe",
                "topic": self._get_subscription_topic(symbol, channel),
                "privateChannel": False,
                "response": True
            }
            
            # Send subscription
            await connection.send_str(orjson.dumps(subscribe_data).decode('utf-8'))
            
            # Start message processing
            await self._process_messages(symbol, channel)
            
        except Exception as e:
            logger.error(f"Error starting WebSocket for {key}: {e}")

    async def _process_messages(self, symbol: str, channel: str):
        """
        Process incoming WebSocket messages for a specific symbol and channel
        
        :param symbol: Trading symbol
        :param channel: WebSocket channel
        """
        key = f"{symbol}_{channel}"
        websocket = self.websockets.get(key)
        
        if not websocket:
            return
        
        connection = websocket['connection']
        queue = self.queues[key]
        
        # Initialize data storage for this symbol and channel
        if key not in self.received_data:
            self.received_data[key] = []
        
        while self.is_running:
            try:
                msg = await asyncio.wait_for(connection.receive(), timeout=0.001)
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = orjson.loads(msg.data)
                    
                    if data.get('type') == 'pong':
                        continue
                    
                    if data.get('type') == 'message' and 'data' in data:
                        processed_data = data['data']
                        processed_data['time_received'] = datetime.now().strftime('%H:%M:%S.%f')[:-5]
                        
                        # Store data
                        self.received_data[key].append(processed_data)
                        
                        # Put data in queue
                        await queue.put(processed_data)
                        
                        # Call custom processor if registered
                        processor_key = f"{symbol}_{channel}"
                        if processor_key in self.custom_processors:
                            await self.custom_processors[processor_key](processed_data)
            
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing message for {key}: {e}")
                break

    async def get_data(self, symbol: str, channel: str):
        """
        Retrieve data from a specific symbol and channel queue
        
        :param symbol: Trading symbol
        :param channel: WebSocket channel
        :return: Processed data dictionary
        """
        key = f"{symbol}_{channel}"
        return await self.queues[key].get()

    async def save_data(self):
        """
        Save collected data to files
        Uses base directory with timestamp and symbol-specific subdirectories
        """
        saving_dir = os.path.join(
            self.base_save_dir, 
            f"{self.release_time.strftime('%Y-%m-%d_%H-%M')}"
        )
        os.makedirs(saving_dir, exist_ok=True)

        for key, data in self.received_data.items():
            # Split key back into symbol and channel
            symbol, channel = key.split('_')
            
            # Create filename
            filename = os.path.join(
                saving_dir, 
                f"{symbol}_{channel}_data.json"
            )
            
            # Save data
            with open(filename, "w") as f:
                json.dump(data, f, indent=2)
        
        logger.info(f"Data saved to directory: {saving_dir}")
        return saving_dir

    async def start(self):
        """Start WebSocket connections for all symbols and channels"""
        self.is_running = True
        tasks = []
        
        for symbol in self.symbols:
            for channel in self.channels:
                tasks.append(
                    asyncio.create_task(self.start_websocket(symbol, channel))
                )
        
        await asyncio.gather(*tasks)

    async def stop(self):
        """
        Stop all WebSocket connections, clean up resources, and save data
        """
        self.is_running = False
        
        # Close WebSocket connections
        for key, ws_info in self.websockets.items():
            if ws_info['connection'] and not ws_info['connection'].closed:
                await ws_info['connection'].close()
            
            if ws_info['session'] and not ws_info['session'].closed:
                await ws_info['session'].close()
        
        # Save collected data
        await self.save_data()
        
        # Clear resources
        self.websockets.clear()
        self.queues.clear()
        self.received_data.clear()

async def default_match_processor(data):
    """Example default processor for match data"""
    logger.info(f"Match Data: {data}")

async def default_level1_processor(data):
    """Example default processor for level1 data"""
    logger.info(f"Level1 Data: {data}")

async def main():
    """Example usage of KucoinWebsocketDataManager"""
    # Custom save directory
    custom_save_dir = "test"
    
    # Initialize manager with multiple symbols and channels
    manager = KucoinWebsocketDataManager(
        symbols=['BTC', 'ETH', 'XRP'], 
        channels=['match', 'level1'],
        base_save_dir=custom_save_dir
    )
    
    # Register custom processors (optional)
    manager.register_custom_processor('BTC', 'match', default_match_processor)
    manager.register_custom_processor('ETH', 'level1', default_level1_processor)
    
    try:
        # Start WebSocket connections
        await manager.start()
        
        # Run for a specific duration
        await asyncio.sleep(30)
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        # Stop and clean up (will also save data)
        await manager.stop()

if __name__ == "__main__":
    asyncio.run(main())