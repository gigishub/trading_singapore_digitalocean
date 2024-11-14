import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s',
    datefmt='%H:%M:%S'
)

async def task():
    logging.info("Task started")
    await asyncio.sleep(6)
    logging.info("Task completed")
    return {"success": True}

async def delayed_task(name, start_delay):
    await asyncio.sleep(start_delay)
    return await task()

async def main():
    # Create tasks with slight delays
    tasks = [
        asyncio.create_task(delayed_task("Task 1", 0.1)),
        asyncio.create_task(delayed_task("Task 2", 0.2)),
        asyncio.create_task(delayed_task("Task 3", 0.3))
    ]
    
    # Run tasks concurrently and check their responses
    for task in tasks:
        response = await task
        if response.get("success"):
            logging.info("A task completed successfully")
            break

# Run the main function
asyncio.run(main())