import os
import sys
import time
import logging
import fcntl

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Explicitly set logger level

LOCK_FILE = '/tmp/example_locking_script.lock'
lock_file = None  # Declare global variable for the lock file

def create_lock():
    """Create a lock file using fcntl to prevent multiple instances."""
    global lock_file
    lock_file = open(LOCK_FILE, 'w')
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_file.write(str(os.getpid()))
        lock_file.flush()
    except IOError:
        # Another instance is running
        logger.info("Script is already running. Exiting.")
        sys.exit(0)

def remove_lock():
    """Remove the lock file to indicate the script has finished running."""
    global lock_file
    if lock_file:
        lock_file.close()
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)

def main():
    create_lock()
    try:
        # Simulate long-running task
        for i in range(14):
            logger.info("is running...")
            time.sleep(10)  # Simulate a task that takes 10 seconds
        logger.info("Task completed.")
    finally:
        remove_lock()

if __name__ == "__main__":
    main()


#*/1 * * * * /root/trading_systems/tradingvenv/bin/python /root/trading_systems/skipping_test_script.py >> /root/trading_systems/cronlogs_test/skipping_test_script.log 2>&1