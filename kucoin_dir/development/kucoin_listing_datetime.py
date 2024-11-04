

import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import os
import json
import shutil
import traceback

def main():

    directory_bybit  = 'new_pair_data'
    directory_kucoin = 'new_pair_data_kucoin'


    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")

    print('start driver')
    # Initialize the Chrome driver with Service class and options
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)



    try:
        # Navigate to the webpage
        driver.get("https://www.kucoin.com/announcement/new-listings")

        # Wait for the element to be present using WebDriverWait
        elements = WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.XPATH, "//*[@id='root']/div/div[2]/div/div[2]/div[2]/a"))
        )   # entire page //*[@id='root']/div/div[2]/div/div[2]/div[2]
        

        counter = 0
        for element in elements:
            try:
                counter += 1
                announcement_dict = create_announcement_dict(element)
                if announcement_dict:

                    # Create a directory to store the data  
                    dir_for_pair = directory_kucoin
                    os.makedirs(dir_for_pair, exist_ok=True)

                    file_name = f"{announcement_dict['pair']}.json"
                    file_path = os.path.join(dir_for_pair, file_name)

                    if file_name in os.listdir(dir_for_pair):
                        with open(file_path, 'r') as f:
                            existing_pair_dict = json.load(f)
                        
                        # Update the date_time_string field if it exists, otherwise create it
                        existing_pair_dict['exchange'] = "kucoin"
                        existing_pair_dict['date_time_string'] = announcement_dict['date_time_string']
                        existing_pair_dict['day_of_week'] = announcement_dict['day_of_week']
                        existing_pair_dict['hour'] = announcement_dict['hour']
                        existing_pair_dict['minute'] = announcement_dict['minute']
                        existing_pair_dict['second'] = announcement_dict['second']
                        existing_pair_dict['published_datetime'] = announcement_dict['published_datetime']
                        
                        # Write the updated dictionary back to the file
                        with open(file_path, 'w') as f:
                            json.dump(existing_pair_dict, f, indent=4)
                        print(f"{announcement_dict['pair']} has been updated.")
                    else:
                        # Create a new dictionary for the pair

                        
                        with open(file_path, 'w') as f:
                            json.dump(announcement_dict, f, indent=4)
                        print(f"New listing: {announcement_dict['pair']} has been added.")
                        
                else:
                    print(f"item {counter} is no new spot listing")
            except Exception as e:
                print(f'error in loop:{e}')
                print(element.text)


    except TimeoutException:
        print("Timed out waiting for element to appear.")
    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
    finally:
        driver.quit()


    #moves files from kucoin to bybit
    move_files(source_dir=directory_kucoin, destination_dir=directory_bybit)
    print('-----------------------------------------------')
    print('code finished at', datetime.now())
    print()

        

def move_files(source_dir, destination_dir):
    # Ensure the destination directory exists
    os.makedirs(destination_dir, exist_ok=True)
    
    # List all files in the source directory
    files = os.listdir(source_dir)
    
    for file_name in files:
        
        if file_name not in os.listdir(destination_dir):
            # Construct full file path
            source_file = os.path.join(source_dir, file_name)
            destination_file = os.path.join(destination_dir, file_name)
            
            # Copy the file
            shutil.copy(source_file, destination_file)
            print(f"Copied {file_name} to {destination_dir}")

def format_datetime(dt):
    return dt.strftime("%b %d, %Y, %-I:%M%p") if dt.minute != 0 else dt.strftime("%b %d, %Y, %-I%p")

def create_announcement_dict(element):
    # Check if the element's text is not empty
    if 'Trading:' in element.text:
        href = element.get_attribute('href')
        announcement = element.text

        # Extract trading time and date
        trading_match = re.search(r'Trading: (\d{2}:\d{2}) on (\w+ \d+, \d{4})', announcement)
        trading_time = trading_match.group(1)
        trading_date = trading_match.group(2)

        # Extract published date and time
        published_match = re.search(r'(\d{2}/\d{2}/\d{4}), (\d{2}:\d{2}:\d{2})', announcement)
        published_date = published_match.group(1)
        published_time = published_match.group(2)

        # Convert trading date and time to datetime object
        trading_datetime = datetime.strptime(f"{trading_date} {trading_time}", "%B %d, %Y %H:%M")

        # Convert published date and time to datetime object
        published_datetime = datetime.strptime(f"{published_date} {published_time}", "%m/%d/%Y %H:%M:%S")

        # Extract pair
        pair_match = re.search(r'(\w+)\s\((\w+)\)', announcement)
        pair = pair_match.group(2) + "USDT"

        # Create the announcement dictionary
        announcement_dict = {
            "exchange": "kucoin",
            "day_of_week": trading_datetime.strftime("%a").lower(),
            "hour": trading_datetime.hour,
            "minute": trading_datetime.minute,
            "second": trading_datetime.second,
            "url": href,
            "date_time_string": format_datetime(trading_datetime),
            "pair": pair,
            "published_datetime": [
                published_datetime.year,
                published_datetime.month,
                published_datetime.day,
                published_datetime.hour,
                published_datetime.minute,
                published_datetime.second
            ]
        }
        return announcement_dict
    return None


if __name__ == "__main__":
    main()