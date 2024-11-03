import re
from datetime import datetime
from playwright.sync_api import sync_playwright
import os
import json
import shutil
import traceback
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s - %(funcName)s')

def main():
    # OLD directory_bybit = 'new_pair_data'
    directory_kucoin = '/root/trading_systems/kucoin_dir/new_pair_data_kucoin'

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        try:
            # Navigate to the webpage
            page.goto("https://www.kucoin.com/announcement/new-listings")

            # Wait for the element to be present
            page.wait_for_selector("//*[@id='root']/div/div[2]/div/div[2]/div[2]/a")

            elements = page.query_selector_all("//*[@id='root']/div/div[2]/div/div[2]/div[2]/a")

            counter = 0
            for element in elements:
                try:
                    counter += 1
                    announcement_dict = create_announcement_dict(element)
                    #process_announcement(element.inner_text())
                except Exception as e:
                    logging.error(f'creating announcment dictonary{e}')
                    print(element.inner_text())
                    traceback.print_exc()
                try:
                    if announcement_dict:
                        save_and_update_announcement(directory_kucoin, announcement_dict)
                except Exception as e:
                    logging.error(f"saving announcement: {e}")
                    traceback.print_exc()


        except Exception as e:
            logging.error(f"loding elements from webpage: {e}")
            traceback.print_exc()
        finally:
            browser.close()

    logging.debug("Finished kucoin listing script")


def save_and_update_announcement(directory, announcement_dict):
    # Create a directory to store the data
    os.makedirs(directory, exist_ok=True)
    # Create the file name
    date_time_obj = datetime.strptime(announcement_dict['date_time_string'], "%b %d, %Y, %I%p")
    formatted_date_str = date_time_obj.strftime("%y-%m-%d") 

    file_name = f"{formatted_date_str}_{announcement_dict['pair']}.json"
    file_path = os.path.join(directory, file_name)

    if file_name in os.listdir(directory):
        with open(file_path, 'r') as f:
            existing_pair_dict = json.load(f)

        # Update the date_time_string field if it exists, otherwise create it
        existing_pair_dict['date_time_string'] = announcement_dict['date_time_string']

        # Write the updated dictionary back to the file
        with open(file_path, 'w') as f:
            json.dump(existing_pair_dict, f, indent=4)
        logging.debug(f"{announcement_dict['pair']} has been updated.")
    else:
        # Create a new dictionary for the pair
        with open(file_path, 'w') as f:
            json.dump(announcement_dict, f, indent=4)
        logging.debug(f"New listing: {announcement_dict['pair']} has been added.")


def format_datetime_to_str(dt):
    return dt.strftime("%b %d, %Y, %-I:%M%p") if dt.minute != 0 else dt.strftime("%b %d, %Y, %-I%p")


def create_announcement_dict(element):
    # Check if the element's text is not empty
    if 'Trading:' in element.inner_text():
        href = element.get_attribute('href')
        announcement = element.inner_text()

        # Extract trading time and date
        logging.debug(f'printing retrived element for debugging: \n\n {announcement}\n')
        #trading_match = re.search(r'Trading: (\d{2}:\d{2}) on (\w+ \d+, \d{4})', announcement)
        trading_match = re.search(r'(\d{1,2}:\d{2})\s*on\s*(\w+\s+\d{1,2},\s+\d{4})', announcement)
        if trading_match:
            trading_time = trading_match.group(1)
            trading_date = trading_match.group(2)

        # Convert trading date and time to datetime object
        trading_datetime = datetime.strptime(f"{trading_date} {trading_time}", "%B %d, %Y %H:%M")

        # Extract pair
        pair_match = re.search(r'(\w+)\s\((\w+)\)', announcement)
        pair = pair_match.group(2) + "USDT"

        # Create the announcement dictionary
        announcement_dict = {
            "exchange": "kucoin",
            "url": href,
            "date_time_string": format_datetime_to_str(trading_datetime),
            "pair": pair,
        }
        return announcement_dict
    return None



###############################
# OLD FUNCTIONS
################################

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


def OLD_create_announcement_dict(element):
    # Check if the element's text is not empty
    if 'Trading:' in element.inner_text():
        href = element.get_attribute('href')
        announcement = element.inner_text()

        # Extract trading time and date
        trading_match = re.search(r'Trading: (\d{2}:\d{2}) on (\w+ \d+, \d{4})', announcement)
        trading_time = trading_match.group(1)
        trading_date = trading_match.group(2)

        ## Extract published date and time
        # published_match = re.search(r'(\d{2}/\d{2}/\d{4}), (\d{2}:\d{2}:\d{2})', announcement)
        # published_date = published_match.group(1)
        # published_time = published_match.group(2)
        
        ## Convert published date and time to datetime object
        # published_datetime = datetime.strptime(f"{published_date} {published_time}", "%m/%d/%Y %H:%M:%S")

        # Convert trading date and time to datetime object
        trading_datetime = datetime.strptime(f"{trading_date} {trading_time}", "%B %d, %Y %H:%M")



        # Extract pair
        pair_match = re.search(r'(\w+)\s\((\w+)\)', announcement)
        pair = pair_match.group(2) + "USDT"

        # Create the announcement dictionary
        announcement_dict = {
            "exchange": "kucoin",
            "url": href,
            "date_time_string": format_datetime_to_str(trading_datetime),
            "pair": pair,
            # "day_of_week": trading_datetime.strftime("%a").lower(),
            # "hour": trading_datetime.hour,
            # "minute": trading_datetime.minute,
            # "second": trading_datetime.second,
            # "published_datetime": [
            #     published_datetime.year,
            #     published_datetime.month,
            #     published_datetime.day,
            #     published_datetime.hour,
            #     published_datetime.minute,
            #     published_datetime.second
            # ]
        }
        return announcement_dict
    return None

if __name__ == "__main__":
    main()