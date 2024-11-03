import re
import logging
from datetime import datetime
from playwright.sync_api import sync_playwright
import traceback
import os
import json


# Configure logging

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(message)s - %(funcName)s')

def main():

    path_found_pairs_saved = '/root/trading_systems/bitget/new_pair_data_bitget'

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        try:
            page.goto("https://www.bitget.com/support/sections/5955813039257")
            page.wait_for_selector('//*[@id="support-main-area"]/div/div/div[3]/div[2]/div/div[1]/div')
            sections = page.query_selector_all('//*[@id="support-main-area"]/div/div/div[3]/div[2]/div/div[1]/div/section')
            counter = 0
            full_url_list = []
            # Retrieve all the listings by checking the headline of the announcement
            for section in sections:
                try:
                    counter += 1
                    text_content = section.inner_text()
                    # Check if the text contains certain components
                    if 'will list' in text_content.lower():
                        # Retrieve the href attribute
                        href_element = section.query_selector('a')
                        href = href_element.get_attribute('href') if href_element else None
                        logging.debug(f"Section {counter} is new listing")
                        full_url = f"https://www.bitget.com{href}"
                        full_url_list.append(full_url)
                except Exception as e:
                    logging.error(f'Error processing section {counter}: {e}')
                    logging.error(section.inner_text())

            try:
                # Go through all new listings and get the information
                for full_url in full_url_list:
                    annpuncement_dict = get_info_from_page(page, full_url)
                    #logging.debug(annpuncement_dict)
                    logging.debug(f'finished extracting info for {annpuncement_dict["pair"]}')
                    logging.debug('------------------------------------')
                    # Save the information to a file
                    saving_data_to_file(path_found_pairs_saved, annpuncement_dict)  
            except Exception as e:
                logging.error(f"when extracting or saving listing info: {e}")


        except Exception as e:
            logging.error(f"main function error: {e}")
            traceback.print_exc()

        finally:
            browser.close()



def get_info_from_page(page, full_url):
    page.goto(full_url)
    page.wait_for_selector('//*[@id="support-main-area"]/div/div/div[3]/div[2]/div[1]/div/div[2]')
    paragraphs = page.query_selector_all('//*[@id="support-main-area"]/div/div/div[3]/div[2]/div[1]/div/div[2]//p')

    # Initialize variables
    pair = None
    date_time_string = ''
    non_usdt_pair = None

    # Iterate through the paragraph elements and process their text content
    for paragraph in paragraphs:
        text_content = paragraph.inner_text()
        if text_content:
            if 'trading available' in text_content.lower():
                date_time_string = extract_date_time_string(text_content)
                logging.debug(f"Extracted date and time: {date_time_string}")

            if '/usdt' in text_content.lower():
                # Extract the trading pair using regular expression
                match = re.search(r'(\w+)/USDT', text_content)
                if match:
                    pair = match.group(1) + "USDT"
                    logging.debug(f"Extracted pair: {pair}")
            
            if not '/usdt' in text_content.lower() and 'spot trading link' in text_content.lower():
                    #seperate pair from the content
                    non_usdt_pair_found = text_content.split(':')[-1].strip()
                    non_usdt_pair_tuple = non_usdt_pair_found.split('/')
                    non_usdt_pair = non_usdt_pair_tuple[0] + non_usdt_pair_tuple[1]
                    logging.debug(f"Non usdt pair: {non_usdt_pair}")


    # Construct the dictionary
    listing_info = {
        "exchange": "bitget",
        "url": full_url,
        "date_time_string": date_time_string,
        "pair": pair,
        "non_usdt_pair": non_usdt_pair
    }

    return listing_info


def saving_data_to_file(path_found_pairs_saved, listing_info):
    # Create a directory to store the data
    os.makedirs(path_found_pairs_saved, exist_ok=True)

    # Create the file name
    date_time_obj = datetime.strptime(listing_info['date_time_string'], "%b %d, %Y, %I%p")
    formatted_date_str = date_time_obj.strftime("%d-%m-%y") 

    file_name = f"{formatted_date_str}_{listing_info['pair']}.json"
    if listing_info['pair'] is None:
        file_name = f"{listing_info['non_usdt_pair']}.json"
    file_path = os.path.join(path_found_pairs_saved, file_name)

    # Check if the file already exists
    if file_name in os.listdir(path_found_pairs_saved):
        with open(file_path, 'r') as f:
            existing_pair_dict = json.load(f)

        # Update the date_time_string field if it exists, otherwise create it
        existing_pair_dict['exchange'] = listing_info['exchange']
        existing_pair_dict['date_time_string'] = listing_info['date_time_string']
        existing_pair_dict['pair'] = listing_info['pair']
        existing_pair_dict['non_usdt_pair'] = listing_info['non_usdt_pair']

        # Write the updated dictionary back to the file
        with open(file_path, 'w') as f:
            json.dump(existing_pair_dict, f, indent=4)
        logging.debug(f"{listing_info['pair']} has been updated.")
    else:
        # Create a new dictionary for the pair
        with open(file_path, 'w') as f:
            json.dump(listing_info, f, indent=4)
        logging.debug(f"New listing: {listing_info['pair']} has been added.")




def extract_date_time_string(text):
    # Use a regular expression to extract the date and time
    match = re.search(r'Trading Available: (\d{1,2} \w+ \d{4}), (\d{2}:\d{2}) \(UTC\)', text)
    if match:
        date_str = match.group(1)
        time_str = match.group(2)

        # Parse the extracted date and time
        date_time_obj = datetime.strptime(f"{date_str} {time_str}", "%d %B %Y %H:%M")

        # Format the date and time as required
        date_time_string = date_time_obj.strftime("%b %d, %Y, %-I%p")
        return date_time_string
    return None


if __name__ == "__main__":
    main()