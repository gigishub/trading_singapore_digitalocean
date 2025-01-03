import re
import logging
from datetime import datetime
from playwright.sync_api import sync_playwright
import traceback
import os
import json
import time

# Configure logging
# 3,19,33,48 * * * * /root/trading_systems/tradingvenv/bin/python /root/trading_systems/MEXC_dir/MEXC_listing_playwright.py >> /root/trading_systems/MEXC_dir/cronlogs/MEXC_listing_playwright.log 2>&1
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(message)s') #%(funcName)s

def main():

    path_found_pairs_saved = '/root/trading_systems/gate_io/new_pair_dir_gateio'

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        try:
            page.goto("https://www.gate.io/announcements/newlisted?page")
            #waiting for selector to load
            page.wait_for_selector('//div[@class="article-list-content"]')

            div_elements = page.query_selector_all('.article-list-item-content')            
            logging.debug(f'div_elements: {div_elements}')

            base_url = "https://www.gate.io"   
            try:
                include_terms_1 = ['startup','initial','commence','will list','free offering']
                exclude_terms_2 = ['pre-market','premarket','pre market','mining','futures','pilot','Perpetual Contract','listing Schedule']
                            
                #new approach
                tag = 'non_result_schedule'
                non_result_schedule_dict =process_elements_dict_JS(div_elements,include_terms_1,exclude_terms_2,base_url,tag)
                logging.debug(f'results from non Results and listing schedule tag: {json.dumps(non_result_schedule_dict, indent=4)}')
                logging.debug(f'===============   processed startup listings')
            except Exception as e:
                logging.error(f"when processing NON Result and listing schedule: {e}")

            try:
                # Check if the text contains certain components
                include_terms_2 = ['Listing Schedule']
                exclude_terms_2 = ['pre-market','premarket','pre market','mining','futures','pilot','Perpetual Contract']                
                
                #new approach
                tag = 'result_schedule'
                result_schedule_dict =process_elements_dict_JS(div_elements,include_terms_2,exclude_terms_2,base_url,tag)
                logging.debug(f'=====================================processed kickstarter listings')
                logging.debug(f'results Result and listing schedule tag : {json.dumps(result_schedule_dict, indent=4)}')
            except Exception as e:
                logging.error(f"when processing Result and listing schedule: {e}")



            logging.info(f'{len(non_result_schedule_dict)}  Non Result and listing schedule tagsfound and, {len(result_schedule_dict)} Result and listing schedule tags found')
            logging.debug(f'============  processed all the listings  ======  move on to extration of data from listing ========')


            # try:
            #     # xpath that needs to be present toextract data from the: page.wait_for_selector(xpath_to_load)
            #     xapth_to_load = '//*[@id="__next"]/div[3]/div/div/article/div[2]/div[1]/ol[1]'
            #     xpath_to_search_through = '//*[@id="__next"]/div[3]/div/div/article/div[2]/div[1]/ol[1]'
            #     final_initial_listing_list_of_dict = []

            #     for dict in initial_listing_list_of_dict:
            #         initial_listing_strings = get_info_from_page(page, dict['url'],xapth_to_load,xpath_to_search_through)  
            #         dict['date_time_string'] = find_and_extract_date_time_string(initial_listing_strings)
            #         final_initial_listing_list_of_dict.append(dict)
            #         logging.debug(f"finished extracting info for {dict['pair']} realse date: {dict['date_time_string']}")
                    
            #         save_and_update_data_to_file(path_found_pairs_saved, dict)
            #         logging.debug('------------------------------------')
            #         time.sleep(1)
            # except Exception as e:
            #     logging.error(f"when extracting info from initial listing tag: {e}")
            #     traceback.print_exc()

            # try:
            #     # xpath that needs to be present toextract data from the: page.wait_for_selector(xpath_to_load)
            #     xapth_to_load = '//*[@id="__next"]/div[3]/div/div/article/div[2]/div[1]/ol[1]'
            #     xpath_to_search_through = '//*[@id="__next"]/div[3]/div/div/article/div[2]/div[1]/ol[1]'
            #     final_kickstarter_list_of_dict = []

            #     for dict in kickstarter_list_of_dict:
            #         kickstarter_strings = get_info_from_page(page, dict['url'],xapth_to_load,xpath_to_search_through)  
            #         dict['date_time_string'] = find_and_extract_date_time_string(kickstarter_strings)
            #         final_kickstarter_list_of_dict.append(dict)
            #         logging.debug(f"finished extracting info for {dict['pair']} realse date: {dict['date_time_string']}")
                    
            #         save_and_update_data_to_file(path_found_pairs_saved, dict)
            #         logging.debug('------------------------------------')
            #         time.sleep(1)

            # except Exception as e:
            #     logging.error(f"when extracting info from kickstarter only tag: {e}")
            #     traceback.print_exc()

            
            # try:
            #     # xpath that needs to be present toextract data from the: page.wait_for_selector(xpath_to_load)
            #     xapth_to_load = '//*[@id="__next"]/div[3]/div/div/article/div[2]/ul'
            #     xpath_to_search_through = '//*[@id="__next"]/div[3]/div/div/article/div[2]/ul/li'
            #     final_voted_and_arranged_list_of_dicts = []

            #     for dict in voted_and_arranged_list_of_dicts:
            #         vote_arrange_info_strings = get_info_from_page(page, dict['url'],xapth_to_load,xpath_to_search_through)  
            #         dict['date_time_string'] = find_and_extract_date_time_string(vote_arrange_info_strings)
            #         final_voted_and_arranged_list_of_dicts.append(dict)
            #         logging.debug(f"finished extracting info for {dict['pair']} realse date: {dict['date_time_string']}")
                    
            #         save_and_update_data_to_file(path_found_pairs_saved, dict)
            #         logging.debug('------------------------------------')
            #         time.sleep(1)

            # except Exception as e:
            #     logging.error(f"when extracting info from voted and arranged tag: {e}")
            #     traceback.print_exc()


        except Exception as e:
            logging.error(f"main function error: {e}")
            traceback.print_exc()

        finally:
            browser.close()



def process_elements_dict_JS(div_elements, include_terms, exclude_terms, base_url, tag=''):
    """
    Processes a list of div elements from playwright and returns a list of dictionaries.

    Args:
        div_elements (list[ElementHandle]): The list of div elements.
        include_terms (list[str]): The list of terms to include.
        exclude_terms (list[str]): The list of terms to exclude.
        base_url (str): The base URL to prepend to href attributes.
        tag (str): An optional tag.

    Returns:
        list[dict]: A list of dictionaries with extracted information.
    """
    
    result = []

    for div in div_elements:
        text_content = div.evaluate('(element) => element.textContent').strip()
        text_content_lower = text_content.lower().strip()
        if any(term.lower() in text_content_lower for term in include_terms) and not any(term.lower() in text_content_lower for term in exclude_terms):
            logging.debug(f"{text_content}")
            href_element = div.query_selector('a')
            href = href_element.get_attribute('href') if href_element else None
            logging.debug(f"href: {href}")
            pair = extract_new_pair_from_headline(text_content)
            logging.debug(f"{pair} is new listing")
            full_url = f"{base_url}{href}" if href else None
            result.append({
                'exchange': "gateio",
                'tag': tag,
                'url': full_url,
                'date_time_string': None,
                'pair': pair + 'USDT',
            })
            logging.debug(f"\n\n")
    logging.debug(f"===========================")
    return result



def process_elements_dict(li_elements, include_terms, exclude_terms, base_url,tag=''):
    """
    Processes a list of li elements from playwright and returns a dictionary with two lists.

    Args:
        li_elements (list[ElementHandle]): The list of li elements.
        include_terms (list[str]): The list of terms to include.
        exclude_terms (list[str]): The list of terms to exclude.
        base_url (str): The base URL to prepend to href attributes.

    Returns:
        dict: A dictionary with two lists: 'urls' and 'pairs'.
    """
    
    result = []

    for li in li_elements:
        text_content = li.inner_text().lower()
        if any(term in text_content for term in include_terms) and not any(term in text_content for term in exclude_terms):
            logging.debug(f"detected string: {li.inner_text()}")
            href_element = li.query_selector('a')
            href = href_element.get_attribute('href') if href_element else None
            pair = extract_new_pair_from_headline(text_content)
            logging.debug(f"{pair} is new listing")
            logging.debug(f"detected string: {li.inner_text()}")
            full_url = f"{base_url}{href}"
            result.append({
            'exchange': "gateio",
            'tag': tag,
            'url': full_url,
            'date_time_string': None,
            'pair': pair+'USDT',
            })

    logging.debug(f"===========================")
    return result

def process_elements(li_elements, include_terms, exclude_terms, base_url):
    """
    Processes a list of li elements from playwright and returns a dictionary with two lists.

    Args:
        li_elements (list[ElementHandle]): The list of li elements.
        include_terms (list[str]): The list of terms to include.
        exclude_terms (list[str]): The list of terms to exclude.
        base_url (str): The base URL to prepend to href attributes.

    Returns:
        dict: A dictionary with two lists: 'urls' and 'pairs'.
    """
    
    result = {
        'urls': [],
        'pairs': []
    }

    for li in li_elements:
        text_content = li.inner_text()
        if any(term in text_content for term in include_terms) and not any(term in text_content for term in exclude_terms):
            href_element = li.query_selector('a')
            href = href_element.get_attribute('href') if href_element else None
            pair = extract_new_pair_from_headline(text_content)
            logging.debug(f"{pair} is new listing")
            logging.debug(f"detected string: {li.inner_text()}")
            full_url = f"{base_url}{href}"
            result['urls'].append(full_url)
            result['pairs'].append(pair)
    logging.debug(f"===========================")
    return result

def extract_new_pair_from_headline(text):
    """
    Extracts the substring within parentheses if all letters are capital letters.
    Args:
        text (str): The input string.
    Returns:
        str: The extracted substring if all letters are capital letters, otherwise None.
    """
    # Use regular expression to find the substring within parentheses
    match = re.search(r'\(([^)]+)\)', text)
    if match:
        substring = match.group(1)
        # Check if all letters in the substring are capital letters
        if substring.isupper():
            return substring
    return None

def find_and_extract_date_time_string(list_of_paragraphs):
    date_time_string = None
    for text in list_of_paragraphs:
        if 'Trading' in text:
            date_time_string = extract_and_convert_datetime(text)
            logging.debug(f"date and time string: {date_time_string}")
    
    return date_time_string


def create_dict_voted_and_arranged(list_of_text,url):

    date_time_string = None
    pair = None
    for text in list_of_text:
        if 'Trading' in text:
            date_time_string = extract_and_convert_datetime(text)
            logging.debug(f"date and time string: {date_time_string}")
            split_pair = text.split('/')
            match = re.search(r'(\w+)/(\w+)', text)
            if match:
                pair = match.group(1) + match.group(2)

    listing_info = {
    "exchange": "MEXC",
    "tag":"voted_and_arranged", 
    "url": url,
    "date_time_string": date_time_string,
    "pair": pair,
    }

    return listing_info


def get_info_from_page(page, full_url,xpath_to_load,xpath_to_search_through):
    page.goto(full_url)
    page.wait_for_selector(xpath_to_load)
    paragraphs = page.query_selector_all(xpath_to_search_through)
    list_of_strings_for_info_extraction = []
    for paragraph in paragraphs:
        logging.debug(f'element found: {paragraph.inner_text()}')
        list_of_strings_for_info_extraction.append(paragraph.inner_text())
    
    return list_of_strings_for_info_extraction
    



def save_and_update_data_to_file(path_found_pairs_saved, listing_info):
    # Create a directory to store the data
    os.makedirs(path_found_pairs_saved, exist_ok=True)

    # Create the file name
    date_time_obj = datetime.strptime(listing_info['date_time_string'], "%b %d, %Y, %I%p")
    formatted_date_str = date_time_obj.strftime("%y-%m-%d") 

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
        #existing_pair_dict['non_usdt_pair'] = listing_info['non_usdt_pair']

        # Write the updated dictionary back to the file
        with open(file_path, 'w') as f:
            json.dump(existing_pair_dict, f, indent=4)
        logging.debug(f"{listing_info['pair']} has been updated.")

    elif file_name not in os.listdir(path_found_pairs_saved):
        # Create a new dictionary for the pair
        with open(file_path, 'w') as f:
            json.dump(listing_info, f, indent=4)
        logging.debug(f"New listing: {listing_info['pair']} has been added.")
    else:
        logging.debug(f'No changes made to {listing_info["pair"]}')
    
def convert_datetime_string(input_string):
    """
    Converts a date-time string from 'YYYY-MM-DD HH:MM (UTC)' format to '%d %B %Y %H:%M' format.
    Args:
        input_string (str): The input date-time string in 'YYYY-MM-DD HH:MM (UTC)' format.
    Returns:
        str: The formatted date-time string in '%d %B %Y %H:%M' format.
    """
    # Extract the date-time part from the input string
    datetime_part = input_string.split(' (UTC)')[0]
    # Parse the date-time string
    parsed_datetime = datetime.strptime(datetime_part, '%Y-%m-%d %H:%M')
    # Format the parsed date-time into the desired format
    formatted_datetime = parsed_datetime.strftime('%d %B %Y %H:%M')

    return formatted_datetime


import re
from datetime import datetime

def extract_and_convert_datetime(input_string):
    # Regular expression to find the date-time part in the format 'YYYY-MM-DD HH:MM'
    datetime_pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}'

    # Search for the date-time part in the input string
    match = re.search(datetime_pattern, input_string)
    if match:
        datetime_part = match.group(0)

        # Parse the date-time string
        parsed_datetime = datetime.strptime(datetime_part, '%Y-%m-%d %H:%M')

        # Format the parsed date-time into the desired format
        if parsed_datetime.minute == 0:
            formatted_datetime = parsed_datetime.strftime('%b %d, %Y, %-I%p')
        else:
            formatted_datetime = parsed_datetime.strftime('%b %d, %Y, %-I:%M%p')

        return formatted_datetime

    return None


if __name__ == "__main__":
    main()