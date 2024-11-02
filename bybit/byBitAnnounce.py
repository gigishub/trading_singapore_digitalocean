from datetime import datetime
from pybit.unified_trading import HTTP
import requests
import json
import re
import time
import os 
import traceback

# 01/08/2024
# cronjob 
# */15 * * * * /usr/bin/python3 /root/runningcode/byBitAnnounce.py >> /root/cronlogs/byBitAnnounce.log 2>&1


def main():
    try:
        # Create a session object
        session = create_session_bybit(max_retries=20, test=False)

        # Get the announcements data  
        announcements = session.get_announcement(type='new_crypto', tags=['Spot','Spot Listings'], locale="en-US", limit=5)

        # get first position from new crypto listings 
        # only first one because scrapy cant handle looping needs improvement 
        positionInList = 0
        # Extract the details from the first announcement
        title = announcements['result']['list'][positionInList]['title']
        tags = announcements['result']['list'][positionInList]['tags']
        url = announcements['result']['list'][positionInList]['url']
        announcements_published = datetime.fromtimestamp(announcements['result']['list'][positionInList]['publishTime'] / 1000)


        #checking if the announcement is a spot listing
        token_publishTime_dict = process_listing_announcement(title, tags, announcements_published)

        if token_publishTime_dict is not None and f"{token_publishTime_dict['pair']}.json" in os.listdir('new_pair_data'):
            print(f"{token_publishTime_dict['pair']} already exists")
        # continue if the announcement is a spot listing
        elif token_publishTime_dict is not None:
            print(f"New token pair {token_publishTime_dict['pair']} found")
            # get the listing date and time from the specific announcement page
            # uses scrpay here better somethong else 

            listing_date_time_dict= get_listing_info(url)

            # combine the two dictionaries
            # listing time and date and token pair
            listing_date_time_dict.update(token_publishTime_dict)
            
            # save the data to a json file
            dir_name    = 'new_pair_data'
            os.makedirs(dir_name,exist_ok=True)
            file_name_to_safe = get_token_pair(title)
            file_path   = os.path.join(dir_name,file_name_to_safe)
            with open(f'{file_path}.json', 'w') as f:
                json.dump(listing_date_time_dict, f, indent=4)
            time.sleep(10)
            print(f"New token pair {token_publishTime_dict['pair']} added\n")
        else:
            print('first announcement in list is not a spot listing')
    except Exception as e:
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print(f"An error occurred: {e}")
        traceback.print_exc()
        print()


def process_listing_announcement(title, tags, announcements_published):
    """
    Processes a listing announcement to extract and format relevant information.

    This function checks if the announcement is for a 'New Listing' that is also a 'Spot' or 'Spot Listings'.
    If it is, it extracts the token pair from the title and formats the published datetime into a tuple.
    If the announcement does not match the criteria, it prints a message indicating it's not a Spot Listing.

    Parameters:
    - title (str): The title of the announcement.
    - tags (list of str): A list of tags associated with the announcement.
    - announcements_published (datetime): The datetime object representing when the announcement was published.

    Returns:
    - dict: A dictionary containing the token pair and the formatted published datetime if the announcement
            matches the criteria. Returns None otherwise.
    """
    if 'New Listing' in title and ('Spot' and 'Spot Listings') in tags:
        newPair = get_token_pair(title)
        # Extracting the components individually
        datetime_tuple = (announcements_published.year, announcements_published.month, announcements_published.day,
                          announcements_published.hour, announcements_published.minute, announcements_published.second)
        pair_dict = {'pair': newPair, 'published_datetime': datetime_tuple}
        return pair_dict
    else:       
        return None

def get_listing_info(url):
    """
    Fetches and processes listing information from a Bybit announcement webpage.

    This function retrieves the listing announcement content from the specified URL,
    extracts the listing time, and converts it into a dictionary format suitable for
    scheduling tasks with apscheduler.

    Parameters:
    - url (str): The URL of the Bybit announcement webpage to scrape.

    Returns:
    - dict: A dictionary containing the listing time information, formatted for apscheduler.
            Returns None if any step of the process fails, such as if the content cannot be
            retrieved or the listing time cannot be found or converted.

    Note:
    - This function assumes the webpage's content and structure are consistent with the
      expected format for successful scraping and parsing.
    """
    from datetime import datetime
    # Get string with listing info from bybit announcement webpage
    content_str = str_to_search_for_listingTime(url)
    #print(content_str)
    if content_str is None:
        return None  # or handle error appropriately

    # Get listing time from string
    listing_dateTime = find_listing_dateTime_in_str(content_str, preceding_chars=80)
    print(listing_dateTime)
    if not listing_dateTime:
        print('date time not found') # or handle error appropriately

    # Convert listing time to dictionary for apscheduler
    dateTime_dict = convert_dateTime(listing_dateTime[0])
    print(dateTime_dict)
    if dateTime_dict is None:
        return None  # or handle error appropriately
    
    dateTime_dict.update({'url':url})
    dateTime_dict.update({'date_time_string':listing_dateTime[0]})
    
    return dateTime_dict
    

def str_to_search_for_listingTime(url):

    import scrapy
    from scrapy.crawler import CrawlerProcess
    import re
    import json
    global data_collected  # Define a global list to store data
    data_collected = []

    class MySpider(scrapy.Spider):
        name = 'myspider'
        start_urls = [url]

        def parse(self, response):
            script_content = response.css('script#__NEXT_DATA__::text').get()
            # Use a regular expression to extract the JSON string
            json_str = re.search(r'({.*})', script_content, re.DOTALL).group(1)

            # Parse the JSON string into a Python dictionary
            data = json.loads(json_str)
            # Append the data to the global list
            data_collected.append(data)

    process = CrawlerProcess({
        'LOG_LEVEL': 'ERROR',  # Minimize logging output
    })
    process.crawl(MySpider)
    process.start()  # Blocks here until the spider is finished

    return str(data_collected)  # Return the collected data


def find_listing_dateTime_in_str(text, preceding_chars=80):

    """
    Finds matches of a specific date and time format in the given text,
    including a specified number of characters preceding each match.

    Parameters:
    - text (str): The text to search through.
    - preceding_chars (int): The number of characters preceding the match to include.

    Returns:
    - list of str: Matches with their preceding characters.
    """
    import re
    # regex to find pattern looking like Jun 24, 2024, 8AM UTC
    pattern_for_dateTime = r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{1,2}, \d{4}, \d{1,2}(?:AM|PM)'
    # Initialize list to store findings with preceding characters
    matches_with_preceding = []


    # The re.finditer function returns an iterator yielding match objects for all non-overlapping matches.
    for match in re.finditer(pattern_for_dateTime, text):
        # Retrieve the start position of the current match within 'text'.
        # This is the index of the first character of the match in the text.
        start_pos = match.start()
        
        # Calculate the start position for extracting preceding characters.
        # 'preceding_chars' is a variable that determines how many characters before the match should be included.
        # The max function ensures that this position is not less than 0, preventing an IndexError.
        preceding_start_pos = max(0, start_pos - preceding_chars)
        
        # Extract the substring from 'text' that includes the specified number of characters preceding the match,
        # followed by the match itself. This is achieved by slicing 'text' from 'preceding_start_pos' to 'start_pos'
        # and then concatenating the result with the matched text (match.group(0)).
        # match.group(0) returns the entire match.
        preceding_and_match = text[preceding_start_pos:start_pos] + match.group(0)
        
        # Append the extracted substring (preceding characters + match) to the list 'matches_with_preceding'.
        matches_with_preceding.append(preceding_and_match)

    # find string that hast the liting time in it with regex 
    pattern_for_listing_str = r'Listing'
    for match in matches_with_preceding:
        if re.search(pattern_for_listing_str, match, re.IGNORECASE):
            # singlie out only the date and time string with the regex from beginning of function 
            listing_string = re.findall(pattern_for_dateTime, match)
            return listing_string

def convert_dateTime(date_str):
    """
    Parses a date string and extracts the day of the week, hour, minute, and second.

    Parameters:
    - date_str (str): The date string to parse.

    Returns:
    - dict: A dictionary containing the day of the week, hour, minute, and second.

    """
    from datetime import datetime
    import calendar

    # Convert the string to a datetime object
    dt = datetime.strptime(date_str, '%b %d, %Y, %I%p')

    # Extract the day of the week as an integer (Monday is 0 and Sunday is 6)
    day_of_week_int = dt.weekday()

    # Convert the day of the week into the required three-letter string
    day_of_week_str = str(calendar.day_name[day_of_week_int][:3].lower())
    
    # Extract hour, minute, and second
    hour = dt.hour  # Hour in 24-hour format
    minute = dt.minute
    second = dt.second

    # Return the extracted values in a dictionary
    return {
        "day_of_week": day_of_week_str,
        "hour": hour,
        "minute": minute,
        "second": second
    }



# get pair from title function 
def get_token_pair(title):
    """
    Extracts the token pair from the given title.
    Args:
        title (str): The title from which to extract the token pair.
    Returns:
        str: The extracted token pair.
    """
    words = title.split()
    for word in words:
        if '/' in word:
            pair = word.replace('/', '') 
            return pair
        # else:
        #     pattern = r'\(([A-Z]+)\)'
        #     match = re.search(pattern, title)
        #     if match:
        #         return match.group(1)  # Returns the matched token symbol



def create_session_bybit(max_retries=100, test=True):
    """
    Creates a session with Bybit's API.

    Parameters:
    max_retries (int, optional): The maximum number of times to retry creating the session if a ReadTimeout occurs. Defaults to 5.
    test (bool, optional): Whether to create a testnet session or a live session. Defaults to True (testnet).

    Returns:
    HTTP: The created session, or None if the session could not be created after max_retries attempts.
    """
    from pybit.unified_trading import HTTP
    from requests.exceptions import ReadTimeout
    import time
    from requests.exceptions import RequestException
    # If test is True, create a testnet session
    if test:
        for _ in range(max_retries):
            try:
                # Attempt to create the session
                session = HTTP(testnet=True,
                               api_key="YDpyCNtMWiNfKzvRtf",
                               api_secret="Tan1dxTFiTxTOIKtiHJu0Rub3q0n0JxlsQJ5")
                # If the session is created successfully, break the loop and return the session
                return session
            except ReadTimeout:
                # If a ReadTimeout occurs, print a message and retry
                print("ReadTimeout occurred, retrying...")
            except RequestException as e:
                # If a RequestException occurs, print a message and retry
                print(f"RequestException occurred: {e}, retrying...")
                time.sleep(2)
        else:
            # If the session could not be created after max_retries attempts, print a message and return None
            print(f"Failed to create session after {max_retries} retries")
            return None
    # If test is False, create a live session
    else:
        for _ in range(max_retries):
            try:
                # Attempt to create the session
                session = HTTP(testnet=False,
                               api_key="lZ9fMZoU1oTR5fWBNP",
                               api_secret="kcI4mAoQhd32gjXMPkEA2cjuyNC0rR0WHrpe")
                # If the session is created successfully, break the loop and return the session
                return session
            except ReadTimeout:
                # If a ReadTimeout occurs, print a message and retry
                print("ReadTimeout occurred, retrying...")
            except RequestException as e:
                # If a RequestException occurs, print a message and retry
                print(f"RequestException occurred: {e}, retrying...")
                time.sleep(2)
        else:
            # If the session could not be created after max_retries attempts, print a message and return None
            print(f"Failed to create session after {max_retries} retries")
            return None




# Check if the script is being run directly
if __name__ == "__main__":
    main()

