import json 
from datetime import datetime
import os
import time
import re


def main():

    #directory_path = '/root/test_data'
    directory_path = '/root/new_pair_data'
    #directory_path = 'new_pair_data'
    
    json_files = [f for f in os.listdir(directory_path) if f.endswith('.json')]

    for json_file in json_files:
        json_file_path = os.path.join(directory_path, json_file)
        with open(json_file_path, 'r') as f:
            token_dict = json.load(f)

        # get the time remaining to listing
        # !!! needs to cominbined for just input dict and out put remaining time !!!!
        date_time_dict             = parse_date_time_string(token_dict['date_time_string'])
        time_to_listing_seconds    = time_until_listing_seconds(date_time_dict)
        time_to_listing_hours      = time_to_listing_seconds / 3600
        
        #update listing if time to listing is less than 2 hours
        if 0.24 < time_to_listing_hours < 2:
            print('more than 15min less than 2h')
            token_dict.update(get_listing_info(token_dict))
            
            #save updated data 
            dir_name    = 'new_pair_data'
            os.makedirs(dir_name,exist_ok=True)
            file_name_to_safe = token_dict['pair']
            file_path   = os.path.join(dir_name,file_name_to_safe)
            with open(f'{file_path}.json', 'w') as f:
                json.dump(token_dict, f, indent=4)
            print(token_dict)
            
            print(f"listing info updated for {token_dict['pair']}")
    
    print('no listing close')
        








def get_listing_info(token_dict):
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
    token_dict = token_dict

    # Get string with listing info from bybit announcement webpage
    content_str = str_to_search_for_listingTime(token_dict['url'])
    if content_str is None:
        return None  # or handle error appropriately

    # Get listing time from string
    listing_dateTime_list = find_listing_dateTime_in_str(content_str, preceding_chars=80)
    listing_dateTime = listing_dateTime_list[0]
    print(listing_dateTime)
    if not listing_dateTime:
        return None  # or handle error appropriately

    # Convert listing time to dictionary for apscheduler
    dateTime_dict = convert_dateTime(listing_dateTime)
    if dateTime_dict is None:
        return None  # or handle error appropriately

    token_dict.update({'date_time_string':listing_dateTime})
    token_dict.update(dateTime_dict)
    return token_dict


    

        # save the data to a json file
    dir_name    = 'new_pair_data'
    os.makedirs(dir_name,exist_ok=True)
    file_name_to_safe = token_dict['pair']
    file_path   = os.path.join(dir_name,file_name_to_safe)
    with open(f'{file_path}.json', 'w') as f:
        json.dump(dateTime_dict, f, indent=4)
    

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



def time_until_listing_seconds(date_time_dict):
    """
    Calculate the time difference in seconds between the current time and a specified time in dictonary.

    Args:
        date_time_dict (dict): A dictionary containing the target date and time with the following keys:
            - "year" (int): The year of the target date.
            - "month" (int): The month of the target date.
            - "day" (int): The day of the target date.
            - "hour" (int): The hour of the target time.
            - "minute" (int): The minute of the target time.
            - "second" (int): The second of the target time.

    Returns:
        float: The time difference in seconds between the current time and the target date and time.
    """
    # Define the target token and date and time
    year = date_time_dict["year"]
    month = date_time_dict["month"]
    day = date_time_dict["day"]
    hour = date_time_dict["hour"]
    minute = date_time_dict["minute"]
    second = date_time_dict["second"]
    
    # Calculate the time difference in seconds
    time_to_listing_seconds = (datetime(year, month, day, hour, minute, second) - datetime.now()).total_seconds()
    
    return time_to_listing_seconds


def parse_date_time_string(date_time_string):
    """
    Parse a date and time string into its components.

    The function expects a date and time string in the format:
    "Mon DD, YYYY, HH:MM:SSAM/PM" or "Mon DD, YYYY, HH:MMAM/PM"

    Args:
        date_time_string (str): The date and time string to parse.

    Returns:
        dict: A dictionary containing the parsed components:
            - "year" (int): The year.
            - "month" (int): The month number (1-12).
            - "day" (int): The day of the month.
            - "hour" (int): The hour (24-hour format).
            - "minute" (int): The minute.
            - "second" (int): The second.
            If the format is incorrect, returns an empty dictionary.
    """
    # Regular expression to match the date and time components
    regex = r"(\w{3}) (\d{1,2}), (\d{4}), (\d{1,2})(?::(\d{2}))?(?::(\d{2}))?(\w{2})"
    
    match = re.match(regex, date_time_string)
    if match:
        month_str, day, year, hour, minute, second, am_pm = match.groups()
        
        # Convert month abbreviation to month number
        datetime_obj = datetime.strptime(month_str, "%b")
        month = datetime_obj.month
        
        # Convert year, day, and hour to integers
        year = int(year)
        day = int(day)
        hour = int(hour) % 12 + (12 if am_pm.upper() == "PM" else 0)
        
        # Check if minute and second are present, else default to 0
        minute = int(minute) if minute else 0
        second = int(second) if second else 0
        
        # Return the components as a dictionary
        return {
            "year": year,
            "month": month,
            "day": day,
            "hour": hour,
            "minute": minute,
            "second": second
        }
    else:
        print("Date time format is incorrect")
        # Return an empty dictionary or handle the incorrect format as needed
        return {}

if __name__ == "__main__":
    main()