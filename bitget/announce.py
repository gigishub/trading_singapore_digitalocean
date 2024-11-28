import requests
import datetime

# Bitget API base URL
BASE_URL = "https://api.bitget.com"

# Endpoint for announcements
ANNOUNCEMENTS_ENDPOINT = "/api/v2/public/annoucements"

# Set up parameters
params = {
    "language": "en_US",
    "annType": "coin_listings",  # Specifically for new listings
    "startTime": int((datetime.datetime.now() - datetime.timedelta(days=30)).timestamp() * 1000),  # 30 days ago
    "endTime": int(datetime.datetime.now().timestamp() * 1000)  # Current time
}

# Make the API request
response = requests.get(BASE_URL + ANNOUNCEMENTS_ENDPOINT, params=params)

# Check if the request was successful
if response.status_code == 200:
    announcements = response.json()["data"]
    
    # Process each announcement
    for announcement in announcements:
        print(f"Title: {announcement}")
        # print(f"Description: {announcement['description']}")
        # print(f"Created at: {datetime.datetime.fromtimestamp(announcement['createdTime']/1000)}")
        # print(f"URL: {announcement['url']}")
        # print("---")
else:
    print(f"Error: {response.status_code}")
    print(response.text)