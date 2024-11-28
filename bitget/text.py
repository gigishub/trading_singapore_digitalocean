import random
from playwright.sync_api import sync_playwright

def get_page_text(url, proxy, user_agent):
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
        )
        context = browser.new_context(
            user_agent=user_agent
        )
        page = context.new_page()
        page.goto(url)
        
        # Retrieve all text from the page
        page_text = page.inner_text('body')
        
        browser.close()
        return page_text

# URL to retrieve text from
url = "https://www.bitget.com/en/support/articles/12560603818893"

# List of free proxy servers
proxy_list = [
    "http://106.14.91.83:8443",
    "http://8.211.49.86:8081",
    "http://117.54.114.35:80",
    "http://47.105.122.72:8008",
    "http://189.193.254.98:8080",
    "socks4://201.217.245.193:60606",
    "socks4://154.73.45.58:60606",
    "socks4://103.5.3.41:1080",
    "socks4://120.26.52.35:3128",
    "socks5://85.143.254.38:1080"
]

# Randomly select a proxy from the list
proxy = random.choice(proxy_list)

# Custom user agent
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# Get the text from the page using the specified proxy and user agent
try:
    page_text = get_page_text(url, proxy, user_agent)
    print(f"Successfully retrieved text using proxy: {proxy}")
    print(page_text)
except Exception as e:
    print(f"Failed to retrieve text using proxy {proxy}. Error: {str(e)}")