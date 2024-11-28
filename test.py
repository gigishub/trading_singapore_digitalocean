import requests
import json




def token_info(basecoin):
    # Define the API endpoint
    url = 'https://api.kucoin.com/api/v1/symbols'

    # Send a GET request to the endpoint
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        symbols = data['data']

        
        # Find the minimal order size for a specific token pair
        pair = f'{basecoin}-USDT'  # Replace with your token pair
        for symbol in symbols:
            if symbol['symbol'] == pair:
                print(f"Pair: {pair}")
                return json.dumps(symbol,indent =4)
    else:
        print(f"Failed to retrieve data")
        return response.status_code

# Call the function
print(token_info('SDM'))