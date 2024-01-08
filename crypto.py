import requests
from datetime import datetime, timedelta
import pandas as pd

def fetch_token_data(coin):
    url = f'https://api.coingecko.com/api/v3/coins/{coin}/market_chart?vs_currency=usd&days=7'
    params = {
        'vs_currency': 'usd'
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        return data['prices']
    else:
        print(f'Failed to fetch data for {token}. Status code: {response.status_code}')
        return []

# List of top ten tokens
top_ten = ['bitcoin', 'ethereum', 'tether', 'binancecoin', 'solana', 'ripple','usd-coin', 'staked-ether','cardano', 'avalanche-2']
exchange_rate = []

# Fetch and print hourly exchange rate data for each token
for token in top_ten:
    token_data = fetch_token_data(token)
    if token_data:
        print(f'Token: {token}')
        prices = pd.DataFrame(token_data, columns=['date', 'price'])
        prices['time'] = prices['date']
        prices['date'] = prices.date.apply(lambda x: datetime.fromtimestamp(x // 1000)).dt.floor('h')
        prices['name'] = token
        exchange_rate.append(prices)
    else:
        print(f'No data available for {token.capitalize()}')
rates = pd.concat(exchange_rate, ignore_index=True)
rates.to_csv('token.csv', index=False)


