import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import os

import sqlalchemy
load_dotenv()

# load env variables
db_host = os.environ["INSTANCE_HOST"]
db_user = os.environ["POSTGRES_USER"]
db_pass = os.environ["POSTGRES_PW"]
db_name = os.environ["POSTGRES_DB"]
db_port = os.environ["POSTGRES_PORT"]


top_ten = ['bitcoin', 'ethereum', 'tether', 'binancecoin', 'solana', 'ripple','usd-coin', 'staked-ether','cardano', 'avalanche-2']
connection_str = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
# SQLAlchemy engine
engine = create_engine(connection_str)

try:
    with engine.connect() as connection_str:
        print('Successfully connected to the PostgreSQL database')
except Exception as ex:
    print(f'Sorry failed to connect: {ex}')

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
        print(f'Failed to fetch data for {coin}. Status code: {response.status_code}')
        return []


# List of top ten tokens
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
# rates.to_csv('token.csv', index=False)

rates.to_sql('exch', con=engine, if_exists="replace", index=False)

