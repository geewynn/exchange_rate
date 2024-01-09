import os
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

# Load database connection details from environment variables
db_host = os.environ["INSTANCE_HOST"]
db_user = os.environ["POSTGRES_USER"]
db_pass = os.environ["POSTGRES_PW"]
db_name = os.environ["POSTGRES_DB"]
db_port = os.environ["POSTGRES_PORT"]

# List of top ten tokens on CoinGecko
top_ten = ['bitcoin', 'ethereum', 'tether', 'binancecoin', 'solana',
           'ripple', 'usd-coin', 'staked-ether', 'cardano', 'avalanche-2']

# Create connection string for SQLAlchemy
connection_str = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
# Create SQLAlchemy engine
engine = create_engine(connection_str)

# Set up logging
logging.basicConfig(filename='extract_rate_data.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    with engine.connect() as connection_str:
        logging.info('Successfully connected to the PostgreSQL database')
except Exception as ex:
    logging.error(f'Failed to connect: {ex}')

def fetch_token_data(coin: str, days: int) -> list:
    """
    Fetches historical market chart data for a cryptocurrency from CoinGecko API.

    Args:
    - coin (str): The name of the cryptocurrency.
    - days (int): The number of days of historical data to fetch.

    Returns:
    - list: A list of tuples containing timestamp and price data.
    """
    url = f'https://api.coingecko.com/api/v3/coins/{coin}/market_chart?vs_currency=usd&days={days}'
    params = {
        'vs_currency': 'usd'
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        return data['prices']
    else:
        logging.error(f'Failed to fetch data for {coin}. Status code: {response.status_code}')
        return []

def extract_rate_data(days: int) -> None:
    """
    Extracts hourly exchange rate data for the top ten cryptocurrencies from CoinGecko,
    processes the data, and stores it in a PostgreSQL database.

    Args:
    - days (int): The number of days of historical data to fetch.

    Returns:
    - None
    """
    exchange_rate = []

    # Fetch hourly exchange rate data for each token
    for token in top_ten:
        token_data = fetch_token_data(token, days)
        if token_data:
            logging.info(f'Token: {token}')
            # Create a DataFrame from the fetched data
            prices = pd.DataFrame(token_data, columns=['date', 'price'])
            # Convert timestamp to datetime and round to the nearest hour
            prices['time'] = prices['date']
            prices['date'] = prices['date'].apply(
                lambda x: datetime.fromtimestamp(x // 1000)).dt.floor('h')
            prices['name'] = token
            exchange_rate.append(prices)
        else:
            logging.warning(f'No data available for {token.capitalize()}')

    # Combine data for all tokens into a single DataFrame
    rates = pd.concat(exchange_rate, ignore_index=True)

    # Store the data in the PostgreSQL database
    rates.to_sql('exch', con=engine, if_exists="replace", index=False)

if __name__ == '__main__':
    # usage: fetch hourly exchange rate data for the past 7 days
    extract_rate_data(7)
