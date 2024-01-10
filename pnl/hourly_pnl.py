import os
from typing import Any, Dict, List, Tuple
import psycopg2
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

load_dotenv()

API_KEY = os.environ.get('X_API_KEY')
WALLET_ADDRESS=os.environ.get('WALLET_ADDRESS')
print(WALLET_ADDRESS)
db_host = os.environ["INSTANCE_HOST"]
db_user = os.environ["POSTGRES_USER"]
db_pass = os.environ["POSTGRES_PW"]
db_name = os.environ["POSTGRES_DB"]
db_port = 5432

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_wallet_balance(wallet_address: str) -> Dict[str, Any]:
    """
    Get the wallet balance for a given wallet address.

    Args:
    - wallet_address (str): The wallet address for which to retrieve the balance.

    Returns:
    - dict: A dictionary containing the wallet balance data.
    """
    url = 'https://api.allium.so/api/v1/explorer/queries/UWHFUe3BPTFpd7EDVIiI/run'
    headers = {
        'Content-Type': 'application/json',
        'X-API-KEY': API_KEY
    }
    data = {
        'address': wallet_address
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        data = response.json()
        return data['data']
    else:
        logging.error(f'Failed to fetch wallet balance for address {wallet_address}. Status code: {response.status_code}')
        return {}

def connect_to_db():
    connection_str = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    # Create SQLAlchemy engine
    engine = create_engine(connection_str)

    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    try:
        with engine.connect() as connection_str:
            print('Successfully connected to the PostgreSQL database')
            logging.info('Successfully connected to the PostgreSQL database')
        return engine
    except Exception as ex:
        print(f'Failed to connect: {ex}')
        logging.error(f'Failed to connect: {ex}')

def get_exchange_rates() -> List[Tuple[datetime, float, datetime, str]]:
    """
    Get exchange rates up to a specified timestamp.

    Returns:
    - list of tuples: A list of tuples containing exchange rate data.
    """
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user, 
            password=db_pass, 
            host=db_host 
        )
        with conn.cursor() as cursor:
            query = "SELECT date, price, time, name FROM exch;"
            cursor.execute(query)
            rows = cursor.fetchall()
        conn.close()
        return rows
    except Exception as e:
        logging.error(f'Failed to fetch exchange rates: {e}')
        return []


def calculate_total_balance(timestamp: datetime, wallet_balances: List[Dict[str, Any]], exchange_rates: List[Tuple[datetime, float, datetime, str]]) -> float:
    """
    Calculate the total balance in USD for a given timestamp.

    Args:
    - timestamp (datetime): The timestamp for which to calculate the total balance.
    - wallet_balances (list of dict): List of wallet balances.
    - exchange_rates (list of tuples): List of tuples containing exchange rate data.

    Returns:
    - float: The total balance in USD.
    """
    total_balance_usd = 0
    for balance in wallet_balances:
        block_timestamp = datetime.strptime(balance["block_timestamp"], "%Y-%m-%dT%H:%M:%S")
        if timestamp <= block_timestamp < timestamp + timedelta(hours=1):
            token_id = balance["token_id"]
            matching_rates = [
                rate[1]
                for rate in exchange_rates
                if rate[3] == token_id and rate[0] <= timestamp
            ]
            token_exchange_rate = matching_rates[0] if matching_rates else None

            if token_exchange_rate is None or token_exchange_rate == 0:
                token_exchange_rate = {
                    "usd-coin": 0.99,
                    "ethereum": 2300,
                    "tether": 1.00
                }.get(token_id, 0)  # Set the rate for tokens not found in the exchange_rates

            token_balance_usd = balance["balance"] * token_exchange_rate
            total_balance_usd += token_balance_usd
    return total_balance_usd


def calculate_hourly_pnl(wallet_address: str) -> pd.DataFrame:
    """
    Calculate the hourly profit and loss for the past 7 days for a given wallet address.

    Args:
    - wallet_address (str): The wallet address for which to calculate the hourly PnL.

    Returns:
    - pandas.DataFrame: A DataFrame containing the hourly PnL data.
    """
    engine = connect_to_db()
    wallet_balances = get_wallet_balance(wallet_address)
    exchange_rates = get_exchange_rates()
    end_time = datetime.now()
    start_time = end_time - timedelta(days=7)
    hourly_pnl = []
    for current_time in (start_time + timedelta(hours=i) for i in range(7 * 24)):
        total_balance = calculate_total_balance(current_time, wallet_balances, exchange_rates)
        previous_balance = calculate_total_balance(current_time - timedelta(hours=1), wallet_balances, exchange_rates)
        pnl = previous_balance - total_balance if previous_balance != 0 else 0
        hourly_pnl.append((current_time, pnl))
    df = pd.DataFrame(hourly_pnl, columns=["Timestamp", "Hourly PnL"])
    print('writing resutl to db')
    df.to_sql(f"{wallet_address}_hourly_pnl", con=engine, if_exists="replace", index=False)
    # df.to_csv(f"{wallet_address}_hourly_pnl.csv", index=False)

    return df

if __name__ == '__main__':
    wallet_address = WALLET_ADDRESS
    calculate_hourly_pnl(wallet_address)
