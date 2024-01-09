import os
import psycopg2
import requests
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

API_KEY=os.environ.get('X_API_KEY')
def get_wallet_balance(wallet_address):
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
    data = response.json()
    return data['data']


def get_exchange_rates():
    """
    Get exchange rates up to a specified timestamp.

    Args:
    - timestamp (datetime): The timestamp up to which to retrieve exchange rates.

    Returns:
    - list of tuples: A list of tuples containing exchange rate data.
    """
    conn = psycopg2.connect(
        dbname=os.environ.get('POSTGRES_DB'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PW'),
        host=os.environ.get('INSTANCE_HOST')
    )
    with conn.cursor() as cursor:
        query = "SELECT date, price, time, name FROM exch;"
        cursor.execute(query)
        rows = cursor.fetchall()
    conn.close()
    return rows


def calculate_total_balance(timestamp, wallet_balances, exchange_rates):
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


def calculate_hourly_pnl(wallet_address):
    """
    Calculate the hourly profit and loss for the past 7 days for a given wallet address.

    Args:
    - wallet_address (str): The wallet address for which to calculate the hourly PnL.

    Returns:
    - pandas.DataFrame: A DataFrame containing the hourly PnL data.
    """
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
    df.to_csv(f"{wallet_address}_hourly_pnl.csv", index=False)

    return df


wallet_address = "0x26a016De7Db2A9e449Fe5b6D13190558d6bBCd5F"
calculate_hourly_pnl(wallet_address)
