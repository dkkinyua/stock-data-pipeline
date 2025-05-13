# DAG to extract daily, monthly and weekly data from AlphaVantage
import os
import json
import requests
import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")
default_args = {
    'owner': 'deecodes',
    'retries': 5,
    'retry_delay': timedelta(minutes=3)
}

print(API_KEY)


@dag(dag_id='extract_daily_data', default_args=default_args, start_date=datetime(2025, 5, 13), schedule_interval='@daily', catchup=False)
def extract_data():
    @task
    def extract_daily_data():
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey={API_KEY}"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            date_today = data['Meta Data']['3. Last Refreshed']
            today_data = {
                'Date': date_today,
                'Open': data['Time Series (Daily)'][date_today]['1. open'],
                'High': data['Time Series (Daily)'][date_today]['2. high'],
                'Low': data['Time Series (Daily)'][date_today]['3. low'],
                'Close': data['Time Series (Daily)'][date_today]['4. close'],
                'Volume': data['Time Series (Daily)'][date_today]['5. volume']
            }

            filepath = '/home/deecodes/stock-data-pipeline/daily_data.json'

            try:
                if os.path.exists(filepath):
                    with open(filepath, 'r') as f:
                        daily_data = json.load(f)
                else:
                    daily_data = []

                # Append today's data
                daily_data.append(today_data)

                with open(filepath, 'w') as f:
                    json.dump(daily_data, f, indent=2)

                print("Data added to file successfully!")

            except Exception as e:
                print(f"Error: {e}")
        else:
            print(f"There is an error. Code: {response.status_code}")

    extract_daily_data()

data_dag = extract_data()
