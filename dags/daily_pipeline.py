# DAG to extract daily, monthly and weekly data from AlphaVantage
import os
import requests
import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
API_KEY = os.getenv("API_KEY")
DB_URL = os.getenv("DB_URL")
default_args = {
    'owner': 'deecodes',
    'retries': 5,
    'retry_delay': timedelta(minutes=3)
}

@dag(dag_id='extract_daily_data', default_args=default_args, start_date=datetime(2025, 5, 13), schedule_interval='@daily', catchup=False)
def extract_data():
    # Extract data from API
    @task
    def extract_daily_data():
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey={API_KEY}"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            return data # Returns a dictionary, unclean data
        else:
            print(f"There is an error. Code: {response.status_code}")

    # Transform data
    @task
    def transform_daily_data(data):
        try:
            daily_date = data['Meta Data']['3. Last Refreshed'] # Get refresh date
            today = data['Time Series (Daily)'][daily_date] # Get data
            today_data = {
                "Date": [daily_date],
                "Open": [today["1. open"]],
                "High": [today["2. high"]],
                "Low": [today["3. low"]],
                "Close": [today["4. close"]],
                "Volume": [today["5. volume"]]
            }
            # Transform into pd.DataFrame, use try...catch... block to catch any errors
            try:
                df = pd.DataFrame(today_data)
                print(df.info())
                df["Date"] = pd.to_datetime(df["Date"]) # sets "Date" column as a datetime object
                df.set_index("Date", inplace=True)# sets "Date" column as the index
                df.astype({"Open": "float", "High": "float", "Low": "float", "Close": "float", "Volume": "float"}) #Change column dtypes
                print(df.dtypes)
                return df

            except Exception as e:
                print(f"DataFrame error: {str(e)}")


        except Exception as e:
            print(f"requests Error: {str(e)}")
    # Loading task
    @task
    def load_to_db(df):
        try:
           engine = create_engine(url=DB_URL)
           df.to_sql(name='daily_stock_data', con=engine, if_exists='append') #append data to db if table exists
           print("Data loaded into table successfully")
        except Exception as e:
            print(f"Loading DataFrame error: {str(e)}")

    data = extract_daily_data()
    df = transform_daily_data(data)
    load_to_db(df)

data_dag = extract_data()
