import os
import requests
import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
API_KEY = os.getenv("API_KEY")
DB_URL= os.getenv("DB_URL")
default_args = {
    'owner': 'deecodes',
    'retries': 5,
    'retry_delay': timedelta(minutes=3)
}

@dag(dag_id='extract_weekly_data', default_args=default_args, schedule='@weekly', start_date=datetime(2025, 5, 13))
def extract_weekly_data():
    @task
    def extract_data():
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=IBM&apikey={API_KEY}'
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            return data # returns a dictionary, unclean data
        else:
            print(f"Error: {response.status_code}")
    
    @task
    def transform_data(data):
        weekly_close = data['Meta Data']['3. Last Refreshed']
        week = data["Weekly Adjusted Time Series"][weekly_close]
        df = pd.DataFrame({
            "Date": weekly_close,
            "Open": [week["1. open"]],
            "High": [week["2. high"]],
            "Low": [week["3. low"]],
            "Close": [week["4. close"]],
            "Adjusted Close": [week["5. adjusted close"]],
            "Volume": [week["6. volume"]],
        })

        df["Date"] = pd.to_datetime(df["Date"])
        df.astype({"Open": "float", "High": "float", "Low": "float", "Close": "float", "Adjusted Close": "float", "Volume": "float"})
        df.set_index("Date", inplace=True) # Set data as the index
        return df # Return a cleaned df
    
    @task
    def load_to_db(df):
        try:
            engine = create_engine(url=DB_URL)
            df.to_sql(name="weekly_stock_data", con=engine, if_exists='append')
            print("Data loaded into database successfully!") # Append data if table exists
        except Exception as e:
            print(f"Load data error: {str(e)}")

    data = extract_data()
    df = transform_data(data)
    load_to_db(df)

weekly_dag = extract_weekly_data()

            
