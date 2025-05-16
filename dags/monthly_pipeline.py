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
    'retry_delay': timedelta(minutes=3),
    'retries': 5
}

@dag(dag_id='monthly_data_pipeline_dag', default_args=default_args, start_date=datetime(2025, 5, 14), schedule='@monthly', catchup=False)
def extract_monthly_data():
    @task
    def extract_data():
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY_ADJUSTED&symbol=IBM&apikey={API_KEY}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data # returns unclean data in JSON form
        else:
            print(f"Error: {response.status_code}")

    @task
    def transform_data(data):
        monthly_close = data['Meta Data']['3. Last Refreshed'] # Refresh date
        month = data["Monthly Adjusted Time Series"][monthly_close]
        df = pd.DataFrame({
            'Date': monthly_close,
            'Open': [month["1. open"]],
            'High': [month["2. high"]],
            'Low': [month["3. low"]],
            'Close': [month["4. close"]],
            'Adjusted Close': [month["5. adjusted close"]],
            'Volume': [month["6. volume"]],
        })

        df["Date"] = pd.to_datetime(df["Date"])
        df.astype({"Open": "float", "High": "float", "Low": "float", "Close": "float", "Adjusted Close": "float", "Volume": "float"})
        df.set_index("Date", inplace=True) # Set data as the index
        return df
    
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

monthly_dag = extract_monthly_data()
