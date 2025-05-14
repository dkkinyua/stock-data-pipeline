import os
import json
import requests
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

@dag(dag_id='extract_weekly_data', default_args=default_args, schedule='@weekly', start_date=datetime(2025, 5, 13))
def extract_weekly_data():
    @task
    def extract_data():
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=IBM&apikey={API_KEY}'
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            weekly_close = data['Meta Data']['3. Last Refreshed']
            week = data["Weekly Adjusted Time Series"][weekly_close]
            weekly_data = {
                "Date": weekly_close,
                "Open": week["1. open"],
                "High": week["2. high"],
                "Low": week["3. low"],
                "Close": week["4. close"],
                "Adjusted Close": week["5. adjusted close"],
                "Volume": week["6. volume"],
                "Dividend Amount": week["7. dividend amount"]
            }
            # Check if the file exists, if yes append data, else create data
            filepath = "/home/deecodes/stock-data-pipeline/docs/weekly_data.json"
            try:
                if os.path.exists(filepath):
                    with open(filepath, 'r') as f:
                        existing_data = json.load(f)
                else:
                    existing_data = []
                    
                existing_data.append(weekly_data)
                    
                with open(filepath, 'w') as f:
                    json.dump(existing_data, f, indent=2)
                print(f"File uploaded successfully to {filepath}")
            except Exception as e:
                print(f"Error: {str(e)}")
        else:
            print(f"Error: {response.status_code}")

    extract_data()

weekly_dag = extract_weekly_data()

            
