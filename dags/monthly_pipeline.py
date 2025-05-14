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
    'retry_delay': timedelta(minutes=3),
    'retries': 5
}

@dag(dag_id='extract_monthly_data', default_args=default_args, start_date=datetime(2025, 5, 14), schedule='@monthly', catchup=False)
def extract_monthly_data():
    @task
    def extract_data():
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY_ADJUSTED&symbol=IBM&apikey={API_KEY}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            monthly_close = data['Meta Data']['3. Last Refreshed'] # Refresh date
            month = data["Monthly Adjusted Time Series"][monthly_close]
            monthly_data = {
                'Date': monthly_close,
                'Open': month["1. open"],
                'High': month["2. high"],
                'Low': month["3. low"],
                'Close': month["4. close"],
                'Adjusted Close': month["5. adjusted close"],
                'Volume': month["6. volume"],
                'Dividend Amount': month["7. dividend amount"]
            }

            filepath = '/home/deecodes/stock-data-pipeline/docs/monthly_data.json'
            # Check if filepath exists
            try:
                if os.path.exists(filepath):
                    with open(filepath, 'r') as f:
                        existing_data = json.load(f)
                else:
                    existing_data = [] # Initialize an empty list if file doesn't exist
                existing_data.append(monthly_data) # Append monthly_data to existing_data

                # Write to file
                with open(filepath, 'w') as f:
                    json.dump(existing_data, f, indent=2)
                print(f"Data loaded to {filepath} successfully!")
            except Exception as e:
                print(f"Error: {str(e)}")
        else:
            print(f"Error: {response.status_code}")

    extract_data()

monthly_dag = extract_monthly_data()
