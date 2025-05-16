# Stock API Data Pipeline

A comprehensive ETL  stock data pipeline from the AlphaVantage's stock API and loaded into a PostgreSQL database.

## Requirements

These packages are have been used in building this pipeline:
- `apache-airflow-2.10.5` for task scheduling and orchestration.
- `python`
- `pandas` for data cleaning and transformation
- `requests` to get data from our API
- `sqlalchemy`, to create a connection to the PostgreSQL database using the `create_engine` method.
- `psycopg2-binary`, a PostgreSQL database adapter for Python
- `dotenv` to load environment variables from the .env folder.

## Project Structure

```bash
stock-data-pipeline/
│
├── dags/
│   ├── daily_pipeline.py
│   ├── weekly_pipeline.py
│   ├── monthly_pipeline.py
|── airflow/
|── .gitignore
├── .env
├── requirements.txt
└── README.md
```

## Project Setup Instructions.

Follow the following instructions to safely clone this project and set it up in your local machine.
Also, carefully read the note below before setup.

**NOTE: If you are on Windows, use Windows Subsystem for Linux(WSL) for this project. Apache Airflow does NOT support Windows, so it will not work on Windows. Clone this repository in WSL and not Command Prompt or Powershell.**

### 1. Clone this repository.
In your terminal, run the following command to clone this project:

```bash
git clone https://github.com/dkkinyua/stock-data-pipeline.git
cd stock-data-pipeline
```

### 2. Install and activate your virtual environment.

Install your virtual environment using `venv` or any other virtual environment provider e.g. `virtualenv` and activate it.

Installing virtual environments for each project is essential and a good practice to prevent package mixup and allows each project to work with compatible package versions.

Run the following command in your terminal:

```bash
python -m venv your_env
source your_env/bin/activate # Linux and MacOS
your_env\Scripts\activate # Windows
```
When your virtual environment is active, the environment name will show next to your directory in the terminal as shown in the snapshot below:

![Environment Variable Snapshot](https://res.cloudinary.com/depbmpoam/image/upload/v1747393560/Screenshot_2025-05-16_135754_ijppfw.png)

### 3. Install the required dependencies.

In the project structure, there is a `requirements.txt` file which contains all the package required for this project.

To install these packages, run the following command in your terminal. 

```bash
pip install -r requirements.txt
```

### 4. Setup and run Airflow.
Run the following command in your terminal to set the home path for your project's Airflow configuration folder.

```bash
export AIRFLOW_HOME=/path/to/stock-data-pipeline/airflow
```
This will setup the project's root as Airflow's home directory for the config files. Then run the following command to initialize Airflow's database and create the config folder.

```bash
airflow db init
```
Then, run the following command to create an Admin user for Airflow. Remember the credentials you will enter here as this will be your login details when accessing the Airflow UI from your browser.

```bash
airflow users create \
    --username admin \
    --firstname YourName \
    --lastname Admin \
    --role Admin \
    --email admin@example.com \
    --password admin
```

Change these placeholders with your actual credentials.

After creating the Admin user, migrate these changes to the Airflow SQLite db for the changes to apply.

```bash
airflow db migrate
```

Start the Airflow webserver and scheduler by running the following command.

```bash
airflow webserver & airflow scheduler
# You can specify the port you'd want to run using the following command
# airflow webserver -p 8000 & airflow scheduler
```
The Airflow UI will be running on port `8080` on default or on the specified port you entered.

![Airflow UI Login page](https://i.sstatic.net/bxcdr.png)

## Airflow DAGs

There are 3 Airflow DAGs in this project namely `daily_pipeline.py`, `weekly_pipeline.py` and `monthly_pipeline.py` in the `dags/` folder.

Let's explain the setup and how to run of one of the DAGs.

### `daily_pipeline.py`.

This pipeline extracts daily IBM stock data from the AlphaVantage Daily Stock API, transforms and cleans it and loads the data to an Aiven PostgreSQL database scheduled on a daily basis at midnight every day.

```python
from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'your_name',
    'retries': 5, # If the DAG fails to run, there are 5 chances to retry and rerun the DAG
    'retry_delay': timedelta(minutes=3) # Delay retry by 3 minutes
}

@dag(dag_id='daily_data_pipeline_dag', default_args=default_args, start_date=datetime(2025, 5, 13), schedule='@daily')
def extract_data():
    # code
```

There are 3 tasks in this pipeline.

#### i. `extract_daily_data` 
This which extracts data using the `requests` library from the API and returns a dictionary of daily values.

**NOTE: Always store your sensitive information e.g. API keys, Postgres credentials in a `.env` file and define the file in the `.gitignore` to instruct Git to ignore the `.env` file while pushing changes to your repository to avoid exposing sensitive information in your code**

```python
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")

# code

@dag(...)
def extract_data():
    @task
    def extract_daily_data():
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey={API_KEY}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data # Returns a dictionary, unclean data
        else:
            print(f"There is an error. Code: {response.status_code}")
```

#### ii. `transform_daily_data`
Transforms the data into a `pandas` DataFrame, changes the `Date` column into a datetime object, sets the `Date` column as the index because this is time-series data, changes the data types of the other columns to `float` for easier analysis in the database and returns a cleaned dataframe to be loaded into a database.

```python
@dag(...)
def extract_data():
    # other tasks
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
```

#### iii. `load_to_db`
Loads the cleaned dataframe into a Postgres database using the `create_engine` method from `sqlalchemy`.

```python
@dag(...)
def extract_data():
    # other tasks
    @task
    def load_to_db(df):
        try:
           engine = create_engine(url=DB_URL)
           df.to_sql(name='daily_stock_data', con=engine, if_exists='append') #append data to db if table exists
           print("Data loaded into table successfully")
        except Exception as e:
            print(f"Loading DataFrame error: {str(e)}")
```
#### Setting up the task sequence

The tasks return data values when complete. We will use Airflow's XCom feature to share this data between tasks and this will help us in setting up the task sequence.

We know that the task sequence is: `extract_daily_data()` -> `transform_daily_data()` -> `load_to_db()`

Let's set it up.

```python
data = extract_daily_data() # The dict of daily values from API
df = transform_daily_data(data) # Takes in the data from extract_daily_data, transforms it and then returns a dataframe
load_to_db(df) # Loads in the dataframe from transform_daily_data into the database.
```

To run this DAG, go to the Airflow UI in the browser and unpause the DAG.

![DAG overview](https://res.cloudinary.com/depbmpoam/image/upload/v1747396683/Screenshot_2025-05-16_145738_kqj0fi.png)

Head over to `daily_data_pipeline_dag` and click on the Play button to run your DAG.

![Running your Daily DAG](https://res.cloudinary.com/depbmpoam/image/upload/v1747396993/Screenshot_2025-05-16_150250_l3ntua.png)

If your DAG run is successful, the dails column on your DAG page will have the 'Success' status. Check your Graph view to check if your tasks executed as expected. For more information, check your tasks' Event Log view.

![DAG Success Run Graph View](https://res.cloudinary.com/depbmpoam/image/upload/v1747397141/Screenshot_2025-05-16_150524_ct3wh2.png)

To check if your `load_to_db` task loaded the data successfully, check your table in the database to confirm.

![Postgres Check](https://res.cloudinary.com/depbmpoam/image/upload/v1747397335/Screenshot_2025-05-16_150838_zos6ch.png)

## Conclusion

If you would want to contribute any further, email me on denzelkinyua11@gmail.com