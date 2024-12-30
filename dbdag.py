from datetime import datetime as dt, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine
import pandas as pd
import pyodbc as db

# Connection strings
mysql_string = (
    "DRIVER={MySQL ODBC 8.0 Driver};"
    "SERVER=localhost;"
    "DATABASE=buck;"
    "USER=root;"
    "PASSWORD=Bambass/20;"
)

msql_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=DESKTOP-51SU9VA;"
    "DATABASE=buck;"
    "UID=DESKTOP-51SU9VA\\HP USER;"
)

default_args = {
    'owner': 'AnalystB',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=4),
    'email_on_failure': False,
    'email_on_retry': False
}

# Define the DAG
with DAG(
    dag_id='Sql_dagg',
    default_args=default_args,
    description='Sample SQL DAG',
    schedule_interval='@daily',
    start_date=dt(2024, 12, 30),
    catchup=False
) as dag:

    @task()
    def get_file():
        # Load data from a CSV file
        data = pd.read_csv("C:/Users/HP USER/Documents/players.csv")
        return data.to_json()

    @task()
    def transform_data(raw_data):
        # Transform the data (drop NA values)
        df = pd.read_json(raw_data)
        df_cleaned = df.dropna(how='any')
        return df_cleaned.to_json()

    @task()
    def my_sql(cleaned_data):
        engine = create_engine("mysql+pymysql://root:Bambass/20@localhost:3306/buck")
        df_sq = pd.read_json(cleaned_data)
        df_sq.to_sql(
            name='bucks',
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        engine.dispose()
        return "Data successfully loaded into MySQL table 'bucks'."

    @task()
    def load_to_mssql(cleaned_data):
        connection_url = f"mssql+pyodbc:///?odbc_connect={msql_string}"
        engine = create_engine(connection_url)
        df_msql = pd.read_json(cleaned_data)
        try:
            df_msql.to_sql(
                name='bucks',
                con=engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
        finally:
            engine.dispose()
        return "Data successfully loaded into MSSQL table 'bucks'."

    with TaskGroup("Extract_data") as source_data:
        source = get_file()
        transformed = transform_data(source)

    send_mysql = my_sql(transformed)
    send_mssql = load_to_mssql(transformed)


    source_data >> send_mysql >> send_mssql
