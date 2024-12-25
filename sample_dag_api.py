from airflow import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable  # To retrieve Airflow variable

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'email_on_failure': False,
    'email_on_retry': False,
}

def validate_weather_data(ti):
    london_weather = ti.xcom_pull(task_ids='get_london_weather')
    paris_weather = ti.xcom_pull(task_ids='get_paris_weather')
    if not london_weather or not paris_weather:
        raise ValueError("Weather data retrieval failed. Halting pipeline.")


# Retrieve API key from Airflow Variables
API_KEY = Variable.get("api_key", default_var="")

# Define the DAG
with DAG(
    dag_id='weather_dag_new',
    default_args=default_args,
    description='New_dag_for weather_api',
    schedule_interval='@daily',
    start_date=datetime(2024, 12, 20),
    catchup=False,
) as dag:

    # Headers for API requests
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }

    # TaskGroup for parallel fetching tasks
    with TaskGroup("fetch_weather_data") as get_weather_data:
        get_london_weather = SimpleHttpOperator(
            task_id='get_london_weather',
            method='GET',
            http_conn_id=524901,
            endpoint="'/data/2.5/weather?q=London&appid=API_KEY",
            headers=headers,
            response_filter=lambda response: response.json(),
            log_response=True,
        )
        get_paris_weather = SimpleHttpOperator(
            task_id='get_paris_weather',
            method='GET',
            http_conn_id=524901,
            endpoint="'/data/2.5/weather?q=Paris&appid=API_KEY'",
            headers=headers,
            response_filter=lambda response: response.json(),
            log_response=True
        )
        
    validate_data = PythonOperator(
        task_id='validate_weather_data',
        python_callable=validate_weather_data,
        )
    
    with TaskGroup("Transform_and_send_api_data") as Transform_send_data:
        # Transformation task
        transform_api_call = SimpleHttpOperator(
        task_id='transform_api_call',
        method='POST',
        http_conn_id='http_default',
        endpoint="/data/2.5/forecast",  
        log_response=True,
        )
        # Send task
        send_transform_api_call = SimpleHttpOperator(
            task_id='send_api_call',
        method='POST',
        http_conn_id='http_default',
        endpoint="/data/2.5/send",
        headers=headers,
        log_response=True,
        )
    process_task_group = BashOperator(
        task_id='process_task_group',
        bash_command="echo 'Processing Transform_and_send_api_data TaskGroup'",
        trigger_rule="all_success",  # Trigger only after all tasks in the TaskGroup succeed
        depends_on_past=False,
        dag=dag,
    )

# Task dependencies
get_weather_data >> validate_data >> transform_api_call >> send_transform_api_call 
Transform_send_data >> process_task_group 
