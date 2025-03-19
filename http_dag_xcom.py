from airflow import DAG 
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import logging

default_arg ={
    'Owner': 'Ginger',
    'depends_on_past':False,
    'email_on_retry':False,
    'email_on_failure':False
}

def retrieve_log_header(**kwargs):
    """
    Extract the 'siteid' value from the nested configuration in DAG run.
    """
    try:
        # Access the nested configuration
        headers = kwargs.get('dag_run').conf.get('headers', {})
        x_vol_tennant = headers.get('x-vol-tennant', None)
        
        if x_vol_tennant:
            print(f"Retrieved siteid: {x_vol_tennant}")
        else:
            print("No 'siteid' found in the provided headers")
        
        # Push value to XComs
        ti = kwargs['ti']
        ti.xcom_push(key='x_vol_tennant', value=x_vol_tennant)
    
    except Exception as e:
        logging.error(f"Failed to retrieve 'siteid': {str(e)}")


def log_success_message(**kwargs):
    """
    Log a success message to indicate that the DAG has executed successfully,
    and include the retrieved XCom value.
    """
    try:
        ti = kwargs['ti']
        x_vol_tennant = ti.xcom_pull(task_ids='retrieve_log_header', key='x_vol_tennant')
        
        if x_vol_tennant:
            logging.info(f"DAG successfully executed. Retrieved x-vol-tennant: {x_vol_tennant}")
        else:
            logging.info("DAG successfully executed. No 'x-vol-tennant' value found.")
    except Exception as e:
        logging.error(f"Failed to log success message: {str(e)}")

with DAG('http_triger_dag',
         start_date=days_ago(1),
         schedule_interval=None,
         dagrun_timeout=timedelta(minutes=1),
         catchup=False) as dag:
    
    retrieve_log = PythonOperator(
        task_id='retrieve_log_header',
        python_callable=retrieve_log_header,
        retries=3,
        retry_delay=timedelta(minutes=1),
        provide_context=True
        
    )
    
    log_success = PythonOperator(
        task_id='log_success',
        python_callable=log_success_message,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )
    

retrieve_log >> log_success