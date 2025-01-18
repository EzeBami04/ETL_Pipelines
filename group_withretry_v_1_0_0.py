"""
==========================================
READ ME:

DAG Demonstrating Task Group Functionality
==========================================

Overview:
    This DAG showcases the usage of Airflow's TaskGroup functionality for grouping 
    related tasks. It also demonstrates retry configurations at both the group 
    and individual task levels.

DAG Details:
    - DAG ID: taskgroup_with_retries_dag
    - Schedule: Runs every 3 hours
    - Start Date: January 1, 2025
    -dagrun_timeout: 1
    - Catchup: False

Purpose:
    1. To illustrate the use of TaskGroup for organizing tasks.
    2. To demonstrate retry settings at the group level and task level.
    3. To showcase a mix of PythonOperator and BashOperator tasks.

Tasks:
    - Task Group ('group'):
        - `task_a`: A Python task that prints a message.
        - `task_b`: A Python task that prints a different message.
        - Retry Configuration:
            - Retries: 3
            - Retry Delay: 1 minute
    - Task c:
        - `task_c`: A Bash task that prints a message and exits successfully.
        - characterized with the trigger rule all_success which only triggers the task when all the parent
        upstream task have been completed.
        
Failure Handling:
- If any task in TaskGroup A fails, Task 3 will not be executed.
- Tasks in TaskGroup A are retried up to 3 times, with a delay of 1 minute between retries.
- Failure details are logged, and additional alerting mechanisms can be configured using Airflow's on_failure_callback.

Usage Instructions:
    1. Place this script in the Airflow DAGs folder.
    2. Start the Airflow scheduler and webserver.
    3. Access the DAG in the Airflow UI and trigger it manually.


Author:
    Ezekiel Omobamidele
    Date: 2025-01-17
"""

from airflow import DAG
from datetime import datetime , timedelta

from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def py_test():
   logging.info("Python callable function for task_a")
    
    
def task_b():
    logging.info("Python Callable function for task_b")

def alert_on_failure(context):
    logging.error(f"Task {context['task_instance'].task_id} failed in DAG {context['dag'].dag_id}")

with DAG(
    dag_id='taskgroup_with_retries_dag',  # unique identifier for the DAG
    schedule_interval='0 */3 * * *',  # cron time expression to run every 3 hours
    dagrun_timeout=timedelta(minutes=1),  # maximum time allowed for a single DAG run
    start_date=datetime(2025, 1, 1),  # start date for the DAG's schedule
    catchup=False  # disable backfilling for missed schedule intervals
) as dag:
    
    with TaskGroup('group') as group:
        """ Creating task group so as to enable each task to be retried after every 1 minute and with 
        a maximum of 3 reties"""
        task_a = PythonOperator(
            task_id='task_a',  # unique identifier for the task
            python_callable=py_test,  # python callable function to execute
            retries=3,  # maximum number of retries
            retry_delay=timedelta(minutes=1),  # delay between retries
            on_failure_callback=alert_on_failure 
            )
        
        task_b = PythonOperator(
            task_id='task_b', # unique identifier for the task
            python_callable=task_b, # python callable function to execute
            retries=3,  # maximum number of retries
            retry_delay=timedelta(minutes=1),  # delay between retries
            on_failure_callback=alert_on_failure
        )
  
task_c= BashOperator(
    task_id ='task_c',
    bash_command="echo 'task_c' && exit 0 ",
    trigger_rule='all_success',
    on_failure_callback=alert_on_failure
    )
logging.info("Creating a Bash task that prints a message and exits is successfully")


group >> task_c