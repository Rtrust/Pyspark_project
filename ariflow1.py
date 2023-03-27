from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_dag_args = {
    'start_date': datetime(2023,3,27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id':1
}

#define dag
with DAG("First_DAG", schedule_interval = None, default_args = default_dag_args) as dag:
    #here at this level we define the tasks of the dag
    task_0 = BashOperator(task_id= 'bash_task', bash_command= "echo 'command excuted from bash Operator' ")
    task_1 = BashOperator(task_id='bash_task_move_data', bash_command='copy "/Users/pc home/Desktop/DATA_CENTER/DATA_LAKE/dataset_raw.txt" "/Users/pc home/Desktop/DATA_CENTER/CLEAN_DATA/"')
    task_2 = BashOperator(task_id='bash_task_move_data', bash_command= "")
    task_0 >> task_1 >> task_2