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

#exo2

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def python_first_function(): pass

default_dag_args = {
     'start_date': datetime(2022, 9, 1), 
     'email_on_failure': False, 'email_on_retry': False, 
     'retries': 1, 'retry_delay': timedelta(minutes=5), 
     'project_id': 1 }

with DAG("first_python_dag", schedule_interval = '@daily', catchup=False, default_args = default_dag_args) as dag_python:
    task_0 = PythonOperator(task_id = "first_python_task", python_callable = python_first_function)


#exo3
import requests 
import time 
import json 
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.python import BranchPythonOperator 
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta 
import pandas as pd 
import numpy as np 
import os

def get_data(**kwargs):
    pass

default_dag_args = {
    'start_date': datetime(2022, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 1
}

with DAG('market_data_dag', default_args=default_dag_args, schedule_interval='@daily') as dag:
    task_0 = PythonOperator(task_id = "get_market_data", python_callable = get_data, op_kwargs = {'tickers' : []})

#exo4
import time 
import json 
from airflow import DAG 
from airflow.operators.postgres_operator import PostgresOperator 
from datetime import timedelta
from airflow.utils.dates import days_ago

default_args = { 'owner': 'airflow',
                'retries': 1, 
                'retry_delay': timedelta(minutes=5),
                
}

create_query = """ """

insert_data_query = """ """

calculate_average_age_query = """ """

dag_postgres = DAG(
    dag_id='postgres_dag_connection',
    default_args=default_args,
    schedule_interval=None,
    start_date = days_ago(1)
)


# Define tasks
create_table = PostgresOperator(
    task_id = "creation_of_table",
    sql = create_query,
    dag = dag_postgres,
    postgres_conn_id = "postgres_pedro_local"
)

insert_data = PostgresOperator(
    task_id = "insertion_of_data",
    sql = insert_data_query,
    dag = dag_postgres,
    postgres_conn_id = "postgres_pedro_local"
)

group_data  = PostgresOperator(
    task_id = "calculating_averag_age",
    sql = calculating_averag_age,
    dag = dag_postgres,
    postgres_conn_id = "postgres_pedro_local"
)

# Define DAG flow
create_table >> insert_data >> group_data

