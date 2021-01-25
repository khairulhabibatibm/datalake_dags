from __future__ import print_function
from builtins import range
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta

import time
from pprint import pprint

from mysql_to_pg import run

seven_days_ago = datetime.combine(
        datetime.today() - timedelta(7), datetime.min.time())

args = {
    'owner': 'airflow',
    'start_date': seven_days_ago,
}

dag = DAG(
    dag_id='copy_db_dag', default_args=args,
    schedule_interval=None)



run_this = PythonOperator(
    task_id='mysql_to_pg',
    python_callable=run,
    op_args=['World'],
    dag=dag)

run_this