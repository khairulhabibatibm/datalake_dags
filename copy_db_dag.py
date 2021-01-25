from __future__ import print_function
from builtins import range
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta

import time
from pprint import pprint

from mysql_to_pg import lithops_run

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
    task_id='copy_db_dag',
    python_callable=lithops_run,
    op_args=[100],
    dag=dag)

run_this