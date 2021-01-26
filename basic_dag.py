from __future__ import print_function
from builtins import range
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG, Variable
from datetime import datetime, timedelta

import time
from pprint import pprint

from lithops_test import run_lithops

seven_days_ago = datetime.combine(
        datetime.today() - timedelta(7), datetime.min.time())

args = {
    'owner': 'airflow',
    'start_date': seven_days_ago,
}

dag = DAG(
    dag_id='basic_dag', default_args=args,
    schedule_interval='*/10 * * * *')

config = Variable.get("lithops_config", deserialize_json=True)

run_this = PythonOperator(
    task_id='basic_dag',
    python_callable=run_lithops,
    op_args=[config],
    dag=dag)

run_this