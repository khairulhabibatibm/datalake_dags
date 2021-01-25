from __future__ import print_function
from builtins import range
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG, Variable
from datetime import datetime, timedelta

import time
from pprint import pprint

from lithops_copy import lithops_run

seven_days_ago = datetime.combine(
        datetime.today() - timedelta(7), datetime.min.time())

args = {
    'owner': 'airflow',
    'start_date': seven_days_ago,
}

dag = DAG(
    dag_id='copy_db_dag', default_args=args,
    schedule_interval=None)

config = Variable.get("lithops_config", deserialize_json=True)
staging_pass = Variable.get("PG_STAGING_PASSWORD")

run_this = PythonOperator(
    task_id='copy_db_dag',
    python_callable=lithops_run,
    op_args=[config,staging_pass],
    dag=dag)

run_this