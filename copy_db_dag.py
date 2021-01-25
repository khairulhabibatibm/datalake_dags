from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG, Variable
from datetime import datetime, timedelta


import time
from pprint import pprint
from staging_user import run_staging_user
from staging_trx import run_staging_trx

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

# config = "test"
# staging_pass = "test"

staging_user_step = PythonOperator(
    task_id='copy_user_to_staging',
    python_callable=run_staging_user,
    op_args=[config,staging_pass],
    dag=dag)

staging_trx_step = PythonOperator(
    task_id='copy_trx_to_staging',
    python_callable=run_staging_trx,
    op_args=[config,staging_pass],
    dag=dag)

staging_user_step >> staging_trx_step