# ~/projects/airflow-local/airflow/dags/hello_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append('/Users/jake/airflow-local/airflow/scripts')
from hello import say_hello

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 18),
    'retries': 1,
}

with DAG(
    'hello_dag',
    default_args=default_args,
    description='A simple DAG to run a Python script',
    schedule=None,
    catchup=False,
) as dag:

    run_python_script = PythonOperator(
        task_id='run_hello',
        python_callable=say_hello
    )
