from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from src.march_madness import data_prep 

dag = DAG(
    dag_id = "march_madness",
    start_date = datetime(2026, 2, 20),
    schedule = None,
    catchup = False
)

hook = PostgresHook(postgres_conn_id = "my_postgres")
engine = hook.get_sqlalchemy_engine()

task_a = PythonOperator(
    task_id = "data_prep",
    python_callable = data_prep.data_prep,
    op_kwargs={
        "engine": engine
    },
    dag = dag
)