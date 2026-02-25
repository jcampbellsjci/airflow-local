from pathlib import Path
from airflow.providers.postgres.hooks.postgres import PostgresHook

def fetch_data(is_file = True, sql_filename = None, sql_text = None):
    # Making connection to db via airflow connection
    hook = PostgresHook(postgres_conn_id = "my_postgres")

    if is_file:
        # Injecting sql file text
        project_root = Path(__file__).resolve().parents[2]
        sql_path = project_root / "sql" / sql_filename
        sql = sql_path.read_text()
    
    else:
        sql = sql_text

    return hook.get_pandas_df(sql)