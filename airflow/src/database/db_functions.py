from pathlib import Path
import pandas as pd

def fetch_data(engine, is_file = True, sql_filename = None, sql_text = None):
    if is_file:
        project_root = Path(__file__).resolve().parents[2]
        sql_path = project_root / "sql" / sql_filename
        sql = sql_path.read_text()
    
    else:
        sql = sql_text

    return pd.read_sql(con = engine, sql = sql)