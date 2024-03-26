from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import vertica_python
import contextlib
from typing import List, Optional, Dict

import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}


def load_dataset_file_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None,
):
    df = pd.read_csv(dataset_path, dtype=type_override)
    num_rows = len(df)
    vertica_conn = vertica_python.connect(
        host='vertica.tgcloudenv.ru',
        port=5433,
        user='stv2024031246',
        password='kbLPKdlka0UfBXV',
        db='dwh'
    )
    columns = ', '.join(columns)
    copy_expr = f"""
    COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY ''''
    """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            print(f"loading rows {start}-{end}")
            df.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            print("loaded")
            start += chunk_size + 1

    vertica_conn.close()


with DAG('load_data_to_vertica',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    load_group_log_data = PythonOperator(
        task_id='load_groups_log_data',
        python_callable=load_dataset_file_to_vertica,
        op_args=('/data/group_log.csv', 'STV2024031246__STAGING', 'group_log', ['group_id', 'user_id', 'user_id_from', 'event', 'event_datetime']),
    )

    load_group_log_data