from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag

import boto3
import pendulum


def fetch_s3_file(bucket: str, key: str):
    AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
    AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=key
    )


bash_command_tmpl = """head -n 10 {{ params.files }}"""


@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_dag_get_group_log():
    file_group_log = PythonOperator(
        task_id='file_group_log',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'},
        )
    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': "/data/group_log.csv"}
    )

    file_group_log >> print_10_lines_of_each


load_group_log = sprint6_dag_get_group_log()