import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param
from psycopg2 import sql
from datetime import datetime

import json

from airflow.exceptions import AirflowSkipException
import try_minio_with_python

def check_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='Postgres')
    source_table = kwargs['params']['source_table']
    data_interval_start = kwargs['data_interval_start']
    data_interval_end = kwargs['data_interval_end']

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
                query = sql.SQL('''SELECT COUNT(*)
                         FROM {}
                         WHERE created between %s and %s''').format(
                    sql.Identifier(source_table)
                )
                cur.execute(query, (data_interval_start, data_interval_end))
                count = cur.fetchone()[0]

                if count == 0:
                    raise AirflowSkipException("Нет данных за этот день")

def update_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='Postgres')

    data_interval_start = kwargs['data_interval_start']
    data_interval_end = kwargs['data_interval_end']

    source_table = kwargs['params']['source_table']
    s3_bucket = kwargs['params']['s3_bucket']

    object_name = f"{data_interval_start.strftime('%Y-%m-%d')}/data.jsonl"
    try_minio_with_python.create_minio_bucket(s3_bucket)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:

            query = sql.SQL('''SELECT * FROM {}
                     WHERE created between %s and %s''').format(
                sql.Identifier(source_table)
            )
            cur.execute(query, (data_interval_start, data_interval_end))
            columns = [i[0] for i in cur.description]

            with open('data.jsonl', 'w', encoding='utf-8') as file:
                for row in cur:
                    try:
                        data_dict = dict(zip(columns, row))
                        json.dump(data_dict, file, ensure_ascii=False, default=str)
                        file.write('\n')
                    except Exception as e:
                        logging.error(f"Ошибка на строке {row}, ошибка: {e}")
                        continue

    try_minio_with_python.upload_to_s3(file_path='data.jsonl', bucket_name=s3_bucket, object_name=object_name)


with DAG(
    dag_id='test1',
    start_date=datetime(2026, 1, 1),
    schedule= "@daily",
    catchup=False,
    params= {
        'source_table': Param('t1', type='string', minLength=1, maxLength=200),
        's3_bucket': Param('bucket-test', type='string', minLength=1, maxLength=200),
        }
) as dag1:
    task2 = PythonOperator(
        task_id='task2',
        python_callable= check_data,
    )
    task3 = PythonOperator(
        task_id='task3',
        python_callable= update_data,
    )

    task2 >> task3
