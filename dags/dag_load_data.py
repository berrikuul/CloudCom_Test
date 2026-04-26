from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from psycopg2 import sql
from airflow.models.param import Param

def load_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='Postgres')
    source_table, target_table = kwargs['params']['source_table'], kwargs['params']['target_table']
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL('SELECT * FROM {} LIMIT 0').format(sql.Identifier(source_table)))
            columns = [column[0] for column in cur.description]

            insert_columns = sql.SQL(", ").join([sql.Identifier(col) for col in columns] + [sql.Identifier('load_time')])
            select_columns = sql.SQL(", ").join([sql.Identifier(col) for col in columns] + [sql.SQL('NOW()')])

            query = sql.SQL('''TRUNCATE {};
                                INSERT INTO {} ({})
                                SELECT {}
                                FROM {};''').format(
                sql.Identifier(target_table),
                sql.Identifier(target_table),
                insert_columns,
                select_columns,
                sql.Identifier(source_table))

            cur.execute(query)


with DAG (
    dag_id='load_t1_to_t2',
    start_date=datetime(2026, 1, 1),
    schedule= "@daily",
    catchup=False,
    params= {
        'source_table': Param('t1', type='string', minLength=1, maxLength=200),
        'target_table': Param('t2', type='string', minLength=1, maxLength=200),
    }


) as dag:
    task1 = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )


