from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy.sql.ddl import CreateSchema
from sqlalchemy import create_engine
import pandas as pd
from docker.types import Mount
from airflow.providers.postgres.operators.postgres import PostgresOperator
import psycopg2

dockerops_kwargs = {
    "mount_tmp_dir": False,
    "mounts": [
        Mount(
            source="C:/airflow-docker/data",
            target="/opt/airflow/data/",
            type="bind",
        )
    ]
}
with DAG(
        dag_id="01",
        description="Прогрузка данных из источника за январь",
        schedule_interval="@monthly",
        default_args={'start_date': datetime(2020, 1, 1), 'depends_on_past': False},
        is_paused_upon_creation=True,
        max_active_runs=1,
        catchup=False
) as dag:
    start_task = EmptyOperator(task_id='START', dag=dag)
    end_task = EmptyOperator(task_id='END', dag=dag)

customer_table_name = "t_employee"


# функция для загрузки данных из csv-файлов в базу данных Postgres
def load_csv(table_name: str, schema: str = "src") -> None:
    df = pd.read_csv('/opt/airflow/table_month_01.csv', delimiter=';', encoding='windows-1251')
    # создание движка базы данных
    engine = create_engine("postgresql+psycopg2://airflow:airflow@172.18.0.3:5432/airflow")
    df.to_sql(table_name, engine, schema=schema, if_exists="append", index=False)


# функция для прогрузки данных из src-слоя в stg-слой
def src_to_stg(**kwargs):
    connection_string = "postgres://airflow:airflow@172.18.0.3:5432/airflow"
    conn = psycopg2.connect(connection_string)
    select_query = 'CALL stg.t_employee_load();'
    cur = conn.cursor()
    cur.execute(select_query)
    cur.close()
    conn.commit()
    conn.close()


# функция для прогрузки данных из stg-слоя в core-слой
def stg_to_core(**kwargs):
    connection_string = "postgres://airflow:airflow@172.18.0.3:5432/airflow"
    conn = psycopg2.connect(connection_string)
    select_query = "CALL core.t_employee_load();"
    cur = conn.cursor()
    cur.execute(select_query)
    cur.close()
    conn.commit()
    conn.close()


load_csv = PythonOperator(
    task_id="load_csv",
    python_callable=load_csv,
    op_kwargs={
        "table_name": customer_table_name,
        "schema": "src"
    }
)

src_to_stg = PythonOperator(
    task_id='src_to_stg',
    python_callable=src_to_stg,
    provide_context=True,
    dag=dag
)

stg_to_core = PythonOperator(
    task_id='stg_to_core',
    python_callable=stg_to_core,
    provide_context=True,
    dag=dag
)

start_task >> load_csv >> src_to_stg >> stg_to_core >> end_task
