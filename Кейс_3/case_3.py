from airflow import DAG
from airflow.providers.jdbc.operators.jdbc import JdbcOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
}

with DAG(
    dag_id='from_postgres_to_greenplum',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Задача для выгрузки данных из PostgreSQL
    extract_data = PostgresOperator(
        task_id='extract_data_task',
        postgres_conn_id='my_postgres_connection',
        sql="""
            SELECT
                tab_num_work_month_key,
                department,
                "position",
                tab_num,
                fio,
                birth_date,
                address,
                phone_1,
                phone_2,
                work_month,
                hours_worked,
                effective_date_from,
                effective_date_to,
                is_active
            FROM
                core.t_employee;
        """,
        do_xcom_push=True
    )

    # Задача для вставки данных в Greenplum
    insert_data = JdbcOperator(
        task_id='insert_data_task',
        jdbc_conn_id='my_greenplum_connection',
        sql="""
            INSERT INTO core.t_employee (
                tab_num_work_month_key,
                department,
                "position",
                tab_num,
                fio,
                birth_date,
                address,
                phone_1,
                phone_2,
                work_month,
                hours_worked,
                effective_date_from,
                effective_date_to,
                is_active
                )
            VALUES ('{{ task_instance.xcom_pull(task_ids="extract_data_task") }}');
        """,
        autocommit=True
    )


    extract_data >> insert_data
