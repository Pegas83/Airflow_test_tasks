from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
from urllib.request import urlopen
import pandas as pd
import json
from sqlalchemy import create_engine
from docker.types import Mount
import pendulum
from airflow.decorators import dag, task

@dag(
     dag_id="Api_integration",
     description="Интеграция с API",
     schedule_interval="@monthly",
     start_date= datetime(2020, 1, 1),
     is_paused_upon_creation=True,
     max_active_runs=1,
     catchup=False
 )
def taskflow_api_etl():
    @task()
    def get_api():
        response = urlopen('https://ru.wikipedia.org/w/api.php?action=query&list=allpages&aplimit=500&apfrom=A&format=json')
        response.encoding = 'Latin-1'
        data_json = json.loads(response.read())
        return data_json

    @task()
    def json_to_pd(data_json):
        query = data_json['query']['allpages']
        df = pd.DataFrame(query)
        return df

    @task()
    def df_do_db(df):
        # engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5438/postgres")
        engine = create_engine("postgresql+psycopg2://airflow:airflow@172.18.0.3:5432/airflow")
        df.to_sql('allpages', engine, schema='public', if_exists="append", index=False)

    data_json = get_api()
    df = json_to_pd(data_json)
    df_do_db(df)

taskflow_api_etl()
