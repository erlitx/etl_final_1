import logging
import os
import pandas as pd
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_NAME = "search_queries_etl"
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 14),
    'retries': 1,
}

dag = DAG(DAG_NAME, default_args=default_args, schedule_interval="@daily", catchup=False)

logging.basicConfig(level=logging.INFO)

MONGO_CONN_ID = 'etl_mongo'
POSTGRES_CONN_ID = 'etl_postgres'
DB_NAME = os.getenv("MONGO_DB", "source_db")
TABLE_NAME = "search_queries"
SCHEMA_NAME = "source"

def extract_from_mongo(**kwargs):
    mongo_client = MongoHook(mongo_conn_id=MONGO_CONN_ID).get_conn()
    collection = mongo_client[DB_NAME][TABLE_NAME]
    data = list(collection.find({}, {"_id": 0}))
    logging.info(f"Извлечено {len(data)} записей")
    kwargs['ti'].xcom_push(key='raw_data', value=data)

def transform_data(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(task_ids='extract', key='raw_data')
    if not raw_data:
        logging.warning("Нет данных для трансформации")
        return
    
    df = pd.DataFrame(raw_data)
    df['filters'] = df['filters'].apply(lambda x: ', '.join(x) if isinstance(x, list) else '')
    df.drop_duplicates(subset=["query_id"], inplace=True)
    df.fillna("", inplace=True)
    
    logging.info(f"Трансформировано {len(df)} записей")
    kwargs['ti'].xcom_push(key='processed_data', value=df.to_dict(orient='records'))

def load_to_postgres(**kwargs):
    processed_data = kwargs['ti'].xcom_pull(task_ids='transform', key='processed_data')
    if not processed_data:
        logging.warning("Нет данных для загрузки")
        return
    
    df = pd.DataFrame(processed_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.reindex(columns=['query_id', 'user_id', 'timestamp', 'query_text', 'filters', 'results_count'])
    df.set_index("query_id", inplace=True)
    
    engine = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_sqlalchemy_engine()
    df.to_sql(TABLE_NAME, schema=SCHEMA_NAME, con=engine, if_exists="replace", index=True)
    
    logging.info(f"Загружено {len(df)} записей в PostgreSQL")

start_task = DummyOperator(task_id='start', dag=dag)
extract_task = PythonOperator(task_id='extract', python_callable=extract_from_mongo, provide_context=True, dag=dag)
transform_task = PythonOperator(task_id='transform', python_callable=transform_data, provide_context=True, dag=dag)
load_task = PythonOperator(task_id='load', python_callable=load_to_postgres, provide_context=True, dag=dag)
finish_task = DummyOperator(task_id='finish', dag=dag)

start_task >> extract_task >> transform_task >> load_task >> finish_task
