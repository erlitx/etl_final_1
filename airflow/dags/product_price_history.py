import logging
import os
import pandas as pd
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_NAME = "product_price_history_etl"
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
TABLE_NAME = "product_price_history"
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
    df = df.explode("price_changes").reset_index(drop=True)
    price_history_df = df["price_changes"].apply(pd.Series)
    df_transformed = pd.concat([df.drop(columns=["price_changes"]), price_history_df], axis=1)
    df_transformed.drop_duplicates(subset=['product_id', 'date'], inplace=True)
    df_transformed.dropna(inplace=True)
    
    logging.info(f"Трансформировано {len(df_transformed)} записей")
    kwargs['ti'].xcom_push(key='processed_data', value=df_transformed.to_dict(orient='records'))

def load_to_postgres(**kwargs):
    processed_data = kwargs['ti'].xcom_pull(task_ids='transform', key='processed_data')
    if not processed_data:
        logging.warning("Нет данных для загрузки")
        return
    
    df = pd.DataFrame(processed_data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.reindex(columns=['product_id', 'date', 'price', 'currency', 'current_price'])
    df.set_index(['product_id', 'date'], inplace=True)
    
    engine = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_sqlalchemy_engine()
    df.to_sql(TABLE_NAME, schema=SCHEMA_NAME, con=engine, if_exists="append", index=True)
    
    logging.info(f"Загружено {len(df)} записей в PostgreSQL")

start_task = DummyOperator(task_id='start', dag=dag)
extract_task = PythonOperator(task_id='extract', python_callable=extract_from_mongo, provide_context=True, dag=dag)
transform_task = PythonOperator(task_id='transform', python_callable=transform_data, provide_context=True, dag=dag)
load_task = PythonOperator(task_id='load', python_callable=load_to_postgres, provide_context=True, dag=dag)
finish_task = DummyOperator(task_id='finish', dag=dag)

start_task >> extract_task >> transform_task >> load_task >> finish_task
