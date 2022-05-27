from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from requests import request
from supabase import create_client, Client
from datetime import datetime, timedelta
from airflow.models.variable import Variable
import pandas as pd
from sqlalchemy import create_engine
import os

POSTGRES_HOST = Variable.get("POSTGRES_HOST", "/opt/airflow")
POSTGRES_USERNAME = Variable.get("POSTGRES_USERNAME", "/opt/airflow")
POSTGRES_NAME = Variable.get("POSTGRES_NAME", "/opt/airflow")
POSTGRES_PASSWORD = Variable.get("POSTGRES_PASSWORD", "/opt/airflow")

###############################################
# Parameters
###############################################
SPARK_MASTER = Variable.get("SPARK_MASTER", "/opt/airflow")
POSTGRES_DRIVER_JAR = Variable.get("POSTGRES_DRIVER_JAR", "/opt/airflow")

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}/{POSTGRES_NAME}"

EMAIL_GITHUB = Variable.get("EMAIL_GITHUB", "/opt/airflow")
KEY_GITHUB = Variable.get("KEY_GITHUB", "/opt/airflow")

SUPABASE_URL: str = Variable.get("SUPABASE_URL")
SUPABASE_KEY: str = Variable.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

engine = create_engine(f'postgresql+psycopg2://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_NAME}')

auth = (
    EMAIL_GITHUB,
    KEY_GITHUB
)

def update_csv(engine, table, header):
    if os.path.isfile(f"/opt/spark/resources/csv/{table}.csv"):
        df = pd.read_csv(f"/opt/spark/resources/csv/{table}.csv", names=header)
        
        df.to_sql(table, engine, if_exists='append',index=False, index_label=None)
        
        os.remove(f"/opt/spark/resources/csv/{table}.csv")
        
        print(f"Inserted {table} successfully")
    else:
        print(f"No {table}")
    


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": "2022-05-26",
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="update_csv_hourly", 
    description="Load github company and more related information about those companies to postgres and then do analysis",
    default_args=default_args, 
    schedule_interval="@hourly",
    max_active_runs=1
)

update_user_hourly = PythonOperator(
    task_id="update_user_hourly",
    python_callable=update_csv,
    op_kwargs={
        "engine": engine,
        "table": "USER",
        "header": [
            'id',
            'login',
            'html_url',
            'type',
            'name',
            'blog',
            'location',
            'email',
            'public_repos',
            'followers',
            'following',
            'created_at',
            'updated_at'
        ]
    },
    dag=dag
)

update_commit_hourly = PythonOperator(
    task_id="update_commit_hourly",
    python_callable=update_csv,
    op_kwargs={
        "engine": engine,
        "table": "COMMIT",
        "header": [
            'sha',
            'date',
            'message',
            'comment_count',
            'html_url',
            'author_id',
            'repository_id'
        ]
    },
    dag=dag
)   

update_user_hourly >> update_commit_hourly