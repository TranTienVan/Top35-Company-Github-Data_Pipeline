from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from numpy import pad
import psycopg2
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from requests import request
from supabase import create_client, Client
from datetime import datetime, timedelta
from airflow.models.variable import Variable
import pandas as pd


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

auth = (
    EMAIL_GITHUB,
    KEY_GITHUB
)

def update_language_repository(supabase: Client):
    df_activity = pd.read_csv("/opt/spark/resources/csv/activity.csv")
    
    LANGUAGE_URL = "https://api.github.com/repos/{}/{}/languages"
    
    for [company_name, repository_id, repository_name, time] in df_activity.values:
        languages = request.get(LANGUAGE_URL.format(company_name, repository_name), auth=auth).json()
        data_language = []
        for language in languages:
            data_language


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": "2022-01-01",
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="update_github_repo_hourly", 
    description="Load github company and more related information about those companies to postgres and then do analysis",
    default_args=default_args, 
    schedule_interval="@hourly",
    max_active_runs=2
)
start = DummyOperator(task_id="start", dag=dag)

spark_update_repo_activity = SparkSubmitOperator(
    task_id="spark_load_repo_activity",
    application="/opt/spark/app/update_activity_repository.py", # Spark application path created in airflow and spark cluster
    name="spark_load_repo_activity",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":SPARK_MASTER},
    application_args=[POSTGRES_URL,POSTGRES_USERNAME,POSTGRES_PASSWORD,POSTGRES_HOST, POSTGRES_NAME, EMAIL_GITHUB, KEY_GITHUB],
    jars=POSTGRES_DRIVER_JAR,
    driver_class_path=POSTGRES_DRIVER_JAR,
    dag=dag)

spark_load_commit = SparkSubmitOperator(
    task_id="spark_load_commit",
    application="/opt/spark/app/load_commit.py", # Spark application path created in airflow and spark cluster
    name="spark_load_commit",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":SPARK_MASTER},
    application_args=[POSTGRES_URL,POSTGRES_USERNAME,POSTGRES_PASSWORD,POSTGRES_HOST, POSTGRES_NAME, EMAIL_GITHUB, KEY_GITHUB, "update"],
    jars=POSTGRES_DRIVER_JAR,
    driver_class_path=POSTGRES_DRIVER_JAR,
    dag=dag)

spark_repository_tag = SparkSubmitOperator(
    task_id="spark_repository_tag",
    application="/opt/spark/app/load_repository_tag.py", # Spark application path created in airflow and spark cluster
    name="spark_repository_tag",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":SPARK_MASTER},
    application_args=[POSTGRES_URL,POSTGRES_USERNAME,POSTGRES_PASSWORD,POSTGRES_HOST, POSTGRES_NAME, EMAIL_GITHUB, KEY_GITHUB, "update"],
    jars=POSTGRES_DRIVER_JAR,
    driver_class_path=POSTGRES_DRIVER_JAR,
    dag=dag)

update_repository_language = PythonOperator(
    task_id="update_repository_language",
    python_callable=update_language_repository,
    op_kwargs={
        "supabase": supabase
    },
    dag=dag
)

   
end = DummyOperator(task_id="end", dag=dag)
start >> spark_update_repo_activity
spark_update_repo_activity >> update_repository_language
spark_update_repo_activity >> spark_load_commit
spark_load_commit >> spark_repository_tag
update_repository_language >> end
spark_repository_tag >> end