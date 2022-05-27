from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
import psycopg2
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta
from airflow.models.variable import Variable


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
    dag_id="spark-postgres", 
    description="Load github company and more related information about those companies to postgres and then do analysis",
    default_args=default_args, 
    schedule_interval="@once"
)


start = DummyOperator(task_id="start", dag=dag)

spark_load_company = SparkSubmitOperator(
    task_id="spark_load_company",
    application="/opt/spark/app/load_company.py", # Spark application path created in airflow and spark cluster
    name="spark_load_company",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":SPARK_MASTER},
    application_args=[POSTGRES_URL,POSTGRES_USERNAME,POSTGRES_PASSWORD,POSTGRES_HOST, POSTGRES_NAME, EMAIL_GITHUB, KEY_GITHUB],
    jars=POSTGRES_DRIVER_JAR,
    driver_class_path=POSTGRES_DRIVER_JAR,
    dag=dag)

spark_load_organization_member = SparkSubmitOperator(
    task_id="spark_load_organization_member",
    application="/opt/spark/app/load_organization_member.py", # Spark application path created in airflow and spark cluster
    name="spark_load_organization_member",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":SPARK_MASTER},
    application_args=[POSTGRES_URL,POSTGRES_USERNAME,POSTGRES_PASSWORD,POSTGRES_HOST, POSTGRES_NAME, EMAIL_GITHUB, KEY_GITHUB],
    jars=POSTGRES_DRIVER_JAR,
    driver_class_path=POSTGRES_DRIVER_JAR,
    dag=dag)

spark_load_license = SparkSubmitOperator(
    task_id="spark_load_license",
    application="/opt/spark/app/load_license.py", # Spark application path created in airflow and spark cluster
    name="spark_load_license",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":SPARK_MASTER},
    application_args=[POSTGRES_URL,POSTGRES_USERNAME,POSTGRES_PASSWORD,POSTGRES_HOST, POSTGRES_NAME, EMAIL_GITHUB, KEY_GITHUB],
    jars=POSTGRES_DRIVER_JAR,
    driver_class_path=POSTGRES_DRIVER_JAR,
    dag=dag)

spark_load_repository = SparkSubmitOperator(
    task_id="spark_load_repository",
    application="/opt/spark/app/load_repository.py", # Spark application path created in airflow and spark cluster
    name="spark_load_repository",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":SPARK_MASTER},
    application_args=[POSTGRES_URL,POSTGRES_USERNAME,POSTGRES_PASSWORD,POSTGRES_HOST, POSTGRES_NAME, EMAIL_GITHUB, KEY_GITHUB],
    jars=POSTGRES_DRIVER_JAR,
    driver_class_path=POSTGRES_DRIVER_JAR,
    dag=dag)

spark_load_repo_language = SparkSubmitOperator(
    task_id="spark_load_repo_language",
    application="/opt/spark/app/load_repo_language.py", # Spark application path created in airflow and spark cluster
    name="spark_load_repo_language",
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
    application_args=[POSTGRES_URL,POSTGRES_USERNAME,POSTGRES_PASSWORD,POSTGRES_HOST, POSTGRES_NAME, EMAIL_GITHUB, KEY_GITHUB, "load"],
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
    application_args=[POSTGRES_URL,POSTGRES_USERNAME,POSTGRES_PASSWORD,POSTGRES_HOST, POSTGRES_NAME, EMAIL_GITHUB, KEY_GITHUB, "load"],
    jars=POSTGRES_DRIVER_JAR,
    driver_class_path=POSTGRES_DRIVER_JAR,
    dag=dag)

# spark_repository_contribution = SparkSubmitOperator(
#     task_id="spark_repository_contribution",
#     application="/opt/spark/app/load_repository_contribution.py", # Spark application path created in airflow and spark cluster
#     name="spark_repository_contribution",
#     conn_id="spark_default",
#     verbose=1,
#     conf={"spark.master":SPARK_MASTER},
#     application_args=[POSTGRES_URL,POSTGRES_USERNAME,POSTGRES_PASSWORD,POSTGRES_HOST, POSTGRES_NAME, EMAIL_GITHUB, KEY_GITHUB],
#     jars=POSTGRES_DRIVER_JAR,
#     driver_class_path=POSTGRES_DRIVER_JAR,
#     dag=dag)

end = DummyOperator(task_id="end", dag=dag)


start >> spark_load_company
start >> spark_load_license
spark_load_company >> spark_load_organization_member
spark_load_company >> spark_load_repository
spark_load_license >> spark_load_repository
spark_load_repository >> spark_load_repo_language
spark_load_repository >> spark_load_commit
spark_load_commit >> spark_repository_tag

spark_load_repo_language >> end
spark_repository_tag >> end
