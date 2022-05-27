import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

with DAG(
    dag_id="create_postgres_database",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
) as dag:
    create_user_table = PostgresOperator(
        task_id="create_user_table",
        postgres_conn_id="postgres-db",
        sql="""
            CREATE TABLE "USER"
            (
                id bigint,
                login varchar(200),
                html_url varchar(200) unique,
                type varchar(200),
                name varchar(400),
                blog varchar(400),
                location varchar(400),
                email varchar(200) unique,
                public_repos int,
                followers int,
                following int,
                created_at timestamp,
                updated_at timestamp
            )
        """
    )
    
    create_user_organization_table = PostgresOperator(
        task_id="create_user_organization_table",
        postgres_conn_id="postgres-db",
        sql="""
            CREATE TABLE "ORGANIZATION_MEMBER"
            (
                organization_id bigint,
                user_id bigint
            )
          """
    )
    
    create_license_table = PostgresOperator(
        task_id = "create_license_table",
        postgres_conn_id="postgres-db",
        sql = """
            CREATE TABLE "LICENSE"(
                key varchar(200),
                name varchar(200),
                spdx_id varchar(200),
                description text,
                implementation text,
                html_url varchar(200)
            )
        """
    )
    
    create_repository_table = PostgresOperator(
        task_id="create_repository_table",
        postgres_conn_id="postgres-db",
        sql="""
            CREATE TABLE "REPOSITORY"
            (
                id bigint,
                name varchar(200),
                full_name varchar(200),
                html_url varchar(200),
                description text,
                created_at timestamp,
                updated_at timestamp,
                pushed_at timestamp,
                homepage varchar(200),
                size int,
                stargazers_count int,
                watchers_count int,
                forks_count int,
                open_issues_count int,
                license_key varchar(200),
                organization_id bigint not null
            )
          """
    )
    
    create_commit_table = PostgresOperator(
        task_id = "create_commit_table",
        postgres_conn_id="postgres-db",
        sql = """
            CREATE TABLE "COMMIT"(
                sha varchar(200),
                date timestamp,
                message text,
                comment_count int,
                html_url varchar(200),
                author_id bigint not null,
                repository_id bigint not null
            )
        """
    )
    
    create_repository_tag_table = PostgresOperator(
        task_id = "create_repository_tag_table",
        postgres_conn_id="postgres-db",
        sql = """
            CREATE TABLE "REPOSITORY_TAG"(
                commit_sha varchar(200),
                name varchar(200),
                zipball_url varchar(200)
            )
        """
    )
    
    create_repository_language_table = PostgresOperator(
        task_id = "create_repository_language_table",
        postgres_conn_id="postgres-db",
        sql = """
            CREATE TABLE "REPOSITORY_LANGUAGE"(
                repository_id bigint,
                language varchar(200),
                used_count int
            )
        """
    )
    
    create_repository_contributor_table = PostgresOperator(
        task_id = "create_repository_contributor_table",
        postgres_conn_id="postgres-db",
        sql = """
            CREATE TABLE "REPOSITORY_CONTRIBUTOR"(
                repository_id bigint,
                contributor_id bigint,
                contributions int
            )
        """
    )
    
    create_repository_activity_table = PostgresOperator(
        task_id = "create_repository_activity_table",
        postgres_conn_id="postgres-db",
        sql = """
            CREATE TABLE "REPOSITORY_ACTIVITY"(
                repository_id bigint,
                datetime_updated timestamp
            )
        """
    )
    
    create_user_table >> create_repository_table
    create_user_table >> create_user_organization_table
    create_license_table >> create_repository_table
    create_repository_table >> create_commit_table 
    create_commit_table >> create_repository_tag_table
    create_repository_table >> create_repository_language_table
    create_commit_table >> create_repository_contributor_table
    create_repository_table >> create_repository_activity_table
    