import pendulum
import os
from dotenv import load_dotenv
from datetime import timedelta
from airflow.utils.dates import days_ago

load_dotenv("/opt/airflow/.env")

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

START_DATE = pendulum.datetime(2025, 1, 1, tz="UTC")

with DAG(
    dag_id="Ingestion_DAG",
    start_date=START_DATE,  # days_ago(0) to change and test !!!
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=1)},
    tags=["ingestion"],
) as dag:

    # with TaskGroup(group_id='Ingestion') as Ingestion_Group: !!!
    run_scrapper = DockerOperator(
        task_id="run_scrapper",
        container_name="MetaCriticScrapper",
        image="ingestion/metacritic_scrapper:latest",
        api_version="auto",
        auto_remove=True,  # To make it disappear once it finishes
        command="sh -c '/app/scripts/start.sh'",
        docker_url="unix://var/run/docker.sock",
        network_mode="IngestionNet",
        force_pull=False,
        environment={
            "IMDB_FILES_TO_DOWNLOAD": os.getenv("IMDB_FILES_TO_DOWNLOAD"),
            "IMDB_DATA_URL": os.getenv("IMDB_DATA_URL"),
            "DATA_FILE_DIRECTORY": os.getenv("DATA_FILE_DIRECTORY"),
            "MONGO_USERNAME": os.getenv("MONGO_USERNAME"),
            "MONGO_PASSWORD": os.getenv("MONGO_PASSWORD"),
            "MONGO_HOST_NAME": os.getenv("MONGO_HOST_NAME"),
            "MONGO_PORT": os.getenv("MONGO_PORT"),
            "MONGO_DB": os.getenv("MONGO_DB"),
            "MONGO_MEDIA_COLLECTION": os.getenv("MONGO_MEDIA_COLLECTION"),
        },
    )

    # with TaskGroup(group_id='Staging') as Staging_Group:
    postgres_loader = DockerOperator(
        task_id="postgres_loader",
        container_name="SQLPersistor",
        image="insarama/sql_persistor",
        api_version="auto",
        auto_remove=True,  # To make it disappear once it finishes
        command="sh -c '/app/scripts/start.sh'",
        docker_url="unix://var/run/docker.sock",
        network_mode=os.getenv("INSARAMA_NET"),
        force_pull=False,
        environment={
            # Mongo
            "MONGO_USERNAME": os.getenv("MONGO_USERNAME"),
            "MONGO_PASSWORD": os.getenv("MONGO_PASSWORD"),
            "MONGO_HOST_NAME": os.getenv("MONGO_HOST_NAME"),
            "MONGO_PORT": os.getenv("MONGO_PORT"),
            "MONGO_DB": os.getenv("MONGO_DB"),
            "MONGO_MEDIA_COLLECTION": os.getenv("MONGO_MEDIA_COLLECTION"),
            "MONGO_RSET_NAME": os.getenv("MONGO_RSET_NAME"),
            # Postgres
            "DW_POSTGRES_HOST": os.getenv("DW_POSTGRES_HOST"),
            "DW_POSTGRES_PORT": os.getenv("DW_POSTGRES_PORT"),
            "DW_POSTGRES_DB": os.getenv("DW_POSTGRES_DB"),
            "DW_POSTGRES_USER": os.getenv("DW_POSTGRES_USER"),
            "DW_POSTGRES_PASSWORD": os.getenv("DW_POSTGRES_PASSWORD"),
            "DW_POSTGRES_LOAD_BATCH_SIZE": os.getenv("DW_POSTGRES_LOAD_BATCH_SIZE"),
        },
    )

    run_scrapper
