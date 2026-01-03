import pendulum
import os
from dotenv import load_dotenv
from datetime import timedelta
from airflow.utils.dates import days_ago
from docker.types import Mount

from airflow import DAG

# Import de TaskGroup nécessaire
from airflow.utils.task_group import TaskGroup
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator

load_dotenv("/opt/airflow/.env")

START_DATE = pendulum.datetime(2025, 1, 1, tz="UTC")

with DAG(
    dag_id="DAG",
    start_date=START_DATE,
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=1)},
    tags=["data_pipeline", "insarama"],
) as dag:

    # 1. Groupe Ingestion
    with TaskGroup(group_id="EnvironnementConfiguration") as EnvironnementConfiguration:

        # build_imdb_image = BashOperator(
        #     task_id="build_imdb_image",
        #     bash_command="docker-compose -f /opt/airflow/dockerETL_images/compose.dockerETL.yaml build",
        # )
        build_imdb_image = BashOperator(
            task_id="build_imdb_image",
            # On construit l'image manuellement et on lui donne le tag attendu par la suite
            bash_command="docker build -t ingestion/imdbcurler /opt/airflow/dockerETL_images/Ingestion/IMDBCurler/",
        )
        run_setup_script = BashOperator(
            task_id="run_setup_script",
            bash_command="bash /opt/airflow/setup_mongo_transient_storage.sh ",
            env={
                "MONGO_USERNAME": os.getenv("MONGO_USERNAME"),
                "MONGO_PASSWORD": os.getenv("MONGO_PASSWORD"),
                "MONGO_HOST_NAME": os.getenv("MONGO_HOST_NAME"),
                "MONGO_PORT": os.getenv("MONGO_PORT"),
                "MONGO_DB": os.getenv("MONGO_DB"),
                "MONGO_HEALTHCHECK_RETRIES": os.getenv("MONGO_HEALTHCHECK_RETRIES"),
                "INSARAMA_NET": os.getenv("INSARAMA_NET"),
            },
        )

    with TaskGroup(group_id="Ingestion") as Ingestion_Group:

        # --- Tâche 1 : IMDB ---
        run_IMDBCurler = DockerOperator(
            task_id="run_IMDBCurler",
            container_name="IMDBCurler",
            image="ingestion/imdbcurler",
            api_version="auto",
            auto_remove=True,
            command="sh -c '/app/scripts/start.sh'",
            docker_url="unix://var/run/docker.sock",
            network_mode="insarama_net",
            mount_tmp_dir=False,
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

        # --- Tâche 2 : Metacritic ---

        run_scrapper = DockerOperator(
            task_id="run_scrapper",
            container_name="MetaCriticScrapper",
            image="ingestion/metacritic_scrapper:latest",
            api_version="auto",
            auto_remove=True,
            command="sh -c '/app/scripts/start.sh'",
            docker_url="unix://var/run/docker.sock",
            network_mode="IngestionNet",
            force_pull=False,
            environment={
                "MONGO_USERNAME": os.getenv("MONGO_USERNAME"),
                "MONGO_PASSWORD": os.getenv("MONGO_PASSWORD"),
                "MONGO_HOST_NAME": os.getenv("MONGO_HOST_NAME"),
                "MONGO_PORT": os.getenv("MONGO_PORT"),
                "MONGO_DB": os.getenv("MONGO_DB"),
            },
        )

    # 2. Groupe Staging
    with TaskGroup(group_id="Staging") as Staging_Group:
        postgres_loader = DockerOperator(
            task_id="postgres_loader",
            container_name="SQLPersistor",
            image="insarama/sql_persistor",
            api_version="auto",
            auto_remove=True,
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

    # Définition de la dépendance entre les groupes
    # Le groupe Staging ne commencera que si le groupe Ingestion est un succès
    EnvironnementConfiguration >> Ingestion_Group >> Staging_Group
