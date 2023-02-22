# standard library imports
import datetime as dt
import os

#Necessary modules for airflow
from airflow import DAG # Directed Acyclic Graph
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator #Bash operator for linux
from airflow.operators.python import PythonOperator #python operator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

#User Defined Module
from User_Defined_Module import steamapi_start

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME","opt/airflow/")

PG_HOST = os.getenv("PG_HOST")
PG_USER=os.getenv('PG_USER')
PG_PASSWORD=os.getenv('PG_PASSWORD')
PG_PORT=os.getenv('PG_PORT')
PG_DATABASE=os.getenv('PG_DATABASE')


# Date-related variables (monthly)
date_name = '{{ execution_date.strftime(\'%B\') }}'
steamapi_data_folder = f"data/steamapi/{date_name}/*"

# Spark configuration
spark_master = "spark://spark:7077"
spark_app_folder = "/opt/airflow/pyspark"

# PostgreSQL configuration
postgres_driver_jar = "/opt/airflow/postgres/postgres_jar/postgresql-42.5.1.jar"
postgres_db = f"jdbc:postgresql://postgres/steam"
postgres_dbtable = "public.SteamAPI"


default_args= {
    "start_date": dt.datetime(2023,1,1),
    "schedule_interval": "@daily",
    "retries": 3,
    "retry_delay": dt.timedelta(minutes=2),
}


local_workflow = DAG(
    "local_ingest_steamapi_dag",
    default_args=default_args,
    max_active_runs = 1,
    tags=['steamapi_dag'],
)

with local_workflow:

    extract_steamapi_task = PythonOperator(
        task_id = "extract_steamapi",
        python_callable = steamapi_start,
        op_kwargs={},
    )

    spark_steamapi_task = SparkSubmitOperator(
        task_id="spark_steamapi_task",
        application=f"{spark_app_folder}/spark_steamAPI.py", # Spark application path created in airflow and spark cluster
        name="spark_test",
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":spark_master},
        application_args=[steamapi_data_folder,postgres_db,PG_USER,PG_PASSWORD,date_name],
        jars=postgres_driver_jar,
        driver_class_path=postgres_driver_jar,
        )

    
    extract_steamapi_task >> spark_steamapi_task