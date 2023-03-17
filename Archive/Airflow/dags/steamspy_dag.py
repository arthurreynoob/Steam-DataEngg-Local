# standard library imports
import datetime as dt
import os

#Necessary modules for airflow
from airflow import DAG # Directed Acyclic Graph
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator #python operator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

#User Defined Module
from User_Defined_Module import steamspy_download_all_page


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME","opt/airflow/")
PG_HOST = os.getenv("PG_HOST")
PG_USER=os.getenv('PG_USER')
PG_PASSWORD=os.getenv('PG_PASSWORD')
PG_PORT=os.getenv('PG_PORT')
PG_DATABASE=os.getenv('PG_DATABASE')

# Date-related variables (daily)
date_name = '{{ execution_date.strftime(\'%Y_%m_%d\') }}'
steamspy_data_folder = f"data/steamspy/{date_name}/*"

# Spark configuration
spark_master = "spark://spark:7077"
spark_app_folder = "/opt/airflow/pyspark"

# PostgreSQL configuration
postgres_driver_jar = "/opt/airflow/postgres/postgres_jar/postgresql-42.5.1.jar"
postgres_db = f"jdbc:postgresql://postgres/steam"
postgres_dbtable = f"public.SteamSpy_{date_name}"


default_args= {
    "start_date": days_ago(0),
    "schedule_interval": "@daily",
    "retries": 3,
    "retry_delay": dt.timedelta(minutes=2),
}

local_workflow = DAG(
    "local_ingest_steamspy_dag",
    default_args=default_args,
    max_active_runs = 1,
    tags=['steamspy_dag'],
)

with local_workflow:

    extract_steamspy_task = PythonOperator(
        task_id = "extract_steamspy",
        python_callable = steamspy_download_all_page,
        op_kwargs={
            "page_start":0,
            "page_end":61
            },
    )

    spark_steamspy_task = SparkSubmitOperator(
        task_id="spark_steamspy_task",
        application=f"{spark_app_folder}/spark_steamspy.py", # Spark application path created in airflow and spark cluster
        name="spark_test",
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":spark_master},
        application_args=[steamspy_data_folder,postgres_db,PG_USER,PG_PASSWORD,postgres_dbtable],
        jars=postgres_driver_jar,
        driver_class_path=postgres_driver_jar,
        )

    
    extract_steamspy_task  >> spark_steamspy_task