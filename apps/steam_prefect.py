import datetime
import pandas as pd
import os

import pyspark
from pyspark.sql import SparkSession, types, functions as F
from pyspark.sql.types import *

from prefect import task,flow
from prefect.tasks import task_input_hash

from User_Defined_Module import steamapi_start, steamspy_download_all_page
import UDF_forPyspark as UDF


@task
def get_monday_date_string():
    """
    Get the date of the monday of the week for folder or file name use
    
    Returns date in string format: "YYYY-MM-DD"
    """
    
    today = datetime.date.today()
    days_since_monday = today.weekday()  # Monday is 0, Sunday is 6
    monday = today - datetime.timedelta(days=days_since_monday)

    return monday.strftime("%Y-%m-%d")


@task(log_prints=True,retries=3,retry_delay_seconds=600,cache_key_fn=task_input_hash, cache_expiration=datetime.timedelta(days=7))
def UDF_steamAPI_start(folder ,file_name = 'steamapi'):
    """
    FOR STEAMAPI
    Makes use of User_Defined_Module.py to 
    download the steamapi data. Saves the data in the specified folder
    """

    steamapi_start(folder,file_name)

    return None

@task(log_prints=True,retries=3,retry_delay_seconds=600,cache_key_fn=task_input_hash, cache_expiration=datetime.timedelta(days=1))
def UDF_steamSpy_start(download_folder):
    """
    FOR STEAMSPY
    """

    steamspy_download_all_page(download_folder=download_folder)


@task(log_prints=True)
def initialize_spark():
    """
    Initialize spark
    """

    print("\n Initializing Spark Session \n")

    spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.driver.memory","8g") \
    .config("spark.jars", "../resources/postgres_jar/postgresql-42.5.1.jar") \
    .appName('steamAPI') \
    .getOrCreate()

    print(f"\n {spark} \n")

    return spark


@task(log_prints=True)
def read_data(spark,data_source,table_schema):
    """
    Reads the downloaded steamapi data that were saved in the specified
    download folder
    """
    df_spark = spark.read.schema(table_schema).parquet(data_source,header=True)

    print(f"Total num of records: {df_spark.count()}")

    return df_spark


@task(log_prints=True)
def UDF_unnest_data(df_spark):
    """
    Makes use of UDF_forPyspark.py to extract the nested data
    returns the unnested DataFrames and their respective table_names
    """
    
    df_raw = df_spark
    df_price = UDF.steamapi_price_overview(df_spark)
    df_metacritic = UDF.steamapi_metacritic(df_spark)
    df_categories = UDF.steamapi_categories(df_spark)
    df_genres = UDF.steamapi_genres(df_spark)

    return [df_raw, df_price, df_metacritic, df_categories, df_genres], \
            ['steamapi_raw','steamapi_price','steamapi_metacritic','steamapi_categories','steamapi_genres']

    # return [df_raw, df_price, df_categories, df_genres], \
    #     ['steamapi_raw','steamapi_price','steamapi_categories','steamapi_genres']

@task(log_prints=True)
def load_to_postgres(df_spark,table_name):
    """
    loads the dataframe into postgres
    """
    
    df_spark.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres/test_db") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", table_name) \
    .option("user", "root") \
    .option("password", "root") \
    .mode("overwrite") \
    .save()

    return None


@flow(name="download_steamWebAPI")
def steamAPI_ETL(steamapi_data_folder,table_schema):
    date_name = get_monday_date_string()
    UDF_steamAPI_start(folder=f"{steamapi_data_folder}/{date_name}")
    spark = initialize_spark()
    df_spark = read_data(spark,f"{steamapi_data_folder}/*",table_schema)
    df_list, table_names = UDF_unnest_data(df_spark)

    for i in range(len(df_list)):
        load_to_postgres(df_list[i],f"{table_names[i]}_{date_name.replace('-','_')}")


@flow(name="download_steamSpyAPI")
def steamSpy_ETL(steamspy_data_folder,table_schema):
    date_name = datetime.date.today().strftime("%Y-%m-%d")
    UDF_steamSpy_start(download_folder=f"{steamspy_data_folder}/{date_name}")
    spark = initialize_spark()
    df_spark = read_data(spark,f"{steamspy_data_folder}/{date_name}/*",table_schema)
    load_to_postgres(df_spark,table_name=f"steamspy_All_{date_name.replace('-','_')}")


@flow(name="Main_SteamProject_Local")
def Main_flow():
    
    schema_steamspy = StructType([
    StructField("appid", LongType(), True),
    StructField("name", StringType(), True),
    StructField("developer", StringType(), True),
    StructField("publisher", StringType(), True),
    StructField("score_rank", StringType(), True),
    StructField("positive", LongType(), True),
    StructField("negative", LongType(), True),
    StructField("userscore", LongType(), True),
    StructField("owners", StringType(), True),
    StructField("average_forever", LongType(), True),
    StructField("average_2weeks", LongType(), True),
    StructField("median_forever", LongType(), True),
    StructField("median_2weeks", LongType(), True),
    StructField("price", StringType(), True),
    StructField("initialprice", StringType(), True),
    StructField("discount", StringType(), True),
    StructField("ccu", LongType(), True)
        ])

    schema_steamapi = StructType([
    StructField("type", StringType(), True),
    StructField("name", StringType(), True),
    StructField("steam_appid", StringType(), True),
    StructField("required_age", StringType(), True),
    StructField("is_free", StringType(), True),
    StructField("dlc", StringType(), True),
    StructField("detailed_description", StringType(), True),
    StructField("about_the_game", StringType(), True),
    StructField("short_description", StringType(), True),
    StructField("supported_languages", StringType(), True),
    StructField("reviews", StringType(), True),
    StructField("header_image", StringType(), True),
    StructField("website", StringType(), True),
    StructField("pc_requirements", StringType(), True),
    StructField("mac_requirements", StringType(), True),
    StructField("linux_requirements", StringType(), True),
    StructField("developers", StringType(), True),
    StructField("publishers", StringType(), True),
    StructField("packages", StringType(), True),
    StructField("package_groups", StringType(), True),
    StructField("platforms", StringType(), True),
    StructField("metacritic", StringType(), True),
    StructField("categories", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("screenshots", StringType(), True),
    StructField("movies", StringType(), True),
    StructField("recommendations", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("support_info", StringType(), True),
    StructField("background", StringType(), True),
    StructField("background_raw", StringType(), True),
    StructField("content_descriptors", StringType(), True),
    StructField("price_overview", StringType(), True)
        ])


    steamSpy_ETL(steamspy_data_folder ="../resources/data/steamSpy", table_schema = schema_steamspy)

    steamAPI_ETL(steamapi_data_folder="../resources/data/steamapi", table_schema = schema_steamapi)

if __name__ == '__main__':
    Main_flow()
