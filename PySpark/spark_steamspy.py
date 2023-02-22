import sys
import pyspark
from pyspark.sql import SparkSession, types, functions as F
import pandas as pd
from pyspark.sql import types

import UDF_forPyspark as UDF

print("\n Initializing Spark Session \n")

spark = SparkSession.builder \
.master("local[*]") \
.config("spark.driver.memory","8g") \
.appName('steamSPY') \
.getOrCreate()

print(f"\n {spark} \n")

parquet_file = sys.argv[1]
postgres_db = sys.argv[2]
postgres_user = sys.argv[3]
postgres_pwd = sys.argv[4]
postgres_dbtable = sys.argv[5]


df_steamspy = spark.read.parquet(parquet_file)

print('\n writing to postgresql \n')

UDF.spark_to_postgres(df_steamspy,postgres_db,postgres_dbtable,postgres_user,postgres_pwd)
