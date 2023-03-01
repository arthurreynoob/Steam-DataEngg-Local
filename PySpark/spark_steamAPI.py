import sys
from pyspark.sql import SparkSession

import UDF_forPyspark as UDF

print("\n Initializing Spark Session \n")

spark = SparkSession.builder \
.master("local[*]") \
.config("spark.driver.memory","8g") \
.appName('steamAPI') \
.getOrCreate()

print(f"\n {spark} \n")

parquet_file = sys.argv[1]
postgres_db = sys.argv[2]
postgres_user = sys.argv[3]
postgres_pwd = sys.argv[4]
date_name = sys.argv[5]

df_steamapi = spark.read.parquet(parquet_file)

print("\n\n Price Overview \n\n")
df_price = UDF.steamapi_price_overview(df_steamapi)
table_name = f"steamapi_price_{date_name}"
UDF.spark_to_postgres(df_price,postgres_db,table_name,postgres_user,postgres_pwd)


print("\n\n Metacritic \n\n")
df_metacritic = UDF.steamapi_metacritic(df_steamapi)
table_name = f"steamapi_metacritic_{date_name}"
UDF.spark_to_postgres(df_metacritic,postgres_db,table_name,postgres_user,postgres_pwd)


print("\n\n Categories \n\n")
df_categories = UDF.steamapi_categories(df_steamapi)
table_name = f"steamapi_categories_{date_name}"
UDF.spark_to_postgres(df_categories,postgres_db,table_name,postgres_user,postgres_pwd)


print("\n\n genres \n\n")
df_genres = UDF.steamapi_genres(df_steamapi)
table_name = f"steamapi_genres_{date_name}"
UDF.spark_to_postgres(df_genres,postgres_db,table_name,postgres_user,postgres_pwd)


print("\n\n raw \n\n")
table_name = f"steamapi_raw_{date_name}"
UDF.spark_to_postgres(df_steamapi,postgres_db,table_name,postgres_user,postgres_pwd)
