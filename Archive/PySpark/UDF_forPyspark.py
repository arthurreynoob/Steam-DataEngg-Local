import pyspark
from pyspark.sql import SparkSession, types, functions as F
import pandas as pd
from pyspark.sql import types

from pyspark.sql.types import IntegerType,BooleanType,DateType,MapType,StringType,StructType,StructField,FloatType, ArrayType
from pyspark.sql.functions import struct,explode,create_map, from_json, col


def spark_to_postgres(df,postgres_db,postgres_dbtable,postgres_user,postgres_pwd):

    df.write \
    .format("jdbc") \
    .option("url", postgres_db) \
    .option("dbtable", postgres_dbtable) \
    .option("user", postgres_user) \
    .option("password", postgres_pwd) \
    .mode("overwrite") \
    .save()


def steamapi_price_overview(df):

    price_schema = StructType([StructField('currency',StringType(),True), \
                    StructField('initial',IntegerType(),True), \
                    StructField('final',IntegerType(),True), \
                    StructField('discount_percent',FloatType(),True), \
                    StructField('initial_formatted',StringType(),True), \
                    StructField('final_formatted',StringType(),True)])

    df_price_overview = df.select('name','steam_appid','is_free', \
                                from_json('price_overview',price_schema).alias('price_overview2')) \
                                .select('name','steam_appid','is_free','price_overview2.*')

    return df_price_overview


def steamapi_metacritic(df):

    metacritic_schema = StructType([StructField('score',IntegerType(),True), \
                               StructField('url',StringType(),True)])

    df_metacritic = df.select('name','steam_appid', \
                              from_json('metacritic',metacritic_schema).alias('metacritic2')) \
                              .select('name','steam_appid','metacritic2.*')
    
    return df_metacritic


def steamapi_categories(df):

    categories_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("description", StringType(), True)])

    arrayschema = ArrayType(categories_schema)

    # Parse the 'categories' column into an array of structs
    df_parsed = df.select('steam_appid', \
                        from_json(col('categories'),arrayschema).alias('categories_array'))

    # Explode the array of structs into multiple rows
    df_exploded = df_parsed.select('steam_appid', \
                                explode('categories_array').alias('categories'))

    # Select the desired columns
    df_categories = df_exploded.select('steam_appid', \
                    df_exploded.categories.id.alias('categories_id'), \
                    df_exploded.categories.description.alias('categories_description') \
                    )

    return df_categories


def steamapi_genres(df):
    genre_schema = StructType([
    StructField("id", StringType(), True),
    StructField("description", StringType(), True)
    ])

    arrayschema = ArrayType(genre_schema)

    # Parse the 'genres' column into an array of structs
    df_parsed = df.select('steam_appid', \
                        from_json(col('genres'),arrayschema).alias('genres_array'))

    # Explode the array of structs into multiple rows
    df_exploded = df_parsed.select('steam_appid', \
                                explode('genres_array').alias('genres'))

    # Select the desired columns
    df_genres = df_exploded.select('steam_appid', \
                    df_exploded.genres.id.astype(IntegerType()).alias('genres_id'), \
                    df_exploded.genres.description.alias('genres_description') \
                    )

    return df_genres
    