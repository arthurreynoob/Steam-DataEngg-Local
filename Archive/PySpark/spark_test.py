import pyspark
from pyspark.sql import SparkSession, types, functions as F
import pandas as pd
from pyspark.sql import types

spark = SparkSession.builder \
.master("local[*]") \
.config("spark.driver.memory","15g") \
.appName('test') \
.getOrCreate()

print(spark)
print("\n\n\n\n\n\n\n\n\n\n\n HELLO WORLD \n\n\n\n\n\n\n\n\n\n\n")