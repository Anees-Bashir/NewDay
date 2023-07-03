from pyspark.sql import SparkSession, types
from pyspark.sql.types import *
from pyspark.sql.functions import rank, col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


spark = SparkSession \
    .builder \
    .appName("Techtest") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


df_min_max_avg = spark.read.parquet("/root/techtest/final_datasets/min_max_avg_by_movie/*")
df_min_max_avg.show(10)

df_top_three_final = spark.read.parquet("/root/techtest/final_datasets/top_three_movies_by_user/*")
df_top_three_final.show(10)

df_movies = spark.read.parquet("/root/techtest/final_datasets/original_movies/*")
df_movies.show(10)

df_ratings = spark.read.parquet("/root/techtest/final_datasets//original_ratings/*")
df_ratings.show(10)

