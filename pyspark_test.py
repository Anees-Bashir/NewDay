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

# MovieID::Title::Genres - from README file

movie_schema = StructType([
    StructField('MovieID', IntegerType(), True),
    StructField('Title', StringType(), True),
    StructField('Genres', StringType(), True)
])


# UserID::MovieID::Rating::Timestamp - from README file

rating_schema = StructType([
    StructField('UserID', IntegerType(), True),
    StructField('MovieID', StringType(), True),
    StructField('Rating', IntegerType(), True),
    StructField('Timestamp', IntegerType(), True)
])
    
df_movies = spark.read.format("csv").schema(movie_schema).option("delimiter", "::").load('./ml-1m/movies.dat')

df_ratings = spark.read.format("csv").schema(rating_schema).option("delimiter", "::").load('./ml-1m/ratings.dat')




df_min_rating = df_ratings.groupby('MovieID').agg({'Rating': 'min'})

df_max_rating = df_ratings.groupby('MovieID').agg({'Rating': 'max'})

df_avg_rating = df_ratings.groupby('MovieID').agg({'Rating': 'avg'})




df_moives_ratings = df_movies.join(df_min_rating, 'MovieID', 'inner')

df_moives_ratings = df_moives_ratings.join(df_max_rating, 'MovieID', 'inner')

df_moives_ratings = df_moives_ratings.join(df_avg_rating, 'MovieID', 'inner')



# --------------------------------------------------------------------------------------------------------------------------------------------------------------------


partition = Window.partitionBy("UserID").orderBy(col('Rating').desc(), col('Timestamp').desc())


# use row_number instead of rank/dense_rank() because we had record with the same timestamp
df_rating_with_rank = df_ratings.withColumn('RANK', row_number().over(partition))

df_top_three = df_rating_with_rank.filter('RANK <= 3')


#  join to movie dataset to pull through movie name 

df_top_three_with_title = df_top_three.join(df_movies, 'MovieID', 'inner') 

# select final dataset columns 

df_top_three_final = df_top_three_with_title.select('UserID', 'MovieID', 'Title', 'RANK')


# df_top_three_final.show(1000)

# --------------------------------------------------------------------------------------------------------------------------------------------------------------


df_top_three_final.write.parquet('./final_datasets/top_three_movies_by_user')

df_moives_ratings.write.parquet('./final_datasets/min_max_avg_by_movie')
