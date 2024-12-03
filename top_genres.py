from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType, BooleanType
from pyspark.sql import SparkSession
from itertools import combinations
from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, year, month, dayofmonth,expr, udf,col,rank,row_number,split
from datetime import datetime
from pyspark.sql import functions as f




schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("timestamp", LongType(), True)
])

if __name__ == '__main__':
    spark = SparkSession.builder.appName("PythonPi").getOrCreate()
    ratings = spark.read.schema(schema).option("header",True).csv("s3a://retail-employees/data/movies/ratings.csv")
    movies = spark.read.option("header",True).csv("s3a://retail-employees/data/movies/movies.csv")
    movies_with_genres = movies.withColumn("genre", f.explode(f.split("genres", "\\|")))
    
    movie_ratings = movies_with_genres.join(ratings, "movieId")
    avg_ratings = movie_ratings.groupBy("genre").agg(f.avg("rating")).show(5)


    movies_with_genres.createOrReplaceTempView("movies_with_genres")
    ratings.createOrReplaceTempView("ratings")


    query = """
SELECT genre, AVG(r.rating) AS avg_rating
from movies_with_genres m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY genre
ORDER BY avg_rating DESC
LIMIT 5
"""

    spark.sql(query).show()

#rdd
    moviesRDD = movies.rdd
    ratingsRDD = ratings.rdd

    movies_genres_rdd = moviesRDD.flatMap(lambda row: [(genre, row.movieId) for genre in row.genres.split("|")])
    ratings_genres_rdd = ratingsRDD.map(lambda row: (row.movieId, row.rating))
    
    
    
