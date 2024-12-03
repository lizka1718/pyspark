from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType
from pyspark.sql import SparkSession
from itertools import combinations
from pyspark.sql.window import Window
from pyspark.sql.functions import rank,row_number,split
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

    movies_genres = movies.withColumn("genre", f.explode(f.split("genres", "\\|")))
    joined = movies_genres.join(ratings, "movieId")

    min_ratings = joined.groupBy("movieId", "genre").agg(f.count("rating").alias("rating_count"),f.avg("rating").alias("avg_rating")).filter("rating_count >= 10")

    windowSpec = Window.partitionBy("genre").orderBy(f.desc("avg_rating"))
    r_movies = min_ratings.withColumn("rank", rank().over(windowSpec)).filter(("rank <= 5"))
    joined2 = r_movies.join(movies, "movieId")
    result = joined2.select("genre", "title", "avg_rating").show()


    movies_genres.createOrReplaceTempView("movies")
    ratings.createOrReplaceTempView("ratings")

    query = """
SELECT genre, title, avg_rating
FROM (
    SELECT m.genre, mo.title, AVG(r.rating) AS avg_rating, COUNT(r.rating) AS rating_count,
           ROW_NUMBER() OVER (PARTITION BY m.genre ORDER BY AVG(r.rating) DESC) AS rank
    FROM (
        SELECT movieId, EXPLODE(SPLIT(genres, '\\|')) AS genre
        FROM movies
    ) m
    JOIN ratings r ON m.movieId = r.movieId
    JOIN movies mo ON m.movieId = mo.movieId
    GROUP BY m.genre, mo.title
    HAVING COUNT(r.rating) >= 10
) t
WHERE rank <= 5
"""
    spark.sql(query).show()



    moviesRDD = movies.rdd
    ratingsRDD = ratings.rdd

    movies_genres_rdd = moviesRDD.flatMap(lambda row: [(row.movieId, (row.title, genre)) for genre in row.genres.split("|")])
    ratings_rdd = ratingsRDD.map(lambda row: (row.movieId, float(row.rating)))

    movie_ratings_rdd = movies_genres_rdd.join(ratings_rdd)

    genre_movie_ratings_rdd = movie_ratings_rdd.map(lambda x: (x[1][0][1], (x[0], x[1][0][0], x[1][1])))


    
    