from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType, BooleanType
from pyspark.sql import SparkSession
from itertools import combinations
from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, year, month, dayofmonth,expr, udf,col,rank,row_number,split
from datetime import datetime




schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("timestamp", LongType(), True)
])




#1.DataFrame API
if __name__ == "__main__":
    spark = SparkSession.builder.appName("PythonPi").getOrCreate()

    ratings = spark.read.schema(schema).option("header",True).csv("s3a://retail-employees/data/movies/ratings.csv")
    movies = spark.read.option("header",True).csv("s3a://retail-employees/data/movies/movies.csv")
    ratings_df = ratings.withColumn("year", year(from_unixtime("timestamp")))
    joined_df = ratings_df.join(movies, "movieId")


    most_rated_movies = joined_df.groupBy("year", "movieId","title").count()
    windowSpec = Window.partitionBy("year").orderBy(col("count").desc())
    most_rated_movies = most_rated_movies.withColumn("rank", rank().over(windowSpec)).filter(col("rank")==1)
    most_rated_movies.select("year", "title", "count").show()

#sql
    ratings.createOrReplaceTempView("ratings")
    movies.createOrReplaceTempView("movies")

    sql_query = """
        WITH RatingYear AS (
            SELECT movieId, rating, YEAR(FROM_UNIXTIME(timestamp)) AS year
            FROM ratings
        ),
        JoinedData AS (
            SELECT r.year, r.movieId, m.title, COUNT(*) AS count
            FROM RatingYear r
            JOIN movies m ON r.movieId = m.movieId
            GROUP BY r.year, r.movieId, m.title
        ),
        RankedMovies AS (
            SELECT year, title, count,
                   RANK() OVER (PARTITION BY year ORDER BY count DESC) AS rank
            FROM JoinedData
        )
        SELECT year, title, count
        FROM RankedMovies
        WHERE rank = 1
    """
    
    spark.sql(sql_query).show()


#rdd

    ratingsRDD = ratings.rdd
    moviesRDD = movies.rdd
    ratings_rdd_with_year = ratingsRDD.map(lambda row: (datetime.fromtimestamp(row.timestamp).year, row.movieId))
    ratings_count_per_year = ratings_rdd_with_year.map(lambda x: ((x[0], x[1]), 1)).reduceByKey(lambda a, b: a + b)
    most_rated_movies = ratings_count_per_year.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey() \
    .mapValues(lambda x: max(x, key=lambda y: y[1])).map(lambda x: (x[0], x[1][0], x[1][1]))

    movies_rdd_with_id = moviesRDD.map(lambda row: (int(row.movieId), row.title))
    joined_rdd = most_rated_movies.map(lambda x: (x[1], (x[0], x[2]))).join(movies_rdd_with_id).map(lambda x: (x[1][0][0], x[1][0][1], x[1][1]))
    result = joined_rdd.collect()


    
