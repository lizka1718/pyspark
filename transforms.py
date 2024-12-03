from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType
from pyspark.sql import SparkSession
from itertools import combinations
from pyspark.sql import functions as F

schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("timestamp", LongType(), True)
])



if __name__ == "__main__":
    spark = SparkSession.builder.appName("PythonPi").getOrCreate()

    ratings = spark.read.schema(schema).option("header",True).csv("s3a://retail-employees/data/movies/ratings.csv")
    ratingsRDD = ratings.rdd


    movies = spark.read.option("header",True).csv("s3a://retail-employees/data/movies/movies.csv")
    moviesRDD = movies.rdd


    movieRatingsDF = movies.join(ratings, ratings["movieId"] == movies["movieId"])
    movieRatingsDF = movieRatingsDF.select(ratings["movieId"], "rating", "title", "genres")
    movieRatingsDF.printSchema()

    movieRatingsRDD = movieRatingsDF.rdd
    movieRatingsRDD = movieRatingsRDD.map(lambda x: (int(x[0]), (x[1], x[2], x[3])))
    movieRatingsRDD = movieRatingsRDD.mapValues(lambda x: (x[0], x[1].split(" "), x[2]))
    movieRatingsRDD = movieRatingsRDD.mapValues(lambda x: (x[0], x[1][-1], x[2]))
    movieRatingsRDD = movieRatingsRDD.mapValues(lambda x: (x[0], x[1].strip("()"), x[2]))
    movieRatingsRDD = movieRatingsRDD.mapValues(lambda x: (x[0], int(x[1]), x[2]))

    movieRatingsRDD = movieRatingsRDD.map(lambda x: (x[1], int(x[0]), x[2]))


    ratingsRDD = ratingsRDD.map(lambda x: (x['movieId'], x['rating']))
    averageRatingsRDD = ratingsRDD.aggregateByKey((0, 0), lambda acc, rating: (acc[0] + rating, acc[1] + 1),lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])).mapValues(lambda x: x[0] / x[1])
    averageRatingsRDD.collect()

 
    
    movies.show(15,False)

    links = spark.read.option("header",True).csv("s3a://retail-employees/data/movies/links.csv")
    links.show(15,False)

    tags = spark.read.option("header",True).csv("s3a://retail-employees/data/movies/tags.csv")
    tags.show(15,False)

    ratings.createOrReplaceTempView("ratings")
    tags.createOrReplaceTempView("tags")
    links.createOrReplaceTempView("links")
    movies.createOrReplaceTempView("movies")

    spark.sql("select * from links").show()

    
    moviesRDD = moviesRDD.map(lambda row: (row.movieId, (row.genres, row.title.split(" "))))
    moviesRDD = moviesRDD.map(lambda x: (int(x[0]), x[1]))

    moviesYears = moviesRDD.mapValues(lambda x: x[1][-1])
    moviesYears = moviesYears.mapValues(lambda x: x.strip("()"))
    moviesYears = moviesYears.mapValues(lambda x: x if x.isdigit() else None).filter(lambda x: x[1] is not None).mapValues(lambda x: int(x))
    moviesYears = moviesYears.map(lambda x: (x[1], 1))
    moviesYears.reduceByKey(lambda a, b: a + b).sortByKey(ascending=False).toDF().show(5,True)

    moviesGenres = moviesRDD.mapValues(lambda x: x[0])
    moviesGenres.flatMapValues(lambda x: x.split("|")).map(lambda x: (x[1], x[0])).groupByKey().sortByKey(ascending=True).mapValues(list).toDF().show(5,True)
    moviesGenres.mapValues(lambda x: x.split("|")).flatMap(lambda x: [(pair, 1) for pair in combinations(sorted(x[1]), 2)]).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False).toDF().show(5,True)
    
    moviesGenres.flatMapValues(lambda x: x.split("|")).map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False).toDF().show(5,True)




    spark.stop()