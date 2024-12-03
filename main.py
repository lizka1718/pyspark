from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType, BooleanType
from pyspark.sql import SparkSession
from itertools import combinations
from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, year, month, dayofmonth,expr, udf,col,rank,row_number,split

schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("timestamp", LongType(), True)
])