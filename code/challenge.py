from pyspark import SparkContext as sc
from pyspark.sql import SparkSession
from pyspark.sql import Row, functions
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/tmp").appName("PopularMovies").getOrCreate()

def mapper(line):
    fields = line.split(' ')
    return Row(USER_ID=int(fields[0]), MOV_ID=int(fields[1]), RATINGS=int(fields[2]), TIMESTMP=int(fields[3]))


lines = spark.sc.textFile("file:///D:/STUDY/Data Engineering/PYSPARK/data/udemy_data/ml-100k/u.data")
# create table rdd
movie = lines.map(mapper())

schema_movie = spark.createDataFrame(movie).cache()
schema_movie.createOrReplaceTempView("movie")

movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map( lambda xy: (xy[1],xy[0]) )
sortedMovies = flipped.sortByKey()

results = sortedMovies.collect()

for result in results:
    print(result)
