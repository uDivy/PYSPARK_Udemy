from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("challenge")
sc = SparkContext(conf=conf)


def sliced(line):
    divide = line.split(',')
    return divide[1], divide[2]


readRDD = sc.textFile("file:///D:/STUDY/Data Engineering/PYSPARK/data/udemy_data/fakefriends.csv")
mappedRDD = readRDD.map(sliced)
num_lst = list(mappedRDD.countByKey().items())
newRDD = sc.parallelize(num_lst)
nextRDD = mappedRDD.reduceByKey(lambda x, y: int(x)+int(y))

# ((name), (sum_of_age, total_num)
finalRDD = nextRDD.join(newRDD)
outRDD = finalRDD.mapValues(lambda a: int(a[0])//int(a[1]))
for k, v in outRDD.collect():
    print("Average age of {}'s is {}".format(k, v))

