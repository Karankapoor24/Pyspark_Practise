from pyspark import SparkContext

sc = SparkContext("local[*]", "movieData")
sc.setLogLevel("ERROR")
inp = sc.textFile("C:/Users/karan/OneDrive/Desktop/Codes/Data/movie-data.txt")
rdd1 = inp.map(lambda x : (x.split("\t")[2], 1))
rdd2 = rdd1.reduceByKey(lambda x,y: x+y)
rdd3 = rdd2.sortBy(lambda x: x[1],False)
result = rdd3.collect()

for a in result:
    print(a)