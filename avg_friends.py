from pyspark import SparkContext, RDD

sc = SparkContext("local[*]", "average-friends")
sc.setLogLevel("ERROR")
inp = sc.textFile("C:/Users/karan/OneDrive/Desktop/Codes/Data/friends-data.csv")
rdd1 = inp.map(lambda x : (int(x.split("::")[2]),int(x.split("::")[3])))
rdd2 = rdd1.map(lambda y: (y[0], (y[1],1) ))
rdd3 = rdd2.reduceByKey(lambda a,b: (a[0]+b[0], a[1] + b[1]))
rdd4 = rdd3.map(lambda  c: (c[0],c[1][0]/c[1][1]))
result = rdd4.collect()
for a in result:
    print(a)