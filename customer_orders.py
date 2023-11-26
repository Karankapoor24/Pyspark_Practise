from pyspark import SparkContext

sc = SparkContext("local[*]", "custOrders")
sc.setLogLevel("ERROR")
inp = sc.textFile("C:/Users/karan/OneDrive/Desktop/Codes/Data/customer-orders.csv")
rdd1 = inp.map(lambda x : (x.split(",")[0], float(x.split(",")[2])))
rdd2 = rdd1.reduceByKey(lambda x,y: x+y)
rdd3 = rdd2.sortBy(lambda x: x[1],False)
result = rdd3.collect()

for a in result:
    print(a)
