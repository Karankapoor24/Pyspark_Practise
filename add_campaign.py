from pyspark import SparkContext, RDD

sc = SparkContext("local[*]", "add-campaign")
sc.setLogLevel("ERROR")
inp = sc.textFile("C:/Users/karan/OneDrive/Desktop/Codes/Data/bigdata-campaign.csv")
rdd1 = inp.map(lambda x : (float(x.split(",")[10]),x.split(",")[0]))
rdd2 = rdd1.flatMapValues(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda  x: (x[1].lower(),x[0]))
rdd4 = rdd3.reduceByKey(lambda x,y: x+y)
rdd5 = rdd4.sortBy(lambda x: x[1],False)
result = rdd5.collect()
for a in result:
    print(a)