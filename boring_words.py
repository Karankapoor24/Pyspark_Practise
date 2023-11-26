from pyspark import SparkContext, RDD

def load_boring_words():
    boring_words = set(line.strip() for line in open("C:/Users/karan/OneDrive/Desktop/Codes/Data/boringwords.txt"))
    return boring_words

sc = SparkContext("local[*]", "add-campaign")
sc.setLogLevel("ERROR")
inp = sc.textFile("C:/Users/karan/OneDrive/Desktop/Codes/Data/bigdata-campaign.csv")
name_set = sc.broadcast(load_boring_words())
rdd1 = inp.map(lambda x : (float(x.split(",")[10]),x.split(",")[0]))
rdd2 = rdd1.flatMapValues(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda  x: (x[1].lower(),x[0]))
rddf = rdd3.filter(lambda x: x[0] not in name_set.value)
rdd4 = rddf.reduceByKey(lambda x,y: x+y)
rdd5 = rdd4.sortBy(lambda x: x[1],False)
result = rdd5.collect()
for a in result:
    print(a)