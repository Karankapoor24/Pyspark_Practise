from pyspark import SparkContext, StorageLevel

sc = SparkContext("local[*]", "persist")
sc.setLogLevel("ERROR")

base_rdd = sc.textFile("C:/Users/karan/OneDrive/Desktop/Codes/Data/customer-orders.csv")
mapped_rdd = base_rdd.map(lambda x: (x.split(",")[0],float(x.split(",")[2])))
total_by_cust = mapped_rdd.reduceByKey(lambda x,y: x+y)
premier_cust = total_by_cust.filter(lambda x: x[1] > 5000)
doubled_amount = premier_cust.map(lambda x: (x[0],x[1]*2)).persist(
    StorageLevel.MEMORY_ONLY
)
result = doubled_amount.collect()

for a in result:
    print(a)
