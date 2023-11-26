from pyspark import SparkContext, RDD

sc = SparkContext("local[*]", "wordcount")
sc.setLogLevel("ERROR")
inp = sc.textFile("C:/Users/karan/OneDrive/Desktop/Codes/Data/search_data.txt")
words = inp.flatMap(lambda x : x.split(" "))
word_counts = words.map(lambda x : (x.lower(),1))
final_count = word_counts.reduceByKey(lambda x,y : x+y)
out = final_count.sortBy(lambda x: x[1],False)
result = out.collect()
for a in result:
    print(a)