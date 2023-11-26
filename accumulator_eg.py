from pyspark import SparkContext, RDD

def func1(line):
    if len(line) == 0:
        my_accum.add(1)

sc = SparkContext("local[*]", "add-campaign")
sc.setLogLevel("ERROR")
inp = sc.textFile("C:/Users/karan/OneDrive/Desktop/Codes/Data/samplefile.txt")
my_accum = sc.accumulator(0)
inp.foreach(lambda x: func1(x))
print(my_accum.value)
