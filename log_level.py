from pyspark import SparkContext, RDD

def log(line):
    cols = line.split(":")
    lev = cols[0]
    return (lev,1)

sc = SparkContext("local[*]", "log-level")
sc.setLogLevel("ERROR")

sample_list = ["WARN: Thursday 13 July 4104",
                "ERROR: Thursday 13 July 4107",
                "ERROR: Thursday 13 July 4107",
                "ERROR: Thursday 13 July 4107",
                "WARN: Thursday 13 July 4104",
                "ERROR: Thursday 13 July 4107"
]

listRDD = sc.parallelize(sample_list)
log_level_extract = listRDD.map(log)
final_out = log_level_extract.reduceByKey(lambda x,y: x+y)
result = final_out.collect()

for a in result:
    print(a)

