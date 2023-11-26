from pyspark import SparkContext, RDD

def func1(line):
    fields = line.split(",")
    if int(fields[1]) > 18:
        return (fields[0], fields[1], fields[2], "Yes")
    return (fields[0], fields[1], fields[2], "No")

def parse_line(line):
    fields = line.split(",")
    station = fields[0]
    entry_type = fields[2]
    temprature = float(fields[3])
    return (station,entry_type,temprature)

sc = SparkContext("local[*]", "add-campaign")
sc.setLogLevel("ERROR")

# A1 Solution
#inp = sc.textFile("C:/Users/karan/OneDrive/Desktop/Codes/Data/W9A1.csv")
#rdd1 = inp.map(func1)

# A2 Solution
lines = sc.textFile("C:/Users/karan/OneDrive/Desktop/Codes/Data/W9A2.csv")
parsed_lines = lines.map(parse_line)
min_temps = parsed_lines.filter(lambda  x: x[1] == "TMIN")
station_temps = min_temps.map(lambda x: (x[0],x[2]))
min_temp_station = station_temps.reduceByKey(lambda x,y: min(x,y))
result = min_temp_station.collect()


for a in result:
    print(a)
