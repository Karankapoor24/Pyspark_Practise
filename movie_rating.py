from pyspark import SparkContext

def get_cols(x):
    line = x.split("::")
    return (int(line[1]),float(line[2]))


sc = SparkContext("local[*]", "persist")
sc.setLogLevel("ERROR")

movie_rdd = sc.textFile("C:/Users/karan/OneDrive/Desktop/Codes/Data/movies.dat")
rating_rdd = sc.textFile("C:/Users/karan/OneDrive/Desktop/Codes/Data/ratings.dat")

mapped_rdd = rating_rdd.map(lambda x: (int(x.split("::")[1]),float(x.split("::")[2])))
movie_counts = mapped_rdd.mapValues(lambda x: (x,1.0))
movie_totals = movie_counts.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).filter(lambda x: x[1][1] > 1000)
movie_rating = movie_totals.mapValues(lambda x: (x[0]/x[1]))
filtered_movies = movie_rating.filter(lambda x: x[1] > 4.5)

movies_mapped_rdd = movie_rdd.map(lambda x: (int(x.split("::")[0]),x.split("::")[2]))
joined_rdd = movies_mapped_rdd.join(filtered_movies)

top_movies = joined_rdd.map(lambda x: x[1])

result = top_movies.collect()

for a in result:
    print(a)


