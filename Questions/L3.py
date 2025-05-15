"""

You are analyzing a social network dataset at Google.
Your task is to find mutual friends between two users, Karl and Hans.
There is only one user named Karl and one named Hans in the dataset.

The output should contain 'user_id' and 'user_name' columns.

users

user_id	user_name
1		Karl
2		Hans
3		Emma
4		Emma
5		Mike
6		Lucas
7		Sarah
8		Lucas
9		Anna
10		John


friends

user_id	friend_id
1		3
1		5
2		3
2		4
3		1
3		2
3		6
4		7
5		8
6		9
7		10
8		6
9		10
10		7
10		9


Output

user_id	user_name
3		Emma

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# Initialize Spark Session
spark = SparkSession.builder.appName("UsersAndFriendsDataFrames").getOrCreate()

# Define schema for users table
users_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("user_name", StringType(), True)
])

# Define schema for friends table
friends_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("friend_id", IntegerType(), True)
])

# Create data for users table
users_data = [
    (1, 'Karl'), (2, 'Hans'), (3, 'Emma'), (4, 'Emma'), (5, 'Mike'),
    (6, 'Lucas'), (7, 'Sarah'), (8, 'Lucas'), (9, 'Anna'), (10, 'John')
]

# Create data for friends table
friends_data = [
    (1, 3), (1, 5), (2, 3), (2, 4), (3, 1), (3, 2), (3, 6), (4, 7), (5, 8),
    (6, 9), (7, 10), (8, 6), (9, 10), (10, 7), (10, 9)
]

# Create DataFrames
users_df = spark.createDataFrame(users_data, schema=users_schema)
friends_df = spark.createDataFrame(friends_data, schema=friends_schema)

# Show DataFrames
users_df.show()
friends_df.show()

karl_alias = friends_df.alias("k")
hans_alias = friends_df.alias("h")
users_alias = users_df.alias("u")

karl_friends = karl_alias.join(users_alias, karl_alias.user_id == users_alias.user_id, "inner")\
               .filter(F.col("u.user_name") == "Karl")\
               .select("k.user_id", "u.user_name", "k.friend_id")
karl_friends.show()

hans_friends = hans_alias.join(users_alias, hans_alias.user_id == users_alias.user_id, "inner")\
               .filter(F.col("u.user_name") == "Hans")\
               .select("h.user_id", "u.user_name", "h.friend_id")
hans_friends.show()

ans = karl_friends.join(hans_friends,
                    karl_friends.friend_id == hans_friends.friend_id, "inner")\
    .select("karl_friends.friend_id")

ans.show()

