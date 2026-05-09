from datetime import datetime
import os
os.environ["SPARK_HOME"] = r"D:\Quick Access\Downloads\spark-3.5.6-bin-hadoop3\spark-3.5.6-bin-hadoop3"
os.environ["PYSPARK_PYTHON"] = r"C:\Users\ADMIN\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\ADMIN\AppData\Local\Programs\Python\Python310\python.exe"

import sys
sys.path.insert(0, os.path.join(os.environ["SPARK_HOME"], "python"))
sys.path.insert(0, os.path.join(os.environ["SPARK_HOME"], "python", "lib", "pyspark.zip"))
sys.path.insert(0, os.path.join(os.environ["SPARK_HOME"], "python", "lib", "py4j-0.10.9.7-src.zip"))



from pyspark import SparkContext
sc = SparkContext.getOrCreate()



movie_rdd = sc.textFile(r"D:\VisualStudio\IE212\lab 3\movies.txt")
occ_rdd = sc.textFile(r"D:\VisualStudio\IE212\lab 3\occupation.txt")
ratings_rdd = sc.textFile(r"D:\VisualStudio\IE212\lab 3\ratings")
users_rdd = sc.textFile(r"D:\VisualStudio\IE212\lab 3\users.txt")

def drop_col_ratings (row:str)-> tuple[str]:
    user_id, movie_id, rating, timestamp = row.split(",")
    date = datetime.fromtimestamp(int(timestamp)).month
    return date,  (float(rating),1) 
date_movie_rdd = ratings_rdd.map(drop_col_ratings)

avg_month_rdd = date_movie_rdd \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda x: (x[0] / x[1], x[1]))

for row in avg_month_rdd.collect():
    month, (ratings, count) = row
    print(f"Monhth: {month} - Avg: {ratings:.2f} - Count: {count}")
#print(date_movie_rdd.count())