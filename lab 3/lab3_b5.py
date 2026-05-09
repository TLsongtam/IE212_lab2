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

#user_id, gender, age, occ, zipcode
def drop_col_user (row: tuple):
    user_id, _, _, occ, _ = row.split(",")
    return user_id, occ
user_occ_rdd = users_rdd.map(drop_col_user)
user_occ_dict = user_occ_rdd.collectAsMap()
#user_id, occ

#UserID, MovieID, Rating, Timestamp
def drop_col_ratings(row: tuple):
    user_id, _, rating, _ = row.split(',')
    return user_id , rating
ratings_rdd = ratings_rdd.map(drop_col_ratings)
#UserID, Rating

def extract_occupation(row: str):
    occ_id, occ_name = row.split(",")
    return occ_id, occ_name.strip()
occ_dict = occ_rdd.map(extract_occupation).collectAsMap()


def assign_occupation(row: tuple):
    user_id, rating = row
    occ_id = user_occ_dict.get(user_id, "0")   # ✅ dict
    occupation = occ_dict.get(occ_id, "unknown") 
    return occupation, float(rating)

occupation_rating_rdd = ratings_rdd.map(assign_occupation)
#(occ, rating)


def reshape(row: tuple):
    occ, rating = row
    return occ, (rating, 1)
occupation_rating_rdd = occupation_rating_rdd.map(reshape)
#occ(rating, 1)

occupation_avg_rdd = occupation_rating_rdd \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda x: (x[0] / x[1], x[1]))


# Kết quả: (occupation, rating)
for row in occupation_avg_rdd.collect():
    occupation, (avg_rating, count )= row
    print(f"{occupation} - Avg Rating: {avg_rating:.2f} - Count: {count}")