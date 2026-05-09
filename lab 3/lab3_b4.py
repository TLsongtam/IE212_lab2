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
ratings_rdd = sc.textFile(r"D:\VisualStudio\IE212\lab 3\ratings")
users_rdd = sc.textFile(r"D:\VisualStudio\IE212\lab 3\users.txt")

#movie_id, title, genres
def drop_col_movies(row: tuple):
    movie_id, title, _ = row.split(",")
    return movie_id, title
movie_rdd = movie_rdd.map(drop_col_movies)
#movies_id, title

#UserID, MovieID, Rating, Timestamp
def drop_col_ratings(row: tuple):
    user_id, movieid, rating, _ = row.split(',')
    return user_id ,( movieid, rating)
ratings_rdd = ratings_rdd.map(drop_col_ratings)
#UserID,(MovieID, Rating)

#UserID, Gender, Age, Occupation, Zip-code
def drop_col_user(row: tuple):
    user_id, _, age, _, _ = row.split(",")
    age = int(age)
    if age < 20 :
        ages_group = '-20'
    elif (age > 19  and age < 30):
        ages_group = '20-29'
    elif (age > 29  and age < 40):
        ages_group = '30-39'
    elif (age > 39):
        ages_group = '40+'
    return user_id, ages_group
user_ages_group_rdd = users_rdd.map(drop_col_user)
#UserID, ages_group

ratings_user_ages_group_rdd = ratings_rdd.join(user_ages_group_rdd)
#User_id, ((movies_id, rating), ages_group)
ratings_user_ages_group_rdd = ratings_user_ages_group_rdd.map(lambda x: (x[1][0][0], (x[1][0][1], x[1][1])))
#Moive_id ( ratings, ages_group)

def map_by_ages_group(row: tuple):
    movie_id, (ratings, ages_group) = row
    r = float(ratings)
    if ages_group == '-20':
        return movie_id, (r, 1, 0, 0, 0, 0, 0, 0)
    elif ages_group == '20-29':
        return movie_id, (0, 0, r, 1, 0, 0, 0, 0)
    elif ages_group == '30-39':
        return movie_id, (0, 0, 0, 0, r, 1, 0, 0)
    elif ages_group == '40+':
        return movie_id, (0, 0, 0, 0, 0, 0, r, 1)
    
ratings_user_ages_group_rdd = ratings_user_ages_group_rdd.map(map_by_ages_group)

def sum_by_ages_group(a, b):
    return (a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3], a[4] + b[4], a[5] + b[5], a[6] + b[6], a[7] + b[7] )
ratings_user_ages_group_rdd = ratings_user_ages_group_rdd.reduceByKey(sum_by_ages_group)


def calc_avg (row):
    movie_id, (total_1, count_1, total_2, count_2, total_3, count_3, total_4, count_4 ) = row
    avg_1 = total_1/count_1 if count_1 > 0 else 0
    avg_2 = total_2/count_2 if count_2 > 0 else 0
    avg_3 = total_3/count_3 if count_3 > 0 else 0
    avg_4 = total_4/count_4 if count_4 > 0 else 0
    return movie_id, (avg_1, count_1, avg_2, count_2, avg_3, count_3, avg_4, count_4 )

ratings_user_ages_group_rdd = ratings_user_ages_group_rdd.map(calc_avg)

result_rdd = ratings_user_ages_group_rdd.join(movie_rdd)

for row in result_rdd.collect():
    movieid, ((avg_1, count_1, avg_2, count_2, avg_3, count_3, avg_4, count_4), title) = row
    print("-"+title)
    if count_1 != 0: print(f"\t[-20]-Avg: {avg_1:.2f}, count : {count_1} ") 
    if count_2 != 0: print(f"\t[20-29]-Avg: {avg_2:.2f}, count : {count_2} ")
    if count_3 != 0: print(f"\t[30-39]-Avg: {avg_3:.2f}, count : {count_3} ")
    if count_4 != 0: print(f"\t[40+]-Avg: {avg_4:.2f}, count : {count_4} ")

