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

def drop_col_movies(row: tuple):
    movie_id, title, _ = row.split(",")
    return movie_id, title

movie_rdd = movie_rdd.map(drop_col_movies)

#UserID, MovieID, Rating, Timestamp
def drop_col_ratings(row:tuple):
    user_id, movieid, rating, _ = row.split(',')
    return user_id ,( movieid, rating)
ratings_rdd = ratings_rdd.map(drop_col_ratings)
#UserID,(MovieID, Rating)

#UserID, Gender, Age, Occupation, Zip-code
def drop_col_user(row: tuple):
    user_id, gender, _, _, _ = row.split(",")
    return user_id, gender
user_gender_rdd = users_rdd.map(drop_col_user)
#UserID, Gender

ratings_users_gender_rdd = ratings_rdd.join(user_gender_rdd)
# UserID, ((movie_id, rating), gender)
ratings_users_gender_rdd = ratings_users_gender_rdd.map(lambda x: (x[1][0][0], (x[1][0][1], x[1][1], 1)))
# movie, (userID, rating, gender)

def map_by_gender(row):
    movie_id, (rating, gender, _) = row
    r = float(rating)
    if gender == 'F':
        return movie_id, (r, 1, 0, 0)
    else:
        return movie_id, (0, 0, r, 1)
    
movies_genders__ratings_rdd = ratings_users_gender_rdd.map(map_by_gender)

def sum_by_gender(a, b):
    return (a[0]+b[0], a[1]+b[1], a[2]+b[2], a[3]+b[3])
movies_genders__ratings_rdd = movies_genders__ratings_rdd.reduceByKey(sum_by_gender)


def calc_avg_by_gender(row):
    movie_id, (f_total, f_count, m_total, m_count) = row
    f_avg = f_total / f_count if f_count > 0 else 0
    m_avg = m_total / m_count if m_count > 0 else 0
    return movie_id, (f_avg, f_count, m_avg, m_count)

#(movie_id, (f_avg, f_count, m_avg, m_count)
movies_genders__ratings_rdd = movies_genders__ratings_rdd.map(calc_avg_by_gender)

#(movie_id, (f_avg, f_count, m_avg, m_count)
result_rdd = movies_genders__ratings_rdd.join(movie_rdd)

#(movie_id, ((f_avg, f_count, m_avg, m_count), title)
for row in result_rdd.collect():
    movie_id, ((f_avg, f_count, m_avg, m_count), title) = row
    print(f"{title} - F: {f_avg:.2f} ({f_count}) - M: {m_avg:.2f} ({m_count})")

