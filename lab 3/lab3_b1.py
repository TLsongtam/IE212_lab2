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

def drop_col_movies(row:str)-> tuple[str]:
    movie_id, title, _ = row.split(",")
    return movie_id, title

movie_rdd = movie_rdd.map(drop_col_movies)

def drop_col_ratings(row:str) -> tuple[str]:
    _, movie_id, ratings,_ = row.split(",")
    return movie_id, (float(ratings), 1)

ratings_rdd = ratings_rdd.map(drop_col_ratings)

total_ratings_count = sc.accumulator(0)

# Hàm tăng biến này
def count_ratings(row):
    global total_ratings_count
    total_ratings_count += 1  # tăng mỗi khi gặp 1 dòng
    return row

ratings_rdd.foreach(count_ratings)

def get_total_ratings(v1: tuple, v2: tuple):
    rating_1, c1 = v1
    rating_2, c2 = v2
    return rating_1 + rating_2, c1 + c2

total_rating_rdd = ratings_rdd.reduceByKey(get_total_ratings)

def get_avg_ratings(k: tuple):
    movie_id, (total_ratings, count) = k
    avg_ratings = total_ratings/count
    return movie_id, (avg_ratings, count)

avg_ratings_rdd = total_rating_rdd.map(get_avg_ratings)

movie_statistic_rdd = movie_rdd.join(avg_ratings_rdd) #(movie_id, titile, (avg_ratings, count))

for row in movie_statistic_rdd.collect():
    _, (title, (avg_ratings, count)) = row
    print(f"Title: {title} - avg. Rating {avg_ratings:.2f} - Total ratings {count}")

print(f"Tổng số ratings: {total_ratings_count}")

movie_have_more_10_ratings_rdd = movie_statistic_rdd.filter(
    lambda x: x[1][1][1] > 10
)
movie_have_more_10_ratings_rdd.sortBy(
    lambda x: x[1][1][0], ascending=False
)
top_movie = movie_have_more_10_ratings_rdd.first()
_, (title, (avg_rating, count)) = top_movie
print(f"Phim điểm cao nhất: {title} - {avg_rating:.2f} ({count} đánh giá)")