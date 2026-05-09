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

movies_rdd = sc.textFile(r"D:\VisualStudio\IE212\lab 3\movies.txt")
ratings_rdd = sc.textFile(r"D:\VisualStudio\IE212\lab 3\ratings")


def extract_genre(row: str):
    movie_id, title, genres = row.split(",")
    genre_list = genres.strip().split("|")
    return [(movie_id, genre) for genre in genre_list]

genre_rdd = movies_rdd.flatMap(extract_genre)


#b1
movie_genres_rdd = genre_rdd.groupByKey().mapValues(list)
#b2
def extract_rating(row: str):
    _, movie_id, rating, _ = row.split(",")
    return movie_id, float(rating)

movieid_rating_rdd = ratings_rdd.map(extract_rating)
# Kết quả: (movie_id, rating)

# Join để ghép rating với list genres
joined_rdd = movieid_rating_rdd.join(movie_genres_rdd)
# Kết quả: (movie_id, (rating, [genre1, genre2, ...]))

# FlatMap ra từng (genre, rating)
genre_rating_rdd = joined_rdd.flatMap(
    lambda x: [(genre, x[1][0]) for genre in x[1][1]]
)
#b3
genre_rating_sum_rdd = genre_rating_rdd.mapValues(lambda r: (r, 1)) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda x: x[0] / x[1])
# Kết quả: (genre, avg_rating)

for row in genre_rating_sum_rdd.collect():
    genre, avg = row
    print(f"Genre: {genre} - Avg Rating: {avg:.2f}")