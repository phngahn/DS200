from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("GenreRatings")
sc = SparkContext(conf=conf)

# Bước 1: Đọc file movies.txt và tạo map MovieID → List of Genres
movies = sc.textFile("./movies.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (int(fields[0]), fields[2].split("|")))  # Genres cách nhau bằng "|"

# Bước 2: Đọc ratings và map MovieID → Rating
ratings1 = sc.textFile("./ratings_1.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (int(fields[1]), float(fields[2])))

ratings2 = sc.textFile("./ratings_2.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (int(fields[1]), float(fields[2])))

ratings = ratings1.union(ratings2)

# Bước 3: Join ratings với movies để có (MovieID, (Rating, Genres))
joined = ratings.join(movies)

# Bước 4: Tách ra (Genre, Rating)
genreRatings = joined.flatMap(lambda x: [(genre, x[1][0]) for genre in x[1][1]])

# Bước 5: Tính trung bình cho từng genre
genreTotals = genreRatings.map(lambda x: (x[0], (x[1], 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

genreAverages = genreTotals.mapValues(lambda x: x[0] / x[1])

# In kết quả
for genre, avg in genreAverages.collect():
    print(f"{genre}: Điểm TB = {avg:.2f}")
