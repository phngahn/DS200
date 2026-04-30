from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("MovieRatings")
sc = SparkContext(conf=conf)

# Bước 1: Đọc file movies.txt và tạo map MovieID → Title
movies = sc.textFile("./movies.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (int(fields[0]), fields[1]))

# Bước 2: Đọc ratings_1.txt và ratings_2.txt, map MovieID → (Rating, 1)
ratings1 = sc.textFile("./ratings_1.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (int(fields[1]), (float(fields[2]), 1)))

ratings2 = sc.textFile("./ratings_2.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (int(fields[1]), (float(fields[2]), 1)))

ratings = ratings1.union(ratings2)

# Bước 3: Reduce để tính tổng điểm và số lượt đánh giá
ratingTotals = ratings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Bước 4: Tính điểm trung bình
averageRatings = ratingTotals.mapValues(lambda x: (x[0] / x[1], x[1]))

# Danh sách phim có ít nhất 5 lượt đánh giá
filtered5 = averageRatings.filter(lambda x: x[1][1] >= 5)
movieNames = movies.collectAsMap()

print("Danh sách phim có ít nhất 5 lượt đánh giá:")
for movieID, (avgRating, count) in filtered5.collect():
    print(f"{movieID}, {movieNames[movieID]}, Điểm TB = {avgRating:.2f}, Số lượt = {count}")

# Bước 5: Tìm phim có điểm trung bình cao nhất với ít nhất 50 lượt đánh giá
filtered50 = averageRatings.filter(lambda x: x[1][1] >= 50)

if filtered50.isEmpty():
    print("Không có phim nào có ít nhất 50 lượt đánh giá.")
else:
    topMovie = filtered50.max(key=lambda x: x[1][0])
    print(f"\nPhim có điểm trung bình cao nhất (>=50 lượt): {movieNames[topMovie[0]]}, "
          f"Điểm TB = {topMovie[1][0]:.2f}, Số lượt = {topMovie[1][1]}")
