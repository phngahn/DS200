from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("GenderRatings")
sc = SparkContext(conf=conf)

# Bước 1: Đọc file users.txt và tạo map UserID → Gender
users = sc.textFile("./users.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (int(fields[0]), fields[1]))  # (UserID, Gender)

# Bước 2: Đọc ratings và map UserID → (MovieID, Rating)
ratings1 = sc.textFile("./ratings_1.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (int(fields[0]), (int(fields[1]), float(fields[2]))))

ratings2 = sc.textFile("./ratings_2.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (int(fields[0]), (int(fields[1]), float(fields[2]))))

ratings = ratings1.union(ratings2)

# Join với users để thêm giới tính
ratingsWithGender = ratings.join(users)

# Bước 3: Map thành (MovieID, (Gender, Rating))
movieGenderRatings = ratingsWithGender.map(lambda x: (x[1][0][0], (x[1][1], x[1][0][1])))

# Tính trung bình rating cho mỗi phim theo từng giới tính
movieGenderPairs = movieGenderRatings.map(lambda x: ((x[0], x[1][0]), (x[1][1], 1)))

totals = movieGenderPairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
averages = totals.mapValues(lambda x: x[0] / x[1])

# In kết quả
for (movieID, gender), avg in averages.collect():
    print(f"MovieID {movieID}, Gender {gender}, Điểm TB = {avg:.2f}")
