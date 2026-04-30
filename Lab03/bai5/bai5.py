from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("OccupationRatings")
sc = SparkContext(conf=conf)

# Bước 1: Đọc file users.txt và tạo map UserID → OccupationID
users = sc.textFile("./users.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (int(fields[0]), int(fields[3])))  

# Đọc file occupation.txt để map ID → OccupationName
occupations = sc.textFile("./occupation.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (int(fields[0]), fields[1]))

occupationDict = occupations.collectAsMap()

# Bước 2: Đọc ratings và map UserID → (MovieID, Rating)
ratings1 = sc.textFile("./ratings_1.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (int(fields[0]), float(fields[2])))

ratings2 = sc.textFile("./ratings_2.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (int(fields[0]), float(fields[2])))

ratings = ratings1.union(ratings2)

# Bước 3: Join ratings với users để thêm OccupationID
ratingsWithOcc = ratings.join(users)

# Bước 4: Map thành (OccupationID, (Rating, 1))
occRatings = ratingsWithOcc.map(lambda x: (x[1][1], (x[1][0], 1)))

# Reduce để tính tổng điểm và số lượt
occTotals = occRatings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Tính trung bình
occAverages = occTotals.mapValues(lambda x: (x[0] / x[1], x[1]))

# In kết quả
for occID, (avg, count) in occAverages.collect():
    print(f"Occupation: {occupationDict[occID]}, Điểm TB = {avg:.2f}, Số lượt = {count}")
