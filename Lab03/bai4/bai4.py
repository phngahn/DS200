from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("AgeGroupRatings")
sc = SparkContext(conf=conf)

# Bước 1: Đọc file users.txt và tạo map UserID → Age Group
def age_to_group(age):
    age = int(age)
    if age <= 18:
        return "Teen"
    elif age <= 30:
        return "Young Adult"
    elif age <= 50:
        return "Adult"
    else:
        return "Senior"

users = sc.textFile("./users.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (int(fields[0]), age_to_group(fields[2])))  

# Bước 2: Đọc ratings và map UserID → (MovieID, Rating)
ratings1 = sc.textFile("./ratings_1.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (int(fields[0]), (int(fields[1]), float(fields[2]))))

ratings2 = sc.textFile("./ratings_2.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (int(fields[0]), (int(fields[1]), float(fields[2]))))

ratings = ratings1.union(ratings2)

# Join với users để thêm nhóm tuổi
ratingsWithAge = ratings.join(users)

# Bước 3: Map thành (MovieID, (AgeGroup, Rating))
movieAgeRatings = ratingsWithAge.map(lambda x: (x[1][0][0], (x[1][1], x[1][0][1])))

# Tính trung bình rating cho mỗi phim theo từng nhóm tuổi
movieAgePairs = movieAgeRatings.map(lambda x: ((x[0], x[1][0]), (x[1][1], 1)))

totals = movieAgePairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
averages = totals.mapValues(lambda x: x[0] / x[1])

# In kết quả
for (movieID, ageGroup), avg in averages.collect():
    print(f"MovieID {movieID}, AgeGroup {ageGroup}, Điểm TB = {avg:.2f}")
