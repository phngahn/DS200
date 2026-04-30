from pyspark import SparkConf, SparkContext
import datetime

conf = SparkConf().setAppName("TimeRatings")
sc = SparkContext(conf=conf)

# Hàm chuyển timestamp → năm
def timestamp_to_year(ts):
    return datetime.datetime.fromtimestamp(int(ts)).year

# Bước 1: Đọc ratings và map (Year, (Rating, 1))
ratings1 = sc.textFile("./ratings_1.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (timestamp_to_year(fields[3]), (float(fields[2]), 1)))

ratings2 = sc.textFile("./ratings_2.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (timestamp_to_year(fields[3]), (float(fields[2]), 1)))

ratings = ratings1.union(ratings2)

# Bước 2: Reduce để tính tổng điểm và số lượt cho mỗi năm
yearTotals = ratings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Bước 3: Tính trung bình
yearAverages = yearTotals.mapValues(lambda x: (x[0] / x[1], x[1]))

# In kết quả
for year, (avg, count) in sorted(yearAverages.collect()):
    print(f"Năm {year}: Điểm TB = {avg:.2f}, Số lượt = {count}")
