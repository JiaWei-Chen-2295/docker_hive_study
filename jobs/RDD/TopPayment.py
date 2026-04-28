"""
综合实例1：求 TOP 值
从多个文本文件中找出 payment 字段中最大的 Top 5 个值。
每行格式：orderid,userid,payment,productid
"""
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("TopPayment")
sc = SparkContext(conf=conf)

# 读取目录下所有文件（orders1.txt, orders2.txt, orders3.txt）
lines = sc.textFile("file:///opt/spark/jobs/data/orders*.txt")

# 解析每行，提取 payment 字段（第3个字段，索引2）
payments = lines.map(lambda line: int(line.split(",")[2]))

# 降序排序，取前5个
top5 = payments.sortBy(lambda x: x, ascending=False).take(5)

print("=" * 40)
print("Payment 字段最大的 Top 5 值：")
print("=" * 40)
for i, p in enumerate(top5, 1):
    print(f"第{i}名: {p}")

sc.stop()
