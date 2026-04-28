"""
RDD 排序任务：
1. 读取多个输入文件中的整数
2. 全局升序排序
3. 输出 rank 和 value（每行两个整数）到新文件
"""
from pyspark import SparkConf, SparkContext


conf = SparkConf().setAppName("RankSortIntegers")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

input_path = "file:///opt/spark/jobs/data/file[1-3].txt"
output_dir = "hdfs://hadoop-namenode:8020/opt/spark/jobs/output/rank_sorted_result"

lines = sc.textFile(input_path)

# 兼容两种输入：每行一个整数，或一行包含多个以空白分隔的整数
numbers = (
    lines
    .map(lambda line: line.strip())
    .filter(lambda line: line)
    .flatMap(lambda line: line.split())
    .map(lambda token: int(token))
)

sorted_numbers = numbers.sortBy(lambda x: x, ascending=True)

# zipWithIndex 从 0 开始，因此 rank 需要 +1
ranked = sorted_numbers.zipWithIndex().map(lambda x: (x[1] + 1, x[0]))


print("=" * 44)
print("排序结果（rank\tvalue）")
print("=" * 44)
for rank, value in ranked.collect():
    print(f"{rank}\t{value}")

print("=" * 44)
print("结果未保存到文件（仅控制台输出）")
print("=" * 44)

sc.stop()
