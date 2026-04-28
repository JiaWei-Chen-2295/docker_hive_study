"""
综合实例2：每个省份销量前3名商品
从 sales.txt 中统计每个省份中每种商品的销量，再取每个省销量前3名的商品。
每行格式：时间戳,省份,城市,用户,商品
"""
from pyspark import SparkConf, SparkContext
from itertools import islice

conf = SparkConf().setAppName("TopProvinceProduct")
sc = SparkContext(conf=conf)
lines = sc.textFile("file:///opt/spark/jobs/data/sales.txt")
province_product = lines.map(lambda line: (line.split(",")[1], line.split(",")[4]))

# 统计每个 (省份, 商品) 的销量
counts = province_product.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)

# 按省份分组：((省份, 商品), 销量) -> (省份, [(商品, 销量)])
grouped = counts.map(lambda x: (x[0][0], (x[0][1], x[1])))

# 按省份聚合
province_groups = grouped.groupByKey()
def top3(iterable):
    return sorted(iterable, key=lambda x: x[1], reverse=True)[:3]

result = province_groups.mapValues(top3)
output = result.collect()

print("=" * 50)
print("每个省份销量前3名商品：")
print("=" * 50)
for province, products in sorted(output, key=lambda x: x[0]):
    print(f"\n省份 {province}:")
    for rank, (product, count) in enumerate(products, 1):
        print(f"  第{rank}名: 商品 {product}, 销量 {count}")

sc.stop()
