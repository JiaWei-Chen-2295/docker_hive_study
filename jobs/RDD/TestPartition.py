from pyspark import SparkConf, SparkContext
# 定义自定义分区规则：根据 key 值的最后一位数字分配 (0-9)
def MyPartitioner(key):
    print("MyPartitioner is running")
    print('The key is %d' % key)
    return key % 10
def main():
    print("The main function is running")
    # 环境配置：本地模式运行
    conf = SparkConf().setMaster("local").setAppName("MyApp")
    sc = SparkContext(conf=conf)
    # 创建 0-9 的数据，初始分 5 个区
    data = sc.parallelize(range(10), 5)
    # 核心操作：
    # 1. 转为键值对 (x, 1) -> 2. 按自定义规则分为 10 个区 -> 3. 还原数据格式 -> 4. 保存到本地
    data.map(lambda x: (x, 1)) \
        .partitionBy(10, MyPartitioner) \
        .map(lambda x: x[0]) \
        .saveAsTextFile("file:///opt/spark/jobs/rdd/partitioner")

if __name__ == '__main__':
    main()
