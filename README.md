# Apache Hive + Spark Docker 学习环境

基于 Docker 搭建的 Apache Hive 4.0.0 + Spark 3.5.1 完整学习环境。

## 架构

| 服务 | 镜像 | 端口 | 说明 |
|------|------|------|------|
| hadoop-namenode | bde2020/hadoop-namenode:2.0.0-hadoop3.1.3-java8 | 9870, 8020 | HDFS NameNode |
| hadoop-datanode | bde2020/hadoop-datanode:2.0.0-hadoop3.1.3-java8 | 9864 | HDFS DataNode |
| mysql | mysql:8.0 | 3306 | Hive 元数据库 |
| hive-metastore | apache/hive:4.0.0 (自定义) | 9083 | Hive Metastore 服务 |
| hive-server | apache/hive:4.0.0 (自定义) | 10000, 10002 | HiveServer2 服务 |
| spark-master | apache/spark:3.5.1 (自定义) | 8080, 7077, 4040 | Spark Master 节点 |
| spark-worker | apache/spark:3.5.1 (自定义) | 8081 | Spark Worker 节点 |

## 首次启动

### 1. 拉取镜像（如果还没有）

```bash
docker pull apache/hive:4.0.0
docker pull mysql:8.0
docker pull bde2020/hadoop-namenode:2.0.0-hadoop3.1.3-java8
docker pull bde2020/hadoop-datanode:2.0.0-hadoop3.1.3-java8
docker pull apache/spark:3.5.1
```

### 2. 构建并启动

```bash
docker compose up --build -d
```

### 3. 等待 MySQL 健康检查通过后，初始化 HDFS 目录

等待约 30 秒让所有服务启动，然后执行：

```bash
docker exec hadoop-namenode sh -c "
  hadoop fs -mkdir -p /user/hive/warehouse &&
  hadoop fs -mkdir -p /tmp/hive &&
  hadoop fs -mkdir -p /spark-logs &&
  hadoop fs -chmod -R 777 /tmp &&
  hadoop fs -chmod 777 /spark-logs &&
  hadoop fs -chown -R hive:hive /user/hive
"
```

### 4. 等待 HiveServer2 就绪（约 60 秒）

可通过以下命令查看启动进度：

```bash
docker logs -f hive-server
```

看到 `Starting HiveServer2` 和 `Hive Session ID = ...` 后再等约 30 秒即可使用。

## 日常使用

### 数据目录挂载

`hive-server` 容器已挂载 `./doc` 到 `/data` 目录，可直接在 Hive 中使用本地数据文件：

```sql
-- 加载本地文件到 Hive 表
load data local inpath '/data/03-sql/stu.txt' into table student;
```

| 宿主机路径 | 容器内路径 |
|-----------|------------|
| ./doc/03-sql/stu.txt | /data/03-sql/stu.txt |
| ./doc/* | /data/* |

### 停止环境

```bash
docker compose stop
```

### 重新启动（数据不丢失）

```bash
docker compose start
```

> 注意：不要使用 `docker compose down -v`，`-v` 会删除所有数据卷，需要重新走首次启动流程。

### 使用 Beeline 连接 Hive

```bash
docker exec -it hive-server /opt/hive/bin/beeline -u "jdbc:hive2://localhost:10000/"
```
beeline -u jdbc:hive2://localhost:10000/default -n root
### 测试示例

```sql
SHOW DATABASES;
CREATE TABLE test (id INT, name STRING);
INSERT INTO TABLE test VALUES (1, 'zhangsan');
INSERT INTO TABLE test VALUES (2, 'lisi');
SELECT * FROM test;
SELECT COUNT(*) FROM test;
```

退出 Beeline：

```sql
!quit
```

## Web UI 地址

| 服务 | 地址 |
|------|------|
| HDFS 文件浏览 | http://localhost:9870 |
| HiveServer2 | http://localhost:10002 |
| Spark Master UI | http://localhost:8080 |
| Spark Worker UI | http://localhost:8081 |
| Spark Application UI | http://localhost:4040 |

Hive 数据在 HDFS 中的路径：`/user/hive/warehouse`

## Spark 使用

### 使用 Spark Shell 连接 Hive

```bash
# 进入 Spark Master 容器
docker exec -it spark-master /bin/bash

# 启动 Spark Shell（支持 Hive）
/opt/spark/bin/spark-shell
```

或者一步到位：

```bash
docker exec -it spark-master /opt/spark/bin/spark-shell
```

### Spark Shell 示例

```scala
// 查看 Hive 数据库
spark.sql("SHOW DATABASES").show()

// 查看 Hive 表
spark.sql("SHOW TABLES").show()

// 创建表并插入数据
spark.sql("CREATE TABLE IF NOT EXISTS spark_test (id INT, name STRING)")
spark.sql("INSERT INTO spark_test VALUES (1, 'spark_user1'), (2, 'spark_user2')")

// 查询数据
spark.sql("SELECT * FROM spark_test").show()

// 退出
:quit
```

### 使用 PySpark

```bash
# 进入 Spark Master 容器
docker exec -it spark-master /bin/bash

# 启动 PySpark
/opt/spark/bin/pyspark
```

或者一步到位：

```bash
docker exec -it spark-master /opt/spark/bin/pyspark
```

```Python
textFile = spark.read.text("hdfs://hadoop-namenode:8020/spark_study/README.md")
```

```python
# 查看 Hive 数据库
spark.sql("SHOW DATABASES").show()

# 读取 Hive 表
df = spark.sql("SELECT * FROM default.test")
df.show()

# 退出
exit()
```

### 提交 Spark 任务

```bash
# 在 Spark Master 容器内提交任务
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class org.apache.spark.examples.SparkPi \
  /opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar 100
```

## 数据卷删除后重新初始化

如果执行了 `docker compose down -v` 或需要完全重建：

```bash
# 1. 重新构建并启动
docker compose up --build -d

# 2. 等待约 30 秒后初始化 HDFS 目录
docker exec hadoop-namenode sh -c "
  hadoop fs -mkdir -p /user/hive/warehouse &&
  hadoop fs -mkdir -p /tmp/hive &&
  hadoop fs -mkdir -p /spark-logs &&
  hadoop fs -chmod -R 777 /tmp &&
  hadoop fs -chmod 777 /spark-logs &&
  hadoop fs -chown -R hive:hive /user/hive
"

# 3. 等待约 60 秒后即可使用 Beeline 连接
```

## 排查问题

```bash
# 查看各服务日志
docker logs hive-server
docker logs hive-metastore
docker logs hive-mysql
docker logs hadoop-namenode
docker logs hadoop-datanode

# 检查 HDFS 健康状态
docker exec hadoop-namenode sh -c "hadoop dfsadmin -report"

# 检查容器状态
docker compose ps
```

## 项目文件

```
docker_hive/
├── docker-compose.yml   # 编排配置（7 个服务）
├── init.sql             # MySQL 初始化脚本
├── init-hive.sql        # Hive 测试 SQL
├── README.md            # 本文件
├── hive/
│   ├── Dockerfile       # 基于 apache/hive:4.0.0，添加 MySQL Connector/J
│   └── hive-site.xml    # Hive 配置（MySQL 元数据库、HDFS、MapReduce 本地模式）
└── spark/
    ├── Dockerfile       # 基于 apache/spark:3.5.1，集成 Hive
    ├── entrypoint.sh    # Spark 启动脚本
    ├── hive-site.xml    # Spark 连接 Hive Metastore 配置
    └── spark-defaults.conf  # Spark 默认配置
```

## 连接信息

| 项目 | 值 |
|------|---|
| MySQL 用户 | hive |
| MySQL 密码 | hivepassword |
| MySQL 数据库 | metastore_db |
| Beeline JDBC URL | jdbc:hive2://localhost:10000/ |
| HDFS NameNode | hdfs://hadoop-namenode:8020 |
| Spark Master | spark://spark-master:7077 |
| Hive Metastore URI | thrift://hive-metastore:9083 |
