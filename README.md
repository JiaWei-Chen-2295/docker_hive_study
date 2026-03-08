# Apache Hive Docker 学习环境

基于 Docker 搭建的 Apache Hive 4.0.0 完整学习环境。

## 架构

| 服务 | 镜像 | 端口 | 说明 |
|------|------|------|------|
| hadoop-namenode | bde2020/hadoop-namenode:2.0.0-hadoop3.1.3-java8 | 9870, 8020 | HDFS NameNode |
| hadoop-datanode | bde2020/hadoop-datanode:2.0.0-hadoop3.1.3-java8 | 9864 | HDFS DataNode |
| mysql | mysql:8.0 | 3306 | Hive 元数据库 |
| hive-metastore | apache/hive:4.0.0 (自定义) | 9083 | Hive Metastore 服务 |
| hive-server | apache/hive:4.0.0 (自定义) | 10000, 10002 | HiveServer2 服务 |

## 首次启动

### 1. 拉取镜像（如果还没有）

```bash
docker pull apache/hive:4.0.0
docker pull mysql:8.0
docker pull bde2020/hadoop-namenode:2.0.0-hadoop3.1.3-java8
docker pull bde2020/hadoop-datanode:2.0.0-hadoop3.1.3-java8
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
  hadoop fs -chmod -R 777 /tmp &&
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

Hive 数据在 HDFS 中的路径：`/user/hive/warehouse`

## 数据卷删除后重新初始化

如果执行了 `docker compose down -v` 或需要完全重建：

```bash
# 1. 重新构建并启动
docker compose up --build -d

# 2. 等待约 30 秒后初始化 HDFS 目录
docker exec hadoop-namenode sh -c "
  hadoop fs -mkdir -p /user/hive/warehouse &&
  hadoop fs -mkdir -p /tmp/hive &&
  hadoop fs -chmod -R 777 /tmp &&
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
├── docker-compose.yml   # 编排配置（5 个服务）
├── init.sql             # MySQL 初始化脚本
├── init-hive.sql        # Hive 测试 SQL
├── README.md            # 本文件
└── hive/
    ├── Dockerfile       # 基于 apache/hive:4.0.0，添加 MySQL Connector/J
    └── hive-site.xml    # Hive 配置（MySQL 元数据库、HDFS、MapReduce 本地模式）
```

## 连接信息

| 项目 | 值 |
|------|---|
| MySQL 用户 | hive |
| MySQL 密码 | hivepassword |
| MySQL 数据库 | metastore_db |
| Beeline JDBC URL | jdbc:hive2://localhost:10000/ |
| HDFS NameNode | hdfs://hadoop-namenode:8020 |
