# Apache Hive Docker Environment

This project sets up a complete Apache Hive environment using Docker containers with existing images. It includes:

- MySQL 8.0 for metadata storage
- Hadoop NameNode and DataNode (bde2020 images)
- Apache Hive 4.0.0 server and metastore with custom MySQL connector

## Prerequisites

- Docker Desktop installed and running
- Docker Compose installed
- The following images should be available locally:
  - `apache/hive:4.0.0`
  - `mysql:8.0`
  - `bde2020/hadoop-namenode:2.0.0-hadoop3.1.3-java8`
  - `bde2020/hadoop-datanode:2.0.0-hadoop3.1.3-java8`

These images can be pulled with:
```bash
docker pull apache/hive:4.0.0
docker pull mysql:8.0
docker pull bde2020/hadoop-namenode:2.0.0-hadoop3.1.3-java8
docker pull bde2020/hadoop-datanode:2.0.0-hadoop3.1.3-java8
```

## Getting Started

### 1. Start the Environment

Run the following command to start all services:

```bash
docker-compose up --build -d
```

Note the `--build` flag to ensure our custom Hive image with MySQL connector is built.

### 2. Check Service Status

Monitor the logs to ensure all services are running:

```bash
docker-compose logs -f
```

Wait for all services to be ready. The Hive server initialization might take a few minutes.

### 3. Initialize the Hive Metastore Schema

Once all services are running, initialize the Hive metastore:

```bash
docker exec -it hive-server schematool -initSchema -dbType mysql
```

### 4. Using Hive

#### Option 1: Direct Hive CLI

Access the Hive CLI through the hive-server container:

```bash
docker exec -it hive-server hive
```

#### Option 2: Beeline Client

Connect using Beeline:

```bash
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000
```

### 5. Testing Your Setup

After connecting to Hive, you can run these test queries:

```sql
SHOW DATABASES;
USE default;
CREATE TABLE test (id INT, name STRING);
SHOW TABLES;
INSERT INTO test VALUES (1, 'zhangsan'), (2, 'lisi');
SELECT * FROM test;
SELECT COUNT(*) FROM test;
```

### 6. Accessing HDFS Web UI

You can view HDFS files at: [http://localhost:9870](http://localhost:9870)

The default Hive warehouse directory is located at `/user/hive/warehouse`.

### 7. Using HiveServer2 with Beeline (Alternative)

To use HiveServer2 with Beeline:

1. Connect with Beeline:
   ```bash
   docker exec -it hive-server beeline
   ```
   
   Then in Beeline:
   ```sql
   !connect jdbc:hive2://localhost:10000
   # Use any username, no password required
   ```

## Stopping the Environment

To stop all services:

```bash
docker-compose down
```

To stop and remove volumes (this will delete all data):

```bash
docker-compose down -v
```

## Troubleshooting

1. If MySQL connection fails, ensure the hive-metastore-db service is running:
   ```bash
   docker ps | grep mysql
   ```

2. Check Hive logs:
   ```bash
   docker logs hive-server
   docker logs hive-metastore
   ```

3. If HDFS is not available, check Hadoop services:
   ```bash
   docker logs hadoop-namenode
   docker logs hadoop-datanode
   ```

4. Verify ports are available:
   - HDFS NameNode UI: 9870
   - HiveServer2: 10000
   - Hive Metastore: 9083

## Project Structure

```
docker_hive/
├── docker-compose.yml          # Docker Compose configuration
├── init.sql                    # MySQL initialization script
├── init-hive.sql               # Hive initialization script
├── README.md                   # This file
└── hive/                       # Hive configuration directory
    ├── Dockerfile              # Custom Dockerfile to add MySQL connector
    ├── hive-site.xml           # Hive configuration
```

## Notes

- We're reusing existing Docker images and extending them where needed
- A custom Dockerfile adds the MySQL connector to the base Hive image
- The first startup may take several minutes as it initializes all services
- The Hive metastore schema initialization is required only for the first time
- All data is persisted in Docker volumes