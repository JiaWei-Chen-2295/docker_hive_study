#!/bin/bash
set -e

echo "=== Spark Container Starting ==="

# Wait for Hive Metastore to be ready
echo "Waiting for Hive Metastore to be ready..."
while ! nc -z hive-metastore 9083; do
    echo "Hive Metastore is not ready yet. Waiting..."
    sleep 5
done
echo "Hive Metastore is ready!"

# Wait for HDFS NameNode to be ready
echo "Waiting for HDFS NameNode to be ready..."
while ! nc -z hadoop-namenode 8020; do
    echo "HDFS NameNode is not ready yet. Waiting..."
    sleep 5
done
echo "HDFS NameNode is ready!"

export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH

echo "Starting Spark $SPARK_MODE..."

# Start Spark based on mode
if [ "$SPARK_MODE" = "master" ]; then
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
        --host spark-master \
        --port 7077 \
        --webui-port 8080
elif [ "$SPARK_MODE" = "worker" ]; then
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
        --webui-port 8081 \
        $SPARK_MASTER_URL
else
    echo "Unknown SPARK_MODE: $SPARK_MODE"
    exit 1
fi
