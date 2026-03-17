FROM apache/spark:3.5.1

USER root

# Install necessary tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Download MySQL Connector for Hive Metastore connection
RUN curl -fSL https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar \
    -o /opt/spark/jars/mysql-connector-j-8.0.33.jar

# Copy configuration files
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY hive-site.xml /opt/spark/conf/hive-site.xml
COPY entrypoint.sh /opt/entrypoint.sh

RUN chmod +x /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]
