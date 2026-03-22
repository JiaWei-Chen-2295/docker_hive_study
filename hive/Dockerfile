FROM apache/hive:4.0.0

USER root

# Install curl for downloading the MySQL connector
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# Download MySQL Connector/J 8.0.33 (artifact renamed from mysql-connector-java to mysql-connector-j)
RUN curl -fSL https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar \
    -o /opt/hive/lib/mysql-connector-j-8.0.33.jar && \
    chown hive:hive /opt/hive/lib/mysql-connector-j-8.0.33.jar

# Copy custom entrypoint to clean PID files before start
COPY entrypoint.sh /custom-entrypoint.sh
RUN chmod +x /custom-entrypoint.sh

USER hive

ENTRYPOINT ["/custom-entrypoint.sh"]
