#!/bin/bash

# Download and install MySQL connector as root
apt-get update && apt-get install -y curl netcat

echo "Installing MySQL connector..."
mkdir -p /opt/hive/lib
curl -L https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.33/mysql-connector-java-8.0.33.jar \
    -o /tmp/mysql-connector-java-8.0.33.jar
cp /tmp/mysql-connector-java-8.0.33.jar /opt/hive/lib/

# Wait for MySQL to be ready
echo "Waiting for MySQL to be ready..."
until nc -z mysql 3306
do
    sleep 1
done
echo "MySQL is ready!"

# Initialize the schema with MySQL using hive user
echo "Initializing Hive schema with MySQL..."
cd /opt/hive
export HIVE_CONF_DIR=/opt/hive/conf
export CLASSPATH=/opt/hive/lib/*:./*

# Run schematool directly since we're already running as hive user in the container
/opt/hive/bin/schematool -dbType mysql -initSchema

# Start the original entrypoint with the passed arguments
exec /original_entrypoint.sh "$@"