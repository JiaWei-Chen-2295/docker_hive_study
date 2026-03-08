-- Create the metastore database
CREATE DATABASE IF NOT EXISTS metastore_db;

-- Grant permissions to hive user (MySQL 8.0 compatible)
GRANT ALL PRIVILEGES ON metastore_db.* TO 'hive'@'%';

FLUSH PRIVILEGES;
