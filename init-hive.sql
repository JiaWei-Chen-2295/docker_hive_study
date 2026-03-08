-- Initialize Hive Metastore
-- This script will be run after Hive is started

SHOW DATABASES;

USE default;

-- Create a sample table for testing
CREATE TABLE IF NOT EXISTS test (
    id INT,
    name STRING
);

SHOW TABLES;

-- Insert sample data
INSERT INTO test VALUES (1, 'zhangsan');
INSERT INTO test VALUES (2, 'lisi');

SELECT * FROM test;

SELECT COUNT(*) FROM test;