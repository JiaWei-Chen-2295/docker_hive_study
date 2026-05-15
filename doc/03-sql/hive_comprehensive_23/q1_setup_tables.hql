-- 题 1：创建数据库（请按需改名，例如学号后四位）
CREATE DATABASE IF NOT EXISTS chen_jia_wei;
USE chen_jia_wei;

DROP TABLE IF EXISTS video;
CREATE TABLE IF NOT EXISTS video (
    video_date     STRING,
    video_id       STRING,
    user_id        STRING,
    video_caption  STRING,
    vv             BIGINT,
    likes          BIGINT,
    shares         BIGINT,
    comments       BIGINT,
    category       STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

DROP TABLE IF EXISTS creator;
CREATE TABLE IF NOT EXISTS creator (
    creator_id STRING,
    follower   BIGINT,
    followed   BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 宿主机 ./doc 挂载到 hive-server 容器内 /data（见 README）
-- 数据文件与本脚本同目录：video.txt、creator.txt

LOAD DATA LOCAL INPATH '/data/03-sql/hive_comprehensive_23/video.txt' OVERWRITE INTO TABLE video;

LOAD DATA LOCAL INPATH '/data/03-sql/hive_comprehensive_23/creator.txt' OVERWRITE INTO TABLE creator;
