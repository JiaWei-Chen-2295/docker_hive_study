-- ============================================
-- 游戏用户数据分析 Hive SQL 脚本
-- 学号：2023154202  姓名：陈佳玮
-- ============================================

-- 1. 创建数据库 game4202（学号后四位）
CREATE DATABASE IF NOT EXISTS game4202;
USE game4202;

-- 2. 创建外部表（用于加载原始数据）
CREATE EXTERNAL TABLE IF NOT EXISTS game_user_raw (
    user_id STRING,
    p_date STRING,
    gender STRING,
    age INT,
    duration DOUBLE
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
STORED AS TEXTFILE 
LOCATION 'hdfs://hadoop-namenode:8020/user/hive/warehouse/game4202.db/game_user_raw';

-- 3. 创建分区表 game_user（按日期分区）
CREATE TABLE IF NOT EXISTS game_user (
    user_id STRING,
    gender STRING,
    age INT,
    duration DOUBLE
)
PARTITIONED BY (p_date STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
STORED AS TEXTFILE;

-- 4. 从外部表导入数据到分区表
-- 导入20260102数据
INSERT OVERWRITE TABLE game_user PARTITION (p_date='20260102')
SELECT user_id, gender, age, duration 
FROM game_user_raw 
WHERE p_date='20260102';

-- 导入20260209数据
INSERT OVERWRITE TABLE game_user PARTITION (p_date='20260209')
SELECT user_id, gender, age, duration 
FROM game_user_raw 
WHERE p_date='20260209';

-- 导入20260312数据
INSERT OVERWRITE TABLE game_user PARTITION (p_date='20260312')
SELECT user_id, gender, age, duration 
FROM game_user_raw 
WHERE p_date='20260312';

-- ============================================
-- 5. 查询全部数据，按照降序年龄排序
-- ============================================
SELECT * FROM game_user ORDER BY age DESC;

-- ============================================
-- 6. 查询每个用户的平均在线时长
-- ============================================
SELECT user_id, AVG(duration) AS avg_duration 
FROM game_user 
GROUP BY user_id;

-- ============================================
-- 7. 为法务部门创建临时表
--    包含年龄小于15且曾经有当日在线时长大于2小时的用户
-- ============================================

-- 7.1 创建临时表存储高时长用户
DROP TABLE IF EXISTS high_duration_users;
CREATE TABLE high_duration_users AS 
SELECT DISTINCT user_id 
FROM game_user 
WHERE duration > 2;

-- 7.2 创建法务部门用户表
DROP TABLE IF EXISTS legal_dept_users;
CREATE TABLE IF NOT EXISTS legal_dept_users (
    user_id STRING,
    gender STRING,
    age INT
);

-- 7.3 插入符合条件的用户数据
SET hive.auto.convert.join=false;
INSERT INTO legal_dept_users 
SELECT DISTINCT a.user_id, a.gender, a.age 
FROM game_user a 
JOIN high_duration_users b ON a.user_id = b.user_id 
WHERE a.age < 15;

-- 7.4 查看结果
SELECT * FROM legal_dept_users;

-- ============================================
-- 8. 用户画像分析
-- ============================================

-- 8.1 性别比分析
SELECT 
    COUNT(DISTINCT user_id) AS total_users,
    SUM(CASE WHEN gender = 'm' THEN 1 ELSE 0 END) AS male_count,
    SUM(CASE WHEN gender = 'f' THEN 1 ELSE 0 END) AS female_count,
    SUM(CASE WHEN gender = 'm' THEN 1 ELSE 0 END) / SUM(CASE WHEN gender = 'f' THEN 1 ELSE 0 END) AS gender_ratio
FROM (
    SELECT user_id, gender 
    FROM game_user 
    GROUP BY user_id, gender
) t;

-- 8.2 年龄分布分析（15岁以下）
SELECT 
    COUNT(DISTINCT user_id) AS total_users,
    SUM(CASE WHEN age < 15 THEN 1 ELSE 0 END) AS under_15_count,
    SUM(CASE WHEN age >= 15 THEN 1 ELSE 0 END) AS above_15_count
FROM (
    SELECT user_id, age 
    FROM game_user 
    GROUP BY user_id, age
) t;







CREATE EXTERNAL TABLE IF NOT EXISTS city (
                                             user_id STRING,
                                             city STRING
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION 'hdfs://hadoop-namenode:8020/user/hive/warehouse/game4202.db/city';


SET hive.auto.convert.join=false;
SELECT DISTINCT a.user_id, a.gender, a.age, b.city
FROM game_user a
         JOIN city b ON TRIM(a.user_id) = TRIM(b.user_id)
WHERE TRIM(b.city) IN ('beijing', 'shanghai', 'guangzhou')
  AND a.age > 15;







WITH user_duration AS (
    SELECT
        user_id,
        p_date,
        duration,
        LAG(duration) OVER (PARTITION BY user_id ORDER BY p_date) AS prev_duration
    FROM game_user
),
     declining_users AS (
         SELECT DISTINCT user_id
         FROM user_duration
         WHERE prev_duration IS NOT NULL
           AND duration < prev_duration
         GROUP BY user_id
         HAVING COUNT(*) = 2  -- 连续两次都在降低（3个时间点需要2次比较）
     )
SELECT a.user_id, a.gender, a.age
FROM game_user a
         JOIN declining_users b ON a.user_id = b.user_id
WHERE a.age > 15
GROUP BY a.user_id, a.gender, a.age;