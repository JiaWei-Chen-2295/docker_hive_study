-- 已经弃用
USE chen_jia_wei;

-- ============================================================================
-- 电商访问日志 HQL 实战
-- 题目表结构：visitlog(userid, shop, visitdate)
-- ============================================================================

DROP TABLE IF EXISTS visitlog;

CREATE TABLE IF NOT EXISTS visitlog (
    userid    STRING,
    shop      STRING,
    visitdate STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

TRUNCATE TABLE visitlog;
LOAD DATA LOCAL INPATH '/data/03-sql/visitlog.txt' INTO TABLE visitlog;

-- 可选校验
SELECT * FROM visitlog ORDER BY userid, visitdate, shop;

-- ============================================================================
-- 需求(1)
-- 每个用户“有访问记录的日期”当天访问总数 + 截止当天累计访问次数
-- ============================================================================
WITH base_visitlog AS (
    SELECT
        userid,
        shop,
        CAST(regexp_replace(visitdate, '/', '-') AS DATE) AS visitdate
    FROM visitlog
),
user_day_cnt AS (
    SELECT
        userid,
        visitdate,
        COUNT(*) AS day_visit_cnt
    FROM base_visitlog
    GROUP BY userid, visitdate
)
SELECT
    userid,
    visitdate,
    day_visit_cnt,
    SUM(day_visit_cnt) OVER (
        PARTITION BY userid
        ORDER BY visitdate
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS accum_visit_cnt
FROM user_day_cnt
ORDER BY userid, visitdate;

-- ============================================================================
-- 需求(2)
-- b 店铺：两次访问时间差 <= 10 天的用户（同一用户在 b 店铺相邻访问比较）
-- ============================================================================
WITH base_visitlog AS (
    SELECT
        userid,
        shop,
        CAST(regexp_replace(visitdate, '/', '-') AS DATE) AS visitdate
    FROM visitlog
),
b_visits AS (
    SELECT
        userid,
        visitdate,
        LAG(visitdate, 1) OVER (PARTITION BY userid ORDER BY visitdate) AS prev_visitdate
    FROM base_visitlog
    WHERE shop = 'b'
)
SELECT DISTINCT
    userid
FROM b_visits
WHERE prev_visitdate IS NOT NULL
  AND DATEDIFF(visitdate, prev_visitdate) <= 10
ORDER BY userid;

-- ============================================================================
-- 需求(3)
-- 每个店铺访问次数前两名访客：店铺名称、访客id、访问次数
-- ============================================================================
WITH base_visitlog AS (
    SELECT
        userid,
        shop,
        CAST(regexp_replace(visitdate, '/', '-') AS DATE) AS visitdate
    FROM visitlog
),
shop_user_cnt AS (
    SELECT
        shop,
        userid,
        COUNT(*) AS visit_cnt
    FROM base_visitlog
    GROUP BY shop, userid
),
shop_user_ranked AS (
    SELECT
        shop,
        userid,
        visit_cnt,
        ROW_NUMBER() OVER (
            PARTITION BY shop
            ORDER BY visit_cnt DESC, userid
        ) AS rn
    FROM shop_user_cnt
)
SELECT
    shop,
    userid,
    visit_cnt
FROM shop_user_ranked
WHERE rn <= 2
ORDER BY shop, rn;

-- ============================================================================
-- 需求(4)
-- 每个店铺 UV（访客数）
-- 思路优化：
-- 1) 用窗口函数对 (shop, userid) 分组内打标，保留每组第1条作为“唯一访客”；
-- 2) 再按 shop 聚合求和，得到精确 UV；
-- 3) 不使用 count(distinct) 与 ndv，兼容更多 Hive 版本。
-- ============================================================================
WITH base_visitlog AS (
    SELECT
        userid,
        shop,
        CAST(regexp_replace(visitdate, '/', '-') AS DATE) AS visitdate
    FROM visitlog
),
shop_user_flag AS (
    SELECT
        shop,
        userid,
        CASE
            WHEN ROW_NUMBER() OVER (PARTITION BY shop, userid ORDER BY visitdate) = 1 THEN 1
            ELSE 0
        END AS is_first_visit
    FROM base_visitlog
)
SELECT
    shop,
    SUM(is_first_visit) AS uv
FROM shop_user_flag
GROUP BY shop
ORDER BY shop;
