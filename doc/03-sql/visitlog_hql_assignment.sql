USE chen_jia_wei;

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

SELECT * FROM visitlog ORDER BY userid, visitdate, shop;



-- 数据预处理成为 View
CREATE VIEW v_visitlog AS
SELECT
    userid,
    shop,
    CAST(regexp_replace(visitdate, '/', '-') AS DATE) AS visitdate
FROM visitlog;

-- 需求1
SELECT
    userid,
    visitdate,
    COUNT(*)                                                     AS day_visit_cnt,
    SUM(COUNT(*)) OVER (PARTITION BY userid ORDER BY visitdate)  AS accum_visit_cnt
FROM v_visitlog
GROUP BY userid, visitdate
ORDER BY userid, visitdate;

-- 需求2
SELECT DISTINCT userid
FROM (
         SELECT
             userid,
             DATEDIFF(
                     visitdate,
                     LAG(visitdate) OVER (PARTITION BY userid ORDER BY visitdate)
             ) AS gap_days
         FROM v_visitlog
         WHERE shop = 'b'
     ) t
WHERE gap_days IS NOT NULL          -- 排除每个用户的第一次（无前驱）
  AND gap_days <= 10
ORDER BY userid;

-- 需求3
-- 每个店铺访问次数前两名的访客信息，输出店铺名称、访客id、访问次数；
CREATE VIEW shop_user_cnt_view AS
SELECT shop, userid, COUNT(*) AS visit_cnt
FROM v_visitlog
GROUP BY shop, userid;


SELECT
    shop, userid, visit_cnt,
    DENSE_RANK() OVER (PARTITION BY shop ORDER BY visit_cnt DESC) AS rk
FROM shop_user_cnt_view;


SELECT shop, userid, visit_cnt
FROM (
         SELECT
             shop, userid, visit_cnt,
             DENSE_RANK() OVER (PARTITION BY shop ORDER BY visit_cnt DESC) AS rk
         FROM shop_user_cnt_view
     ) t
WHERE rk <= 2
ORDER BY shop, rk, userid;

-- 需求4
SELECT
    shop,
    COUNT(DISTINCT userid) AS uv
FROM v_visitlog
GROUP BY shop
ORDER BY shop;