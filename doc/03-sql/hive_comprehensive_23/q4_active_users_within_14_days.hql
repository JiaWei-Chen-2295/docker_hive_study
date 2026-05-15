-- 题 4：统计活跃用户（任意两次发视频间隔 <= 14 天）
-- 输出：user_id、video_id、发布时间、上一次发布时间
--
-- 结构：hw_q4_video_with_dt（统一解析日期）
--       → hw_q4_video_ordered（LAG 取上一次发布时间）
--       → 最终筛选间隔 <= 14 天

USE chen_jia_wei;

DROP VIEW IF EXISTS hw_q4_video_ordered;
DROP VIEW IF EXISTS hw_q4_video_with_dt;

CREATE VIEW hw_q4_video_with_dt AS
SELECT
    user_id,
    video_id,
    CAST(regexp_replace(video_date, '/', '-') AS DATE) AS video_date
FROM video;

CREATE VIEW hw_q4_video_ordered AS
SELECT
    user_id,
    video_id,
    video_date,
    LAG(video_date) OVER (PARTITION BY user_id ORDER BY video_date) AS prev_video_date
FROM hw_q4_video_with_dt;

SELECT
    user_id,
    video_id,
    video_date,
    prev_video_date
FROM hw_q4_video_ordered
WHERE prev_video_date IS NOT NULL
  AND DATEDIFF(video_date, prev_video_date) <= 14
ORDER BY user_id, video_date, video_id;

DROP VIEW IF EXISTS hw_q4_video_ordered;
DROP VIEW IF EXISTS hw_q4_video_with_dt;
