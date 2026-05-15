-- 题 2：统计 2021 年第四季度的视频类型，按每类平均互动率降序
-- 互动率 = (点赞 + 评论 + 分享) / 播放量
-- 注意：category 需要按 "_" 拆分成多个类型
--
-- 结构：hw_q2_video_q4 → hw_q2_category_eng_rows → 最终聚合
-- 如需保留中间视图调试，可删掉文件末尾两行 DROP

USE chen_jia_wei;

DROP VIEW IF EXISTS hw_q2_category_eng_rows;
DROP VIEW IF EXISTS hw_q2_video_q4;

CREATE VIEW hw_q2_video_q4 AS
SELECT
    vv,
    COALESCE(likes, 0)    AS likes,
    COALESCE(comments, 0) AS comments,
    COALESCE(shares, 0)   AS shares,
    category
FROM video
WHERE CAST(regexp_replace(video_date, '/', '-') AS DATE) >= DATE '2021-10-01'
  AND CAST(regexp_replace(video_date, '/', '-') AS DATE) <  DATE '2022-01-01';

CREATE VIEW hw_q2_category_eng_rows AS
SELECT
    category_tag,
    (likes + comments + shares) / CAST(vv AS DOUBLE) AS engagement_rate
FROM hw_q2_video_q4
LATERAL VIEW explode(split(category, '_')) e AS category_tag;

SELECT
    category_tag AS category,
    CONCAT(CAST(ROUND(AVG(engagement_rate) * 100, 2) AS STRING), '%') AS engagement_rate
FROM hw_q2_category_eng_rows
GROUP BY category_tag
ORDER BY AVG(engagement_rate) DESC, category_tag;

DROP VIEW IF EXISTS hw_q2_category_eng_rows;
DROP VIEW IF EXISTS hw_q2_video_q4;
