-- 题 5：统计头部用户（粉丝前 20%）的视频数量与质量
-- 输出：作者 ID、视频 ID、发布时间、该作者视频的平均点赞数
--
-- 结构：hw_q5_creator_ntile（按粉丝数分桶，NTILE(5) 取第 1 桶 ≈ 前 20%）
--       → hw_q5_top_creator_ids（只保留头部作者）
--       → hw_q5_video_avg_likes（每个作者维度下的窗口平均点赞）
--       → 最终 JOIN 过滤

USE chen_jia_wei;

DROP VIEW IF EXISTS hw_q5_video_avg_likes;
DROP VIEW IF EXISTS hw_q5_top_creator_ids;
DROP VIEW IF EXISTS hw_q5_creator_ntile;

CREATE VIEW hw_q5_creator_ntile AS
SELECT
    creator_id,
    NTILE(5) OVER (ORDER BY follower DESC) AS follower_bucket
FROM creator;

CREATE VIEW hw_q5_top_creator_ids AS
SELECT creator_id
FROM hw_q5_creator_ntile
WHERE follower_bucket = 1;

CREATE VIEW hw_q5_video_avg_likes AS
SELECT
    v.user_id,
    v.video_id,
    CAST(regexp_replace(v.video_date, '/', '-') AS DATE) AS video_date,
    AVG(v.likes) OVER (PARTITION BY v.user_id) AS avg_likes
FROM video v;

SELECT
    vw.video_id,
    vw.user_id,
    vw.video_date,
    ROUND(vw.avg_likes, 2) AS avg_likes
FROM hw_q5_video_avg_likes vw
JOIN hw_q5_top_creator_ids tc
    ON vw.user_id = tc.creator_id
ORDER BY vw.user_id, vw.video_date, vw.video_id;

DROP VIEW IF EXISTS hw_q5_video_avg_likes;
DROP VIEW IF EXISTS hw_q5_top_creator_ids;
DROP VIEW IF EXISTS hw_q5_creator_ntile;
