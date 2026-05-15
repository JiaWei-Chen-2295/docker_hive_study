-- 题 3：提取文案中提到 food 的视频，建入新表 foodvideo
-- 字段：视频 ID、作者 ID、播放量、视频文案、作者粉丝量、发布日期
--

SET hive.exec.mode.local.auto=false;
SET hive.auto.convert.join=false;

USE chen_jia_wei;

DROP TABLE IF EXISTS foodvideo;

CREATE TABLE foodvideo (
    video_id      STRING,
    user_id       STRING,
    vv            BIGINT,
    video_caption STRING,
    follower      BIGINT,
    video_date    STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE foodvideo
SELECT
    v.video_id,
    v.user_id,
    v.vv,
    v.video_caption,
    c.follower,
    regexp_replace(v.video_date, '/', '-') AS video_date
FROM video v
JOIN creator c
    ON v.user_id = c.creator_id
WHERE lower(COALESCE(v.video_caption, '')) LIKE '%food%';
