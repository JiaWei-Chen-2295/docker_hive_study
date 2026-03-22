CREATE DATABASE IF NOT EXISTS chen_jia_wei;
use chen_jia_wei;
CREATE TABLE IF NOT EXISTS test(id int);


-- 指定 HDFS 存放的位置
create database if not exists bigdata location '/bigdata';

show databases;

-- 修改描述数据库的属性信息
desc database bigdata;
desc database extended bigdata;
alter database bigdata set dbproperties ('name'='jc');
desc database extended bigdata;

drop database if exists bigdata ;


-- CRUD
use chen_jia_wei;
create table if not exists student(id int, name string)
--     通过 tab 分隔数据
    row format delimited fields terminated by '\t'
    stored as textfile
    location '/user/hive/warehouse/student';
-- 导入数据
-- 因为IDE会将 Tab 直接替换为设置的空格 所以会出现没有结果的情况
-- 清空表
-- truncate table chen_jia_wei.student;
load data local inpath '/data/03-sql/stu.txt' into table student;
select * from chen_jia_wei.student;


-- 根据查询结果建表
create table if not exists student_select as select * from chen_jia_wei.student;
-- 有数据
select * from student_select;
show create table student_select;
-- 无数据 只是复制了表结构
create table if not exists student_like like student;
select * from student_like;


/**
  内部表和外部表
  managed table (内部表) Hive 控制生命周期
  external table (外部表) Hive 不控制生命周期
 */

 create external table if not exists student_external(id int, name string)
    row format delimited fields terminated by '\t'
    stored as textfile
    location '/user/hive/warehouse/student_external';

load data local inpath '/data/03-sql/stu.txt' into table student_external;
select * from student_external;
-- 现在文件还在
drop table if exists student_external;
-- 重新创建 数据还在
create external table if not exists student_external(id int, name string)
    row format delimited fields terminated by '\t'
    stored as textfile
    location '/user/hive/warehouse/student_external';
select * from student_external;
-- 转换为内部表
desc formatted student_external;
alter table student_external set tblproperties ('EXTERNAL'='false');

/**
  分区表
  在 Hive 就是分表
  在 HDFS 中表现为分目录
 */
-- 创建一个分区表
create table student_partition(id int, name string)
partitioned by (month string)
row format delimited fields terminated by '\t';
load data local inpath '/data/03-sql/stu.txt' into table student_partition
--     此处不指定分区 会报错
    partition (month='2023-09');
select * from student_partition;

-- 导入另一个分区的数据
load data local inpath '/data/03-sql/stu.txt' into table student_partition
partition (month='2023-10');
select * from student_partition where month='2023-10'
UNION
select * from student_partition where month='2023-09';

-- 增加单个分区
alter table student_partition add partition (month='2023-11');

-- 增加多个分区
alter table student_partition add partition (month='2023-12') partition (month='2024-01');

-- 此时 直接上传一个空文件夹 叫 month=2024-02
-- 不会产生分区
select * from student_partition;
show partitions student_partition;

-- 方法1： 恢复分区
set hive.msck.path.validation = ignore;
msck repair table student_partition;
show partitions student_partition;
select * from student_partition;

-- 删除单个分区
alter table student_partition drop partition (month='2024-02');
