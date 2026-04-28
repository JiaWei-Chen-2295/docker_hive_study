USE chen_jia_wei;



CREATE TABLE IF NOT EXISTS emp(
                                  empno int, ename string, job string, mgr int,
                                  hiredate string, sal double, comm double, deptno int
);
INSERT INTO emp VALUES
                    (7369, 'SMITH', 'CLERK', 7902, '1980-12-17', 800.0, NULL, 20),
                    (7499, 'ALLEN', 'SALESMAN', 7698, '1981-02-20', 1600.0, 300.0, 30),
                    (7999, 'CHOU', 'SALESMAN', 7698, '1981-02-20', 1600.0, 300.0, 30),
                    (7839, 'KING', 'PRESIDENT', NULL, '1981-11-17', 5000.0, NULL, 10);


-- 3) dept 表 (模拟部门表)
CREATE TABLE IF NOT EXISTS dept(deptno int, dname string);
INSERT INTO dept VALUES (10, 'ACCOUNTING'), (20, 'RESEARCH'), (30, 'SALES');


-- ==============================================================================
-- 1. 基础函数
-- ==============================================================================

-- 1.1 查看函数
SHOW FUNCTIONS;
DESC FUNCTION upper;
DESC FUNCTION EXTENDED upper;

-- 数学函数测试
SELECT round(3.5);
SELECT ceil(3.5);
SELECT floor(3.5);

-- 1.2 空值函数 (NVL)
-- 查看原始 comm 列数据 (包含 NULL)
SELECT comm FROM emp;
-- 用值代替 NULL
SELECT nvl(comm, 0) FROM emp;
-- 用列代替 NULL
SELECT nvl(comm, ename) FROM emp;
-- 嵌套使用: 查询每个员工的sal+comm。如果comm为null找mgr，mgr为null找ename
SELECT nvl(nvl(comm, mgr), ename) FROM emp;

-- 1.3 时间类函数
-- (1) date_format函数
SELECT date_format('2022-03-01','yyyy-MM');
SELECT date_format('2022-03-01 08:08:08','yyyy-MM-dd HH:mm:ss');

-- 思考：时间间隔如果用 / 可否识别？(报错无法识别，通过正则替换后再format)
-- SELECT date_format('2022/01/01', 'yyyy-MM-dd');  -- 这句会报错或返回NULL
SELECT regexp_replace('2022/01/01', '/', '-');
SELECT date_format(regexp_replace('2022/01/01', '/', '-'),'yyyy-MM');

-- (2) 日期加减函数
-- 加法
SELECT date_add('2022-02-10', 1);
SELECT date_add('2022-02-10', -1);
-- 跨年跨月加法
SELECT date_add('2022-12-30', 3);
-- 减法
SELECT date_sub('2022-01-01', 3);

-- (3) datediff 日期相减函数
SELECT datediff('2022-04-05', '2022-04-07');


-- ==============================================================================
-- 2. 条件函数
-- ==============================================================================

-- 2.1 case when 函数
-- 统计各部门人数
SELECT
    sum(case deptno when 10 then 1 else 0 end) as 10_cnt,
    sum(case deptno when 20 then 1 else 0 end) as 20_cnt,
    sum(case deptno when 30 then 1 else 0 end) as 30_cnt
FROM emp;

-- 2.2 if 函数 (创建 stu_score 表并测试)
CREATE TABLE IF NOT EXISTS stu_score(
                                        name string,
                                        score double,
                                        class string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- LOAD DATA LOCAL INPATH '/opt/module/data/stu_score.txt' INTO TABLE stu_score;
-- 插入测试数据
TRUNCATE TABLE stu_score;
INSERT INTO stu_score VALUES
                          ('zhangsan', 99.5, 'bigdata'),
                          ('lisi', 90.5, 'software'),
                          ('wangwu', 60.5, 'bigdata'),
                          ('zhaoliu', 50.0, 'bigdata'),
                          ('zhuqi', 55.0, 'software');

-- 解法一：使用 case when
SELECT class,
       sum(case when score >= 90 then 1 else 0 end) as excellent,
       sum(case when score < 90 and score >= 60 then 1 else 0 end) as pass,
       sum(case when score < 60 then 1 else 0 end) as fail
FROM stu_score
GROUP BY class;

-- 解法二：使用 IF 函数
SELECT class,
       sum(if(score >= 90, 1, 0)) as excellent,
       sum(if(score < 90 and score >= 60, 1, 0)) as pass,
       sum(if(score < 60, 1, 0)) as fail
FROM stu_score
GROUP BY class;



-- ==============================================================================
-- 3. 字符串函数
-- ==============================================================================

-- 3.1 STR_TO_MAP 函数
SELECT str_to_map('1001=2021-03-10, 1002=2021-03-11', ',', '=');

-- 3.2 CONCAT 函数
SELECT concat('hello', '-', 'world');
SELECT concat(CAST(empno AS string), '-', ename) FROM emp;
SELECT concat(CAST(empno AS string), '-', ename, '-', CAST(nvl(mgr, 0) AS string)) FROM emp;

-- 3.3 CONCAT_WS 函数
-- 注意：concat_ws 要求参数必须是 string 类型，如果是数字需转换 (Hive较高版本支持隐式转换)
SELECT concat_ws('-', CAST(empno AS string), ename, CAST(mgr AS string)) FROM emp;
SELECT concat_ws('-', job, ename, hiredate) FROM emp;

-- 3.4 Length 函数
SELECT length(dname) FROM dept;

-- 3.5 字符串截取替换函数
-- (1) substr, substring
SELECT substr('abcde', 3, 2);
SELECT substring('abcde', 3, 2);
SELECT substring('abcde', -2, 2);

-- (2) split (返回数组)
SELECT split('abtcdtef', 't');

-- (3) replace
SELECT replace('bigdata', 'b', 'B');

-- (4) regexp_replace
SELECT regexp_replace('2022-01-01', '-', '/');
SELECT regexp_replace('abc123de', '[0-9]', '*');

-- 3.6 其他常用字符串函数
-- (1) reverse 反转
SELECT reverse('abcedfg');

-- (2) upper / lower 大小写转换
SELECT upper('abSEd');
SELECT ucase('abSEd');
SELECT lower('ABSED');

-- (3) trim 去空格
SELECT trim(' abc ');




-- ==============================================================================
-- 4. 行列转换函数
-- ==============================================================================

-- 4.1 collect_set(col)
SELECT collect_set(deptno) FROM dept;
SELECT concat_ws('-', collect_set(dname)) FROM dept;

-- ------------------------------------------------------------------------------
-- 准备 emp_gender 表
-- 本地文件路径: /opt/module/data/emp_gender.txt
-- 数据内容 (Tab分隔):
-- zhangsan	A	male
-- lisi	A	male
-- wangwu	B	male
-- zhaoliu	A	female
-- zhuqi	B	female
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS emp_gender (
                                          name string,
                                          dept string,
                                          gender string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/data/03-sql/emp_gender.txt' INTO TABLE emp_gender;

-- 行转列查询步骤一
SELECT concat(dept, ',', gender) as dept_g, name FROM emp_gender;

-- 行转列查询步骤二
SELECT dept_g,
       concat_ws('|', collect_set(name))
FROM (
         SELECT concat(dept, ',', gender) as dept_g, name FROM emp_gender
     ) t1
GROUP BY dept_g;


-- ------------------------------------------------------------------------------
-- 4.2 EXPLODE(col) 与 LATERAL VIEW
-- 准备 movielist 表
-- 本地文件路径: /opt/module/data/movie_list.txt
-- 数据内容 (Tab分隔电影名和类型，逗号分隔类型数组):
-- StarWars	science_fiction_film,war_film,action
-- Mulan	action,adventure
-- Frozen	animation,comedy,adventure
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS movielist(
                                        movie string,
                                        category array<string>
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        COLLECTION ITEMS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/data/03-sql/movie_list.txt' INTO TABLE movielist;
SELECT * FROM movielist;

-- 直接 Explode
SELECT explode(category) FROM movielist;

-- 侧写函数 LATERAL VIEW (列转行)
SELECT movie, category_name
FROM movielist
         LATERAL VIEW explode(category) table_tmp AS category_name;

