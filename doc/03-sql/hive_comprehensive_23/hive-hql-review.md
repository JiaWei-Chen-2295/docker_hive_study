# Hive HQL 复习要点（与本目录作业配套）

## 1. `OVER`：窗口函数

- **作用**：指定「在什么范围内、按什么顺序」计算，**一般不把明细行合并掉**。
- **常见括号内容**
  - **`PARTITION BY 列`**：按列切块，只在同一块里互相算（例如每个 `user_id` 一块）。
  - **`ORDER BY 列`**：块内的先后顺序（`LAG`、`NTILE`、`ROW_NUMBER` 等需要）。
- **记忆**：`函数(...) OVER (PARTITION BY ... ORDER BY ...)` = 在这个窗口里算这个函数。

## 2. 窗口聚合 vs `GROUP BY`

| | `GROUP BY` | `AVG(...) OVER (PARTITION BY ...)` |
|---|------------|-------------------------------------|
| 行数 | 每组通常 **一行** | **每行仍在**，多一列窗口结果 |
| 用途 | 汇总统计 | 既要明细又要「组内指标」（如每人均值贴在每条视频上） |

**例**：同一作者 3 条视频，`AVG(likes) OVER (PARTITION BY user_id)` 会给每一行都填上该作者的平均点赞。

## 3. `LAG`（上一行）

- **`LAG(col) OVER (PARTITION BY user ORDER BY date)`**：在每个用户内部按日期排队后，取**当前行上一行**的 `col`。
- **第一行**没有上一行 → `LAG` 为 **`NULL`**。筛活跃用户时常配合 `prev IS NOT NULL`。

## 4. `NTILE(k)` 与「前 20%」

- **`NTILE(5) OVER (ORDER BY follower DESC)`**：全员按粉丝从高到低排序，**尽量均匀分成 5 桶**。
- **桶 1** ≈ 最高的那一段，作业里用来近似 **「前 20%」**（每桶约 20% 人数；总人数不能被 5 整除时，前几桶可能多 1 人，比例略有偏差）。
- **口诀**：桶号越小，排名越靠前。
- **手算**：10 人、`NTILE(5)` → 每桶 2 人，桶 1 = 第 1～2 名；11 人、`NTILE(5)` → 常为 3,2,2,2,2，桶 1 = 第 1～3 名。

## 5. `explode` + `LATERAL VIEW`（拆 `category`）

- **`split(category, '_')`**：把 `book_movie` 变成数组 `[book, movie]`。
- **`LATERAL VIEW explode(...) AS tag`**：**一行变多行**，每个标签一行；Hive 不允许在同一 `SELECT` 里把 `explode` 与普通列混写，需配合 **`LATERAL VIEW`**。

## 6. 本目录脚本在干什么（一眼）

| 文件 | 核心 |
|------|------|
| `q1_*` | 建库表 + `LOAD DATA LOCAL INPATH` 装 CSV |
| `q2_*` | Q4 视频 → 拆类别 → 按类平均互动率 |
| `q3_*` | `JOIN` 作者表 + 文案含 `food` → 写入 `foodvideo` |
| `q4_*` | `LAG` 找上次发布时间，`DATEDIFF ≤ 14` |
| `q5_*` | `NTILE(5)` 取头部作者 + `AVG OVER` 人均点赞贴在每条视频上 |

---

*可与课堂笔记对照；具体语法以课程与 Hive 版本为准。*
