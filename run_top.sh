#!/bin/bash
# 一键运行 TOP 值 和 省份销量前3名 两个作业
# 数据文件位于 jobs/data/ 目录（已挂载到容器 /opt/spark/jobs/data/）
# 用法: bash run_top.sh

set -e

echo "============================================"
echo "  作业1: 求 TOP N payment 值"
echo "============================================"
MSYS_NO_PATHCONV=1 docker exec -e PYTHONUNBUFFERED=1 spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf "spark.ui.showConsoleProgress=false" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.rootCategory=ERROR,console" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.rootCategory=ERROR,console" \
  /opt/spark/jobs/RDD/TopPayment.py 2>/dev/null | grep -v "^[0-9]\{2\}/[0-9]\{2\}/[0-9]\{2\}"

echo ""
echo "============================================"
echo "  作业2: 每个省份销量前3名商品"
echo "============================================"
MSYS_NO_PATHCONV=1 docker exec -e PYTHONUNBUFFERED=1 spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf "spark.ui.showConsoleProgress=false" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.rootCategory=ERROR,console" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.rootCategory=ERROR,console" \
  /opt/spark/jobs/RDD/TopProvinceProduct.py 2>/dev/null | grep -v "^[0-9]\{2\}/[0-9]\{2\}/[0-9]\{2\}"

echo ""
echo "============================================"
echo "  全部完成!"
echo "============================================"
