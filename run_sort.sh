#!/bin/bash
# 运行 RDD 排序任务：多文件整数排序并输出 rank,value
# 用法: bash run_sort.sh

set -e

echo "============================================"
echo "  RDD 排序任务: rank,value 输出"
echo "============================================"
MSYS_NO_PATHCONV=1 docker exec -e PYTHONUNBUFFERED=1 spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf "spark.ui.showConsoleProgress=false" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.rootCategory=ERROR,console" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.rootCategory=ERROR,console" \
  /opt/spark/jobs/RDD/SortByTwoColumns.py 2>/dev/null | grep -v "^[0-9]\{2\}/[0-9]\{2\}/[0-9]\{2\}"

echo ""
echo "============================================"
echo "  完成"
echo "============================================"
