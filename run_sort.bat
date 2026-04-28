@echo off
setlocal

echo ============================================
echo   RDD 排序任务: rank,value 输出
echo ============================================

docker exec -e PYTHONUNBUFFERED=1 spark-master /opt/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --conf "spark.ui.showConsoleProgress=false" ^
  --conf "spark.driver.extraJavaOptions=-Dlog4j.rootCategory=ERROR,console" ^
  --conf "spark.executor.extraJavaOptions=-Dlog4j.rootCategory=ERROR,console" ^
  /opt/spark/jobs/RDD/SortByTwoColumns.py

if errorlevel 1 (
  echo.
  echo 任务执行失败，请确认容器已启动并且 spark-master 可用。
  exit /b 1
)

echo.
echo ============================================
echo   完成
echo ============================================
