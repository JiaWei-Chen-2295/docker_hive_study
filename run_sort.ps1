$ErrorActionPreference = "Stop"

Write-Host "============================================"
Write-Host "  RDD 排序任务: rank,value 输出"
Write-Host "============================================"

$cmd = @(
  "exec",
  "-e", "PYTHONUNBUFFERED=1",
  "spark-master",
  "/opt/spark/bin/spark-submit",
  "--master", "spark://spark-master:7077",
  "--conf", "spark.ui.showConsoleProgress=false",
  "--conf", "spark.driver.extraJavaOptions=-Dlog4j.rootCategory=ERROR,console",
  "--conf", "spark.executor.extraJavaOptions=-Dlog4j.rootCategory=ERROR,console",
  "/opt/spark/jobs/RDD/SortByTwoColumns.py"
)

& docker @cmd

Write-Host ""
Write-Host "============================================"
Write-Host "  完成"
Write-Host "============================================"
