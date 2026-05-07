param(
  [Parameter(Position = 0)]
  [ValidateSet("repl", "notebook", "lab")]
  [string]$Mode = "repl",

  [string]$Container = "spark-master",
  [int]$Port = 8888
)

$sparkCmd = "/opt/spark/bin/pyspark"
$dockerArgs = @("exec", "-it")

switch ($Mode) {
  "repl" {
    $dockerArgs += @(
      "-e", "PYSPARK_DRIVER_PYTHON=python3"
    )
  }
  "notebook" {
    # Keep notebook mode for simple command memory, route to Lab by default.
    $opts = "lab --ip=0.0.0.0 --port=$Port --no-browser --allow-root --IdentityProvider.token=''"
    $dockerArgs += @(
      "-e", "PYSPARK_DRIVER_PYTHON=jupyter",
      "-e", "PYSPARK_DRIVER_PYTHON_OPTS=$opts"
    )
  }
  "lab" {
    $opts = "lab --ip=0.0.0.0 --port=$Port --no-browser --allow-root --IdentityProvider.token=''"
    $dockerArgs += @(
      "-e", "PYSPARK_DRIVER_PYTHON=jupyter",
      "-e", "PYSPARK_DRIVER_PYTHON_OPTS=$opts"
    )
  }
}

$dockerArgs += @($Container, $sparkCmd)

Write-Host "Mode: $Mode | Container: $Container"
if ($Mode -ne "repl") {
  Write-Host "Open: http://localhost:$Port"
}

docker @dockerArgs
