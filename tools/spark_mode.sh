#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-repl}"
CONTAINER="${CONTAINER:-spark-master}"
PORT="${PORT:-8888}"
SPARK_CMD="/opt/spark/bin/pyspark"

usage() {
  echo "Usage: ./tools/spark_mode.sh [repl|notebook|lab]"
  echo "Optional env: CONTAINER=spark-master PORT=8888"
}

if [[ "$MODE" != "repl" && "$MODE" != "notebook" && "$MODE" != "lab" ]]; then
  usage
  exit 1
fi

DOCKER_ARGS=(exec -it)

case "$MODE" in
  repl)
    DOCKER_ARGS+=(-e "PYSPARK_DRIVER_PYTHON=python3")
    ;;
  notebook)
    # This image installs jupyterlab by default.
    # Keep "notebook" command for UX, but route to Lab for compatibility.
    OPTS="lab --ip=0.0.0.0 --port=${PORT} --no-browser --allow-root --IdentityProvider.token=''"
    DOCKER_ARGS+=(
      -e "PYSPARK_DRIVER_PYTHON=jupyter"
      -e "PYSPARK_DRIVER_PYTHON_OPTS=${OPTS}"
    )
    ;;
  lab)
    OPTS="lab --ip=0.0.0.0 --port=${PORT} --no-browser --allow-root --IdentityProvider.token=''"
    DOCKER_ARGS+=(
      -e "PYSPARK_DRIVER_PYTHON=jupyter"
      -e "PYSPARK_DRIVER_PYTHON_OPTS=${OPTS}"
    )
    ;;
esac

echo "Mode: ${MODE} | Container: ${CONTAINER}"
if [[ "$MODE" != "repl" ]]; then
  echo "Open: http://localhost:${PORT}"
fi

# Git Bash (MSYS) may rewrite /opt/... into a Windows path.
# Disable path conversion so docker exec receives Linux container paths unchanged.
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker "${DOCKER_ARGS[@]}" "$CONTAINER" "$SPARK_CMD"
