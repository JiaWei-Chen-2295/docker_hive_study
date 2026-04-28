#!/usr/bin/env python3
"""Submit a Python Spark job to the spark-master container.

Examples:
  python tools/submit_spark_job.py jobs/WordCount.py
  python tools/submit_spark_job.py jobs/RDD/TopPayment.py --spark-args "--deploy-mode client" --job-args "10"
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from pathlib import Path

DEFAULT_SUBMIT_ARGS = [
    "--master",
    "spark://spark-master:7077",
    "--conf",
    "spark.ui.showConsoleProgress=false",
    "--conf",
    "spark.driver.extraJavaOptions=-Dlog4j.rootCategory=ERROR,console",
    "--conf",
    "spark.executor.extraJavaOptions=-Dlog4j.rootCategory=ERROR,console",
]


def _split_args(raw: str) -> list[str]:
    if not raw.strip():
        return []
    # Windows users often type args in CMD/PowerShell style, so keep platform parsing.
    return shlex.split(raw, posix=(os.name != "nt"))


def _resolve_paths(script_input: str) -> tuple[Path, str]:
    workspace_root = Path(__file__).resolve().parents[1]
    jobs_root = workspace_root / "jobs"
    script_path = Path(script_input)

    if not script_path.is_absolute():
        script_path = (workspace_root / script_path).resolve()

    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    if not script_path.is_file() or script_path.suffix.lower() != ".py":
        raise ValueError(f"Script must be a .py file: {script_path}")

    try:
        rel_to_jobs = script_path.relative_to(jobs_root)
    except ValueError as exc:
        raise ValueError(
            f"Script must be inside jobs directory: {jobs_root}"
        ) from exc

    container_script = f"/opt/spark/jobs/{rel_to_jobs.as_posix()}"
    return script_path, container_script


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Submit a Python Spark job through docker exec spark-master"
    )
    parser.add_argument("script", help="Path to the .py file under jobs/")
    parser.add_argument(
        "--spark-args",
        default="",
        help="Extra spark-submit args as a single string",
    )
    parser.add_argument(
        "--job-args",
        default="",
        help="Arguments passed to your Python job as a single string",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        host_script, container_script = _resolve_paths(args.script)
    except (FileNotFoundError, ValueError) as err:
        print(f"Error: {err}", file=sys.stderr)
        return 2

    spark_args = _split_args(args.spark_args)
    job_args = _split_args(args.job_args)

    cmd = [
        "docker",
        "exec",
        "-e",
        "PYTHONUNBUFFERED=1",
        "spark-master",
        "/opt/spark/bin/spark-submit",
        *DEFAULT_SUBMIT_ARGS,
        *spark_args,
        container_script,
        *job_args,
    ]

    print("=" * 44)
    print("Spark Python Job Submit")
    print("=" * 44)
    print(f"Host script    : {host_script}")
    print(f"Container script: {container_script}")
    print("Running command:")
    print(" ".join(shlex.quote(x) for x in cmd))
    print("-" * 44)

    completed = subprocess.run(cmd)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
