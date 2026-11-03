#!/bin/bash
set -e

SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"
EVENT_LOG_DIR="${EVENT_LOG_DIR:-/opt/spark/logs/events}"
JOBS_DIR="/opt/spark/jobs"

echo "=============================================="
echo "  SparkLens Benchmark Runner"
echo "=============================================="
echo "Master : $SPARK_MASTER_URL"
echo "Logs   : $EVENT_LOG_DIR"
echo ""

mkdir -p "$EVENT_LOG_DIR"

echo "Waiting for Spark master and worker to be ready (20s)..."
sleep 20

SUBMIT="/opt/spark/bin/spark-submit"
COMMON_CONF=(
    --master "$SPARK_MASTER_URL"
    --conf "spark.eventLog.enabled=true"
    --conf "spark.eventLog.dir=file://$EVENT_LOG_DIR"
    --conf "spark.ui.enabled=true"
    --conf "spark.sql.adaptive.enabled=false"
    --deploy-mode client
)

run_job() {
    local job="$1"
    local name
    name=$(basename "$job" .py)
    echo ""
    echo "----------------------------------------------"
    echo "  Running: $name"
    echo "----------------------------------------------"
    $SUBMIT "${COMMON_CONF[@]}" "$job" \
        && echo "  DONE: $name" \
        || echo "  FAILED: $name (continuing...)"
    sleep 3
}

for job in "$JOBS_DIR"/job_*.py; do
    run_job "$job"
done

echo ""
echo "=============================================="
echo "  All benchmark jobs finished."
echo "  Open the SparkLens dashboard:"
echo "  http://localhost:8501"
echo "=============================================="
