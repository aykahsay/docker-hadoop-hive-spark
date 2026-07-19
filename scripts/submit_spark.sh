#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: ./submit_spark.sh <path_to_script.py>"
    echo "Example: ./submit_spark.sh scripts/student_predictive.py"
    exit 1
fi

SCRIPT_PATH=$1

# We will pass the exact relative path into the container
# because the whole repository is mounted at /workspace
echo "🚀 Submitting $SCRIPT_PATH to Apache Spark..."
docker exec -it spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    "${@:2}" \
    /workspace/$SCRIPT_PATH
