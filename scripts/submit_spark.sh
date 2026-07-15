#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: ./submit_spark.sh <path_to_script.py>"
    echo "Example: ./submit_spark.sh scripts/student_predictive.py"
    exit 1
fi

SCRIPT_PATH=$1

# We need to extract just the filename if a full path is passed, 
# because the script folder is mounted inside the container at /scripts
FILENAME=$(basename -- "$SCRIPT_PATH")

echo "🚀 Submitting $FILENAME to Apache Spark..."
docker exec -it spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /scripts/$FILENAME
