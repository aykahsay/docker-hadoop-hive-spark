#!/bin/bash

echo "🚀 Starting Unified Big Data Cluster (Hadoop + Hive + Spark)..."
echo "------------------------------------------------------------"

# Navigate to the project root directory where docker-compose.yml is located
cd "$(dirname "$0")/.."

# Boot the cluster
docker-compose up -d

echo ""
echo "⏳ Waiting for services to initialize (this can take up to 30 seconds on first boot)..."
echo "Check progress by running: docker-compose logs -f"
echo ""

echo "🔗 SERVICES ARE AVAILABLE AT:"
echo "------------------------------------------------------------"
echo "📁 HDFS NameNode (Web UI): http://localhost:9870"
echo "🐝 Hive Server:           localhost:10000"
echo "⚡ Spark Master (Web UI):  http://localhost:8080"
echo "⚡ Spark Worker (Web UI):  http://localhost:8081"
echo ""
echo "To interact with Hive:"
echo "  docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000"
echo ""
echo "To submit a PySpark job:"
echo "  ./scripts/submit_spark.sh scripts/your_script.py"
echo "------------------------------------------------------------"
