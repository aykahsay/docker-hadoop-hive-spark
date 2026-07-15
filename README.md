# Unified Big Data Workspace (Hadoop + Hive + Spark)

This repository contains a complete, Dockerized Big Data ecosystem designed for the **Big Data Analytics Group Assignment**. It combines Apache Hadoop, Apache Hive, and Apache Spark into a single, cohesive environment, allowing you to perform both Descriptive and Predictive Analytics entirely from your local machine.

## 🌟 Features
* **Hadoop (HDFS & YARN):** Distributed storage and resource management.
* **Apache Hive:** Data warehouse infrastructure for Descriptive Analytics using SQL-like queries.
* **PostgreSQL:** Serves as the robust backend database for the Hive Metastore.
* **Apache Spark:** In-memory data processing engine for Predictive Analytics and Machine Learning.

## 🚀 Quick Start

### 1. Start the Cluster
Open your terminal (WSL or Bash) in this repository and run:
```bash
./scripts/start_cluster.sh
```
This script will boot up all 6 Docker containers and wait for them to initialize.

### 2. Access Web UIs
Once running, you can monitor your cluster via your web browser:
* **HDFS NameNode:** [http://localhost:9870](http://localhost:9870)
* **Spark Master:** [http://localhost:8080](http://localhost:8080)
* **Spark Worker:** [http://localhost:8081](http://localhost:8081)

## 📊 Descriptive Analysis (Apache Hive)
Hive is used to run MapReduce queries on your data. 

To open the Hive SQL prompt (Beeline):
```bash
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000
```
*Note: Make sure you have uploaded your datasets into HDFS before querying them!*

## ⚡ Predictive Analysis (Apache Spark)
Spark is used to train Machine Learning models on your data.

We have included a helper script to easily submit PySpark scripts to the cluster:
```bash
./scripts/submit_spark.sh scripts/student_predictive.py
```
This will automatically execute the Python script on the `spark-master` container, reading data directly from HDFS.

## 🛑 Stopping the Cluster
To gracefully shut down the cluster and preserve your data:
```bash
docker-compose stop
```
If you want to completely destroy the cluster (this will delete your HDFS data!):
```bash
docker-compose down -v
```
