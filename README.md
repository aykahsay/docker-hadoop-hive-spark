# Docker Hadoop, Spark, and Hive Ecosystem

This is a complete, unified Docker multi-container environment with Hadoop (HDFS), Spark, and Hive. It is designed specifically for Big Data Analytics assignments, combining both **Descriptive Analytics (Hive)** and **Predictive Analytics (Spark)** into a single, cohesive workflow without the massive memory requirements of a full Cloudera sandbox.

## Quick Start

To deploy the HDFS-Spark-Hive cluster, simply run the included start script, or use Docker Compose directly:

```bash
./scripts/start_cluster.sh
# OR
docker-compose up -d
```

`docker-compose` creates a virtual network for the containers to communicate. Once initialized, you can access the web interfaces via your local browser:

* **Namenode:** [http://localhost:9870](http://localhost:9870)
* **Spark master:** [http://localhost:8080](http://localhost:8080)
* **Spark worker:** [http://localhost:8081](http://localhost:8081)
* **Hive Server:** `localhost:10000` (JDBC)

---

## 1. Quick Start HDFS (Dependencies & Requirements)

Before doing any analysis, your data must be loaded into the Hadoop Distributed File System (HDFS). 

1. **Copy your local dataset to the namenode container:**
```bash
docker cp data/student_performance.csv namenode:/student_performance.csv
```

2. **Open a bash shell inside the namenode:**
```bash
docker exec -it namenode bash
```

3. **Create the HDFS directories for your data and Hive:**
```bash
hdfs dfs -mkdir -p /user/hive/data
```

4. **Put the dataset into HDFS:**
```bash
hdfs dfs -put /student_performance.csv /user/hive/data/student_performance.csv
```

---

## 2. Quick Start Spark (Predictive Analysis)

You can check the status of the Spark Master at [http://localhost:8080](http://localhost:8080).

To run PySpark interactively and read the HDFS data:

1. **Go to the command line of the Spark master and start PySpark:**
```bash
docker exec -it spark-master bash
/spark/bin/pyspark --master spark://spark-master:7077
```

2. **Load your dataset from HDFS inside PySpark:**
```python
df = spark.read.csv("hdfs://namenode:9000/user/hive/data/student_performance.csv", header=True, inferSchema=True)
df.show(5)
```

**Automated Submission:** 
If you have written a complete machine learning script (like `scripts/student_predictive.py`), you can submit it directly without opening bash:
```bash
./scripts/submit_spark.sh scripts/student_predictive.py
```

---

## 3. Quick Start Hive (Descriptive Analysis)

Hive is used for querying your massive datasets using SQL-like syntax. The Hive Server runs on port `10000`.

1. **Connect to Hive Server using Beeline:**
```bash
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -n root
```

2. **Create a database and your External Table pointing to the HDFS directory:**
```sql
CREATE DATABASE student_analytics;
USE student_analytics;

CREATE EXTERNAL TABLE IF NOT EXISTS student_performance(
    Student_ID INT,
    Math_Score INT,
    Reading_Score INT,
    Placement_Status STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/data';
```

3. **Run your analytical queries:**
```sql
SELECT Placement_Status, AVG(Math_Score) as Avg_Math 
FROM student_performance 
GROUP BY Placement_Status;
```

---

## Configure Environment Variables

The cluster relies on `docker-compose.yml` environment variables to wire the services together. 

For example, Spark and Datanodes must know where the Hadoop File System is located. This is defined in the `docker-compose.yml` under each service:
```yaml
environment:
  - CORE_CONF_fs_defaultFS=hdfs://namenode:9000
```
This is automatically injected into `/etc/hadoop/core-site.xml` at runtime.

Additionally, the `hive-server` depends on the `postgres` Metastore being fully ready before starting. This requirement is handled via the `SERVICE_PRECONDITION` variable:
```yaml
environment:
  - SERVICE_PRECONDITION=namenode:9870 postgres:5432
```

## Stopping the Cluster
To cleanly shut down the cluster and preserve your database/HDFS data:
```bash
docker-compose stop
```
To destroy the cluster and wipe all HDFS data:
```bash
docker-compose down -v
```
