# Data Preprocessing Using Apache Hive (Step-by-Step Guide)

This repository contains the complete implementation and verified solution for the **Hive Data Preprocessing Group Practical**.

The pipeline ingests raw student score records from an Excel spreadsheet, structures it to match Hive staging schemas, cleans invalid records, standardizes text/gender columns, imputes missing values, and saves the cleaned dataset as an optimized ORC table compressed with Snappy on HDFS.

---

## 📁 Repository Structure

```text
big-data-analytics-group-assignemnt/
│
├── data/
│   └── student_scores.csv       # Clean staging CSV dataset
│
├── scripts/
│   ├── inspect_excel.py         # Python script to extract and clean columns from Excel
│   ├── preprocess.hql           # Step-by-Step Hive SQL preprocessing queries
│   ├── run_preprocess.sh        # Automator to load CSV to HDFS and run HQL via Beeline
│   ├── upload_to_hdfs.sh        # Helper script to load clean CSV into HDFS
│   └── restart_hiveserver2.sh   # Utility script to restart HiveServer2 with safe configs
│
├── project_report.tex           # LaTeX source for the lab report with figure placeholders
└── README.md                    # Setup and execution guide (this file)
```

---

## 🛠️ Step-by-Step Execution Guide

### 1. Ingest Data from Excel
The source Excel file is located at `C:\Users\Admin\.gemini\antigravity-ide\scratch\BigDataProject\big-data-architecture\data\student_score.xlsx`.
Run the Python script in WSL to clean the padding columns/rows, rename variables, and write it to `data/student_scores.csv` matching the Hive table schema order:
```bash
python3 scripts/inspect_excel.py
```

### 2. Configure and Restart HiveServer2
To prevent Hadoop proxyuser impersonation blockages and local Derby metastore database locking issues:
1. Ensure `/opt/hive/conf/hive-site.xml` contains:
   ```xml
   <property>
       <name>hive.server2.enable.doAs</name>
       <value>false</value>
   </property>
   ```
2. Restart HiveServer2 using the automation utility:
   ```bash
   bash scripts/restart_hiveserver2.sh
   ```
   *(This script will stop any locked RunJar instances, launch a background HiveServer2 service, and wait for port 10000 to become active).*

### 3. Run the Preprocessing Job
Execute the main run script:
```bash
bash scripts/run_preprocess.sh
```
This script will automatically:
1. Create the HDFS directory `/data/`.
2. Upload the staging `student_scores.csv` to HDFS.
3. Launch Beeline and execute all the queries defined in `scripts/preprocess.hql`.

---

## 📊 Preprocessing Tasks Summary

The SQL pipeline (`scripts/preprocess.hql`) handles all required lab tasks:
* **Step 1 (Load Staging Table)**: Creates `raw_data` text table.
* **Step 2 (Explore Data)**: Queries raw rows to check schema mapping.
* **Step 3 (Missing Values)**: Uses `COALESCE` to impute missing marks (Mary's empty cell) to `0`.
* **Step 4 (Deduplication)**: Uses `SELECT DISTINCT` to merge John's duplicate records.
* **Step 5 (Format Standardization)**: Converts names to `UPPERCASE`, courses to `lowercase`, and maps inconsistent genders (`male`, `M`) to `Male`/`Female` using `CASE` statements.
* **Step 6 (Noise Detection)**: Filters out outliers (David's score of `120`).
* **Step 7 (CTAS Output)**: Saves 7 clean student records to a snappy-compressed ORC table `cleaned_student_scores`.

### Memory Allocation Workaround
To prevent MapReduce from throwing `OutOfMemoryError: Java heap space` on memory-limited WSL environments, the script sets sorting memory limits:
```sql
SET mapreduce.task.io.sort.mb=10;
```
