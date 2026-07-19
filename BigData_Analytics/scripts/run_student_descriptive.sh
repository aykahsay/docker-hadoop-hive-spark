#!/bin/bash
# ============================================================
# run_student_descriptive.sh
# Runs student_descriptive.hql using hive CLI (embedded metastore)
# Mirrors the exact same pattern as run_all_queries.sh
# ============================================================

export HADOOP_HOME=/opt/hadoop
export HIVE_HOME=/opt/hive
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$PATH

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT="/mnt/c/bigdata/big-data-analytics-group-assignemnt"
OUT_DIR="$SCRIPT_DIR/output"
OUT_FILE="$OUT_DIR/student_descriptive_output.txt"
HIVE_OPTS="--hiveconf hive.exec.mode.local.auto=true --hiveconf mapreduce.task.io.sort.mb=32"

mkdir -p "$OUT_DIR"

echo "=============================================="
echo "  Student Descriptive Analysis – HiveQL"
echo "  $(date)"
echo "=============================================="

echo ""
echo "=== Step 1: Upload dataset to HDFS ==="
hdfs dfs -put -f \
  "$PROJECT/data/student_performance.csv" \
  /data/BigData_Student_Performance_Dataset_1000.csv 2>&1
hdfs dfs -ls /data/ 2>&1 | grep -E "student|BigData"
echo "Upload done."

echo ""
echo "=== Step 2: Run Section 0 — Setup & Table Creation ==="
hive $HIVE_OPTS -e "
CREATE DATABASE IF NOT EXISTS student_analysis;
USE student_analysis;
DROP TABLE IF EXISTS student_perf_raw;
DROP TABLE IF EXISTS student_perf;
CREATE TABLE student_perf_raw (
    student_id STRING, name STRING, gender STRING, age STRING,
    department STRING, program STRING, year STRING, semester STRING,
    attendance STRING, assignment STRING, cat STRING, finalexam STRING,
    studyhours STRING, internetaccess STRING, device STRING,
    parentincome STRING, scholarship STRING, skillscount STRING,
    projects STRING, lmslogins STRING, libraryvisits STRING,
    location STRING, graduated STRING, placement STRING,
    gpa STRING, total STRING, grade STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');
LOAD DATA INPATH '/data/BigData_Student_Performance_Dataset_1000.csv'
OVERWRITE INTO TABLE student_perf_raw;
CREATE TABLE student_perf AS
SELECT student_id, name,
  UPPER(TRIM(gender)) AS gender,
  CAST(age AS INT) AS age,
  UPPER(TRIM(department)) AS department,
  UPPER(TRIM(program)) AS program,
  CAST(year AS INT) AS year,
  CAST(semester AS INT) AS semester,
  CAST(attendance AS INT) AS attendance,
  CAST(assignment AS INT) AS assignment,
  CAST(cat AS INT) AS cat,
  CAST(finalexam AS INT) AS finalexam,
  CAST(studyhours AS INT) AS studyhours,
  UPPER(TRIM(internetaccess)) AS internetaccess,
  UPPER(TRIM(device)) AS device,
  CAST(parentincome AS INT) AS parentincome,
  UPPER(TRIM(scholarship)) AS scholarship,
  CAST(skillscount AS INT) AS skillscount,
  CAST(projects AS INT) AS projects,
  CAST(lmslogins AS INT) AS lmslogins,
  CAST(libraryvisits AS INT) AS libraryvisits,
  INITCAP(TRIM(location)) AS location,
  UPPER(TRIM(graduated)) AS graduated,
  UPPER(TRIM(placement)) AS placement,
  CAST(gpa AS DOUBLE) AS gpa,
  CAST(total AS DOUBLE) AS total,
  UPPER(TRIM(grade)) AS grade
FROM student_perf_raw
WHERE student_id IS NOT NULL AND student_id != '';
SELECT CONCAT('=== Rows loaded: ', COUNT(*), ' ===') FROM student_perf;
" 2>/dev/null | tee "$OUT_DIR/s00_setup.txt"

echo ""
echo "=== Step 3: Section 1 — Data Quality ==="
hive $HIVE_OPTS -e "
USE student_analysis;
SELECT '--- Total Rows ---' AS label;
SELECT COUNT(*) AS total_rows FROM student_perf;
SELECT '--- NULL Check ---' AS label;
SELECT
  SUM(CASE WHEN gpa IS NULL THEN 1 ELSE 0 END) AS null_gpa,
  SUM(CASE WHEN grade IS NULL OR grade='' THEN 1 ELSE 0 END) AS null_grade,
  SUM(CASE WHEN attendance IS NULL THEN 1 ELSE 0 END) AS null_attendance,
  SUM(CASE WHEN placement IS NULL OR placement='' THEN 1 ELSE 0 END) AS null_placement
FROM student_perf;
SELECT '--- Duplicate Student IDs ---' AS label;
SELECT COUNT(*) AS dup_count FROM (
  SELECT student_id, COUNT(*) AS cnt FROM student_perf
  GROUP BY student_id HAVING cnt > 1) d;
SELECT '--- Invalid GPA (out of 0-4) ---' AS label;
SELECT COUNT(*) AS bad_gpa FROM student_perf WHERE gpa < 0 OR gpa > 4;
SELECT '--- Grade Distinct Values ---' AS label;
SELECT grade, COUNT(*) AS cnt FROM student_perf GROUP BY grade ORDER BY grade;
SELECT '--- Program Values ---' AS label;
SELECT program, COUNT(*) AS cnt FROM student_perf GROUP BY program;
SELECT '--- Gender Values ---' AS label;
SELECT gender, COUNT(*) AS cnt FROM student_perf GROUP BY gender;
SELECT '--- Internet Access Values ---' AS label;
SELECT internetaccess, COUNT(*) AS cnt FROM student_perf GROUP BY internetaccess;
SELECT '--- Placement Values ---' AS label;
SELECT placement, COUNT(*) AS cnt FROM student_perf GROUP BY placement;
" 2>/dev/null | tee "$OUT_DIR/s01_quality.txt"

echo ""
echo "=== Step 4: Section 2 — Overall Statistics ==="
hive $HIVE_OPTS -e "
USE student_analysis;
SELECT '--- Numeric Summary Stats ---' AS label;
SELECT
  ROUND(AVG(age),2) AS avg_age, MIN(age) AS min_age, MAX(age) AS max_age,
  ROUND(AVG(gpa),3) AS avg_gpa, MIN(gpa) AS min_gpa, MAX(gpa) AS max_gpa,
  ROUND(STDDEV(gpa),3) AS std_gpa,
  ROUND(AVG(attendance),2) AS avg_attendance, ROUND(STDDEV(attendance),2) AS std_attendance,
  ROUND(AVG(studyhours),2) AS avg_studyhours,
  ROUND(AVG(parentincome),0) AS avg_parentincome,
  MIN(parentincome) AS min_income, MAX(parentincome) AS max_income,
  ROUND(AVG(lmslogins),2) AS avg_lmslogins,
  ROUND(AVG(libraryvisits),2) AS avg_libraryvisits,
  ROUND(AVG(skillscount),2) AS avg_skillscount,
  ROUND(AVG(projects),2) AS avg_projects
FROM student_perf;
SELECT '--- Overall Graduation Rate ---' AS label;
SELECT COUNT(*) AS total,
  SUM(CASE WHEN graduated='YES' THEN 1 ELSE 0 END) AS graduated,
  ROUND(100.0*SUM(CASE WHEN graduated='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS grad_rate_pct
FROM student_perf;
SELECT '--- Overall Placement Rate ---' AS label;
SELECT SUM(CASE WHEN placement='YES' THEN 1 ELSE 0 END) AS placed,
  ROUND(100.0*SUM(CASE WHEN placement='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS placement_pct
FROM student_perf;
SELECT '--- GPA Percentiles ---' AS label;
SELECT
  ROUND(PERCENTILE_APPROX(gpa,0.25),3) AS gpa_p25,
  ROUND(PERCENTILE_APPROX(gpa,0.50),3) AS gpa_median,
  ROUND(PERCENTILE_APPROX(gpa,0.75),3) AS gpa_p75,
  ROUND(PERCENTILE_APPROX(gpa,0.90),3) AS gpa_p90
FROM student_perf;
" 2>/dev/null | tee "$OUT_DIR/s02_summary.txt"

echo ""
echo "=== Step 5: Section 3 — Grade Distribution ==="
hive $HIVE_OPTS -e "
USE student_analysis;
SELECT '--- Grade Count & Percentage ---' AS label;
SELECT grade, COUNT(*) AS cnt,
  ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),2) AS pct,
  ROUND(AVG(gpa),3) AS avg_gpa,
  ROUND(AVG(attendance),2) AS avg_attendance,
  ROUND(AVG(studyhours),2) AS avg_studyhours
FROM student_perf GROUP BY grade ORDER BY grade;
SELECT '--- GPA Band Distribution ---' AS label;
SELECT
  CASE WHEN gpa>=3.5 THEN '3.5-4.0 Distinction'
       WHEN gpa>=3.0 THEN '3.0-3.49 Credit'
       WHEN gpa>=2.5 THEN '2.5-2.99 Pass'
       WHEN gpa>=2.0 THEN '2.0-2.49 Low Pass'
       ELSE 'Below 2.0 Fail' END AS gpa_band,
  COUNT(*) AS students,
  ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),2) AS pct
FROM student_perf
GROUP BY CASE WHEN gpa>=3.5 THEN '3.5-4.0 Distinction'
              WHEN gpa>=3.0 THEN '3.0-3.49 Credit'
              WHEN gpa>=2.5 THEN '2.5-2.99 Pass'
              WHEN gpa>=2.0 THEN '2.0-2.49 Low Pass'
              ELSE 'Below 2.0 Fail' END
ORDER BY MIN(gpa) DESC;
" 2>/dev/null | tee "$OUT_DIR/s03_grade_dist.txt"

echo ""
echo "=== Step 6: Section 4 — Department Analysis ==="
hive $HIVE_OPTS -e "
USE student_analysis;
SELECT '--- Metrics per Department ---' AS label;
SELECT department, COUNT(*) AS students,
  ROUND(AVG(gpa),3) AS avg_gpa,
  ROUND(AVG(attendance),2) AS avg_attendance,
  ROUND(AVG(studyhours),2) AS avg_studyhours,
  ROUND(100.0*SUM(CASE WHEN placement='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS placement_pct,
  ROUND(100.0*SUM(CASE WHEN scholarship='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS scholarship_pct
FROM student_perf GROUP BY department ORDER BY avg_gpa DESC;
SELECT '--- Grade Spread per Department ---' AS label;
SELECT department,
  SUM(CASE WHEN grade='A' THEN 1 ELSE 0 END) AS A,
  SUM(CASE WHEN grade='B' THEN 1 ELSE 0 END) AS B,
  SUM(CASE WHEN grade='C' THEN 1 ELSE 0 END) AS C,
  SUM(CASE WHEN grade='D' THEN 1 ELSE 0 END) AS D,
  SUM(CASE WHEN grade='F' THEN 1 ELSE 0 END) AS F,
  COUNT(*) AS total
FROM student_perf GROUP BY department ORDER BY department;
" 2>/dev/null | tee "$OUT_DIR/s04_department.txt"

echo ""
echo "=== Step 7: Section 5 — Gender Analysis ==="
hive $HIVE_OPTS -e "
USE student_analysis;
SELECT '--- Performance by Gender ---' AS label;
SELECT gender, COUNT(*) AS count,
  ROUND(AVG(gpa),3) AS avg_gpa,
  ROUND(AVG(attendance),2) AS avg_attendance,
  ROUND(AVG(studyhours),2) AS avg_studyhours,
  ROUND(AVG(lmslogins),2) AS avg_lmslogins,
  ROUND(100.0*SUM(CASE WHEN placement='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS placement_pct,
  ROUND(100.0*SUM(CASE WHEN scholarship='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS scholarship_pct,
  ROUND(100.0*SUM(CASE WHEN graduated='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS graduation_pct
FROM student_perf GROUP BY gender ORDER BY avg_gpa DESC;
SELECT '--- Grade Distribution by Gender ---' AS label;
SELECT gender,
  SUM(CASE WHEN grade='A' THEN 1 ELSE 0 END) AS A,
  SUM(CASE WHEN grade='B' THEN 1 ELSE 0 END) AS B,
  SUM(CASE WHEN grade='C' THEN 1 ELSE 0 END) AS C,
  SUM(CASE WHEN grade='D' THEN 1 ELSE 0 END) AS D,
  SUM(CASE WHEN grade='F' THEN 1 ELSE 0 END) AS F
FROM student_perf GROUP BY gender;
" 2>/dev/null | tee "$OUT_DIR/s05_gender.txt"

echo ""
echo "=== Step 8: Section 6 — Program Analysis (BSc vs MSc) ==="
hive $HIVE_OPTS -e "
USE student_analysis;
SELECT '--- BSc vs MSc Key Metrics ---' AS label;
SELECT program, COUNT(*) AS total,
  ROUND(AVG(gpa),3) AS avg_gpa, ROUND(AVG(age),1) AS avg_age,
  ROUND(AVG(attendance),2) AS avg_attendance, ROUND(AVG(studyhours),2) AS avg_studyhours,
  ROUND(100.0*SUM(CASE WHEN placement='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS placement_pct,
  ROUND(100.0*SUM(CASE WHEN graduated='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS graduation_pct,
  ROUND(100.0*SUM(CASE WHEN scholarship='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS scholarship_pct
FROM student_perf GROUP BY program;
SELECT '--- Program x Department ---' AS label;
SELECT program, department, COUNT(*) AS students, ROUND(AVG(gpa),3) AS avg_gpa
FROM student_perf GROUP BY program, department ORDER BY program, avg_gpa DESC;
" 2>/dev/null | tee "$OUT_DIR/s06_program.txt"

echo ""
echo "=== Step 9: Section 7 — Academic Performance ==="
hive $HIVE_OPTS -e "
USE student_analysis;
SELECT '--- Score Averages by Grade ---' AS label;
SELECT grade,
  ROUND(AVG(attendance),2) AS avg_attendance,
  ROUND(AVG(assignment),2) AS avg_assignment,
  ROUND(AVG(cat),2) AS avg_cat,
  ROUND(AVG(finalexam),2) AS avg_finalexam,
  ROUND(AVG(total),2) AS avg_total,
  COUNT(*) AS students
FROM student_perf GROUP BY grade ORDER BY avg_total DESC;
SELECT '--- Attendance Band vs GPA ---' AS label;
SELECT
  CASE WHEN attendance>=90 THEN '90-100 Excellent'
       WHEN attendance>=75 THEN '75-89 Good'
       WHEN attendance>=60 THEN '60-74 Average'
       ELSE 'Below 60 Poor' END AS band,
  COUNT(*) AS students, ROUND(AVG(gpa),3) AS avg_gpa,
  ROUND(100.0*SUM(CASE WHEN placement='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS placement_pct
FROM student_perf
GROUP BY CASE WHEN attendance>=90 THEN '90-100 Excellent'
              WHEN attendance>=75 THEN '75-89 Good'
              WHEN attendance>=60 THEN '60-74 Average'
              ELSE 'Below 60 Poor' END
ORDER BY avg_gpa DESC;
SELECT '--- Study Hours vs GPA ---' AS label;
SELECT
  CASE WHEN studyhours>=8 THEN '8+ High'
       WHEN studyhours>=5 THEN '5-7 Medium'
       WHEN studyhours>=2 THEN '2-4 Low'
       ELSE '0-1 Very Low' END AS study_band,
  COUNT(*) AS students, ROUND(AVG(gpa),3) AS avg_gpa
FROM student_perf
GROUP BY CASE WHEN studyhours>=8 THEN '8+ High'
              WHEN studyhours>=5 THEN '5-7 Medium'
              WHEN studyhours>=2 THEN '2-4 Low'
              ELSE '0-1 Very Low' END
ORDER BY avg_gpa DESC;
" 2>/dev/null | tee "$OUT_DIR/s07_academic.txt"

echo ""
echo "=== Step 10: Section 8 — Engagement Metrics ==="
hive $HIVE_OPTS -e "
USE student_analysis;
SELECT '--- LMS Logins vs GPA ---' AS label;
SELECT
  CASE WHEN lmslogins>=150 THEN '150+ Very Active'
       WHEN lmslogins>=100 THEN '100-149 Active'
       WHEN lmslogins>=50  THEN '50-99 Moderate'
       ELSE '0-49 Inactive' END AS lms_band,
  COUNT(*) AS students, ROUND(AVG(gpa),3) AS avg_gpa
FROM student_perf
GROUP BY CASE WHEN lmslogins>=150 THEN '150+ Very Active'
              WHEN lmslogins>=100 THEN '100-149 Active'
              WHEN lmslogins>=50  THEN '50-99 Moderate'
              ELSE '0-49 Inactive' END
ORDER BY avg_gpa DESC;
SELECT '--- Library Visits vs GPA ---' AS label;
SELECT
  CASE WHEN libraryvisits>=40 THEN '40+ Very High'
       WHEN libraryvisits>=25 THEN '25-39 High'
       WHEN libraryvisits>=10 THEN '10-24 Medium'
       ELSE '0-9 Low' END AS lib_band,
  COUNT(*) AS students, ROUND(AVG(gpa),3) AS avg_gpa
FROM student_perf
GROUP BY CASE WHEN libraryvisits>=40 THEN '40+ Very High'
              WHEN libraryvisits>=25 THEN '25-39 High'
              WHEN libraryvisits>=10 THEN '10-24 Medium'
              ELSE '0-9 Low' END
ORDER BY avg_gpa DESC;
SELECT '--- Skills Count vs GPA & Placement ---' AS label;
SELECT skillscount, COUNT(*) AS students, ROUND(AVG(gpa),3) AS avg_gpa,
  ROUND(100.0*SUM(CASE WHEN placement='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS placement_pct
FROM student_perf GROUP BY skillscount ORDER BY skillscount;
SELECT '--- Projects vs Placement ---' AS label;
SELECT projects, COUNT(*) AS students, ROUND(AVG(gpa),3) AS avg_gpa,
  ROUND(100.0*SUM(CASE WHEN placement='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS placement_pct
FROM student_perf GROUP BY projects ORDER BY projects;
" 2>/dev/null | tee "$OUT_DIR/s08_engagement.txt"

echo ""
echo "=== Step 11: Section 9 — Socioeconomic Analysis ==="
hive $HIVE_OPTS -e "
USE student_analysis;
SELECT '--- Parent Income Quintile vs GPA & Placement ---' AS label;
SELECT
  CASE WHEN parentincome>=240000 THEN 'Q5 Top 240k+'
       WHEN parentincome>=180000 THEN 'Q4 Upper 180-240k'
       WHEN parentincome>=120000 THEN 'Q3 Middle 120-180k'
       WHEN parentincome>=60000  THEN 'Q2 Lower 60-120k'
       ELSE 'Q1 Bottom <60k' END AS income_band,
  COUNT(*) AS students, ROUND(AVG(gpa),3) AS avg_gpa,
  ROUND(100.0*SUM(CASE WHEN placement='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS placement_pct,
  ROUND(100.0*SUM(CASE WHEN scholarship='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS scholarship_pct
FROM student_perf
GROUP BY CASE WHEN parentincome>=240000 THEN 'Q5 Top 240k+'
              WHEN parentincome>=180000 THEN 'Q4 Upper 180-240k'
              WHEN parentincome>=120000 THEN 'Q3 Middle 120-180k'
              WHEN parentincome>=60000  THEN 'Q2 Lower 60-120k'
              ELSE 'Q1 Bottom <60k' END
ORDER BY MIN(parentincome) DESC;
" 2>/dev/null | tee "$OUT_DIR/s09_socioeconomic.txt"

echo ""
echo "=== Step 12: Section 10 — Technology Access ==="
hive $HIVE_OPTS -e "
USE student_analysis;
SELECT '--- Internet Access vs GPA ---' AS label;
SELECT internetaccess, COUNT(*) AS students, ROUND(AVG(gpa),3) AS avg_gpa,
  ROUND(AVG(lmslogins),2) AS avg_lmslogins,
  ROUND(100.0*SUM(CASE WHEN placement='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS placement_pct
FROM student_perf GROUP BY internetaccess;
SELECT '--- Device Type vs GPA ---' AS label;
SELECT device, COUNT(*) AS students, ROUND(AVG(gpa),3) AS avg_gpa,
  ROUND(100.0*SUM(CASE WHEN placement='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS placement_pct
FROM student_perf GROUP BY device ORDER BY avg_gpa DESC;
SELECT '--- Internet x Device Cross-tab ---' AS label;
SELECT internetaccess, device, COUNT(*) AS students, ROUND(AVG(gpa),3) AS avg_gpa
FROM student_perf GROUP BY internetaccess, device ORDER BY avg_gpa DESC;
" 2>/dev/null | tee "$OUT_DIR/s10_technology.txt"

echo ""
echo "=== Step 13: Section 11 — Top & Bottom Performers ==="
hive $HIVE_OPTS -e "
USE student_analysis;
SELECT '--- Top 10 Students by GPA ---' AS label;
SELECT student_id, name, department, program, gpa, grade, attendance, studyhours, placement
FROM student_perf ORDER BY gpa DESC LIMIT 10;
SELECT '--- Bottom 10 Students by GPA ---' AS label;
SELECT student_id, name, department, program, gpa, grade, attendance, studyhours, placement
FROM student_perf ORDER BY gpa ASC LIMIT 10;
SELECT '--- High GPA (>=3.5) Without Placement ---' AS label;
SELECT student_id, name, department, gpa, skillscount, projects, location
FROM student_perf WHERE gpa>=3.5 AND placement='NO' ORDER BY gpa DESC LIMIT 15;
SELECT '--- Students with Low Attendance (<60%) ---' AS label;
SELECT COUNT(*) AS count, ROUND(AVG(gpa),3) AS avg_gpa, ROUND(AVG(finalexam),2) AS avg_finalexam
FROM student_perf WHERE attendance < 60;
" 2>/dev/null | tee "$OUT_DIR/s11_performers.txt"

echo ""
echo "=== Step 14: Section 12 — Location Analysis ==="
hive $HIVE_OPTS -e "
USE student_analysis;
SELECT '--- Students per City ---' AS label;
SELECT location, COUNT(*) AS students, ROUND(AVG(gpa),3) AS avg_gpa,
  ROUND(AVG(parentincome),0) AS avg_income,
  ROUND(100.0*SUM(CASE WHEN placement='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS placement_pct,
  ROUND(100.0*SUM(CASE WHEN internetaccess='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS internet_pct
FROM student_perf GROUP BY location ORDER BY placement_pct DESC;
" 2>/dev/null | tee "$OUT_DIR/s12_location.txt"

echo ""
echo "=== Step 15: Section 13 — Scholarship Impact ==="
hive $HIVE_OPTS -e "
USE student_analysis;
SELECT '--- Scholarship vs No Scholarship ---' AS label;
SELECT scholarship, COUNT(*) AS students,
  ROUND(AVG(gpa),3) AS avg_gpa,
  ROUND(AVG(attendance),2) AS avg_attendance,
  ROUND(AVG(parentincome),0) AS avg_income,
  ROUND(100.0*SUM(CASE WHEN placement='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS placement_pct,
  ROUND(100.0*SUM(CASE WHEN graduated='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS graduation_pct
FROM student_perf GROUP BY scholarship;
SELECT '--- Scholarship Rate by Department ---' AS label;
SELECT department,
  SUM(CASE WHEN scholarship='YES' THEN 1 ELSE 0 END) AS scholars,
  COUNT(*) AS total,
  ROUND(100.0*SUM(CASE WHEN scholarship='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS scholarship_rate
FROM student_perf GROUP BY department ORDER BY scholarship_rate DESC;
" 2>/dev/null | tee "$OUT_DIR/s13_scholarship.txt"

echo ""
echo "=== Step 16: Section 14 — Window Functions / Rankings ==="
hive $HIVE_OPTS -e "
USE student_analysis;
SELECT '--- Top 3 Students per Department by GPA ---' AS label;
SELECT * FROM (
  SELECT student_id, name, department, program, gpa, grade, placement,
    RANK() OVER (PARTITION BY department ORDER BY gpa DESC) AS dept_rank
  FROM student_perf
) ranked WHERE dept_rank <= 3 ORDER BY department, dept_rank;
SELECT '--- GPA Percentile Rank (sample 30 rows) ---' AS label;
SELECT student_id, name, department, gpa,
  ROUND(PERCENT_RANK() OVER (PARTITION BY department ORDER BY gpa)*100,1) AS pct_in_dept,
  ROUND(PERCENT_RANK() OVER (ORDER BY gpa)*100,1) AS overall_pct
FROM student_perf ORDER BY department, pct_in_dept DESC LIMIT 30;
" 2>/dev/null | tee "$OUT_DIR/s14_rankings.txt"

echo ""
echo "=== Step 17: Final Summary ==="
hive $HIVE_OPTS -e "
USE student_analysis;
SELECT '===== FINAL SUMMARY =====' AS label;
SELECT COUNT(*) AS total_students,
  ROUND(AVG(gpa),3) AS overall_avg_gpa,
  ROUND(100.0*SUM(CASE WHEN placement='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS placement_pct,
  ROUND(100.0*SUM(CASE WHEN graduated='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS graduation_pct,
  ROUND(100.0*SUM(CASE WHEN scholarship='YES' THEN 1 ELSE 0 END)/COUNT(*),2) AS scholarship_pct
FROM student_perf;
" 2>/dev/null | tee "$OUT_DIR/s15_summary.txt"

echo ""
echo "=== Merging all sections into single output file ==="
cat "$OUT_DIR"/s*.txt > "$OUT_FILE"

echo ""
echo "=============================================="
echo "  DESCRIPTIVE ANALYSIS COMPLETE!"
echo "  Output: $OUT_FILE"
echo "  Lines: $(wc -l < "$OUT_FILE")"
echo "=============================================="
echo ""
echo "Section files:"
ls -lh "$OUT_DIR"/s*.txt 2>/dev/null
