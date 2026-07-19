from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark Session with Kafka packages
spark = SparkSession.builder \
    .appName("KafkaForexProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🚀 Starting Spark Streaming to consume from Kafka...")

# Define the schema of the JSON message
schema = StructType([
    StructField("date", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True)
])

# 1. Read from Kafka (batch mode for simplicity in this assignment)
# We read all messages currently in the topic.
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "forex_stream") \
    .option("startingOffsets", "earliest") \
    .load()

# 2. Parse the JSON value
# Kafka payload is in binary 'value' column
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 3. Perform any required Spark transformations
# (e.g., dropping nulls)
cleaned_df = parsed_df.dropna()

print(f"✅ Processed {cleaned_df.count()} records from Kafka!")
cleaned_df.show(5)

# 4. Save to Hadoop (HDFS)
# The NameNode is on hdfs://namenode:9000
HDFS_OUTPUT_PATH = "hdfs://namenode:9000/forex_data/processed.csv"

print(f"💾 Saving processed data to Hadoop HDFS: {HDFS_OUTPUT_PATH}")
cleaned_df.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(HDFS_OUTPUT_PATH)

# 5. Save to Shared Workspace (For Streamlit UI)
LOCAL_OUTPUT_PATH = "/workspace/DSA3030_TimeSeries/data/processed_forex.csv"
print(f"💾 Saving processed data for Streamlit UI: {LOCAL_OUTPUT_PATH}")
cleaned_df.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(LOCAL_OUTPUT_PATH)

print("🎉 Spark Processing Complete. Data is safely stored in Hadoop and ready for Streamlit!")
spark.stop()
