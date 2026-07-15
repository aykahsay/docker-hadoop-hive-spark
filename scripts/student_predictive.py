from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

print("⚡ Starting Apache Spark Predictive Analysis...")

# 1. Initialize SparkSession
spark = SparkSession.builder \
    .appName("StudentPerformancePrediction") \
    .getOrCreate()

# 2. Load Data from HDFS
# Adjust the path below to wherever your CSV is stored in HDFS
DATA_PATH = "hdfs://namenode:9000/user/hive/data/student_performance.csv"

try:
    print(f"📥 Loading dataset from {DATA_PATH}...")
    df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)
    
    print("📊 Schema Overview:")
    df.printSchema()

    # NOTE: The exact column names depend on your dataset.
    # Below is a hypothetical example assuming columns like 'Math_Score', 'Reading_Score', and 'Placement_Status'
    
    # 3. Data Preprocessing (Example)
    # If Placement_Status is a string ("Placed" / "Not Placed"), we need to index it to 0.0 and 1.0
    if 'Placement_Status' in df.columns:
        indexer = StringIndexer(inputCol="Placement_Status", outputCol="label")
        df = indexer.fit(df).transform(df)

        # 4. Feature Engineering
        feature_cols = [col for col in df.columns if col not in ['Placement_Status', 'label', 'Student_ID']]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        data = assembler.transform(df)

        # 5. Train/Test Split
        train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

        # 6. Train a Random Forest Model
        print("🌲 Training Random Forest Classifier...")
        rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=100)
        model = rf.fit(train_data)

        # 7. Evaluate the Model
        predictions = model.transform(test_data)
        evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)
        
        print("========================================")
        print(f"🎯 Model Accuracy on Test Data: {accuracy * 100:.2f}%")
        print("========================================")
    else:
        print("⚠️ 'Placement_Status' column not found. Please modify the script with your actual target column name to run the Machine Learning model.")
        df.show(5)

except Exception as e:
    print(f"❌ Error during Spark execution: {e}")
    print("Ensure that the CSV file exists in HDFS at the specified path.")

spark.stop()
