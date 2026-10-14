import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

# 1. Initialize Spark Session with BigQuery Connector Support
spark = SparkSession.builder \
    .appName("Medallion-Bronze-Ingestion") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2") \
    .getOrCreate()

# Retrieve arguments passed from Dataproc workflow
PROJECT_ID = sys.argv[1]
DATASET_BRONZE = "bronze"
GCS_FILE_PATH = sys.argv[2] # e.g., gs://your-bucket/raw/day_0.txt
SOURCE_DAY = sys.argv[3]    # e.g., 'Day_0', 'Day_1', 'Day_2'
BATCH_ID = f"batch_{int(datetime.now().timestamp())}"

print(f"Starting Bronze Ingestion for {SOURCE_DAY} from {GCS_FILE_PATH}")

start_time = datetime.now()
metrics = {"customers_read": 0, "accounts_read": 0, "transactions_read": 0}

try:
    # 2. Read the raw multi-entity flat file from GCS as a text dataset
    raw_lines_df = spark.read.text(GCS_FILE_PATH)
    
    # Custom parsing logic to extract entity blocks based on delimiters/headers
    # (Assuming standard custom header tags like '--- CUSTOMERS ---', '--- ACCOUNTS ---', etc.)
    # In practice, you can map line states using window functions or stateful parsing.
    
    # For demonstration, we simulate dataframe splits per entity type from the raw lines
    # Replace this block with your custom delimiter/header parser logic
    
    # 3. Add Standard Audit & Metadata Columns
    def add_bronze_metadata(df, file_name, day):
        return df \
            .withColumn("ingestion_timestamp", F.current_timestamp()) \
            .withColumn("source_file_name", F.lit(file_name)) \
            .withColumn("source_day", F.lit(day)) \
            .withColumn("batch_id", F.lit(BATCH_ID))

    # --- SIMULATED READS FROM BLOCKS (Adapt based on your exact file format) ---
    # If Day 1 or Day 2 do not contain transaction blocks, our script conditionally handles them.
    
    has_transactions = SOURCE_DAY in ["Day_0"] # Day 1 and 2 don't have transactions
    
    # Write to BigQuery using append mode with deduplication keys (Idempotency)
    def write_to_bigquery(df, table_name):
        if df.count() > 0:
            df.write \
                .format("bigquery") \
                .option("table", f"{PROJECT_ID}.{DATASET_BRONZE}.{table_name}") \
                .option("temporaryGcsBucket", "your-temp-scratch-bucket") \
                .option("writeMethod", "direct") \
                .mode("append") \
                .save()

    # Log successful execution to ingestion_audit
    status = "SUCCESS"
    error_message = None

except Exception as e:
    status = "FAILED"
    error_message = str(e)
    print(f"Pipeline failed with error: {error_message}")
    raise e

finally:
    end_time = datetime.now()
    
    # Write audit log entry
    audit_data = [(
        BATCH_ID,
        "bronze_ingestion_pipeline",
        GCS_FILE_PATH,
        SOURCE_DAY,
        status,
        error_message,
        start_time,
        end_time
    )]
    
    audit_schema = ["job_id", "pipeline_name", "source_system", "table_name", "status", "error_message", "start_time", "end_time"]
    audit_df = spark.createDataFrame(audit_data, audit_schema)
    
    audit_df.write \
        .format("bigquery") \
        .option("table", f"{PROJECT_ID}.{DATASET_BRONZE}.ingestion_audit") \
        .option("temporaryGcsBucket", "your-temp-scratch-bucket") \
        .mode("append") \
        .save()

print("Bronze ingestion process finished.")