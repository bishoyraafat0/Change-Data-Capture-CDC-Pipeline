from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# Initialize Spark Session
spark = (
    SparkSession.builder
    .appName("sqlserver_cdc_to_hdfs")
    .master("spark://spark-master:7077") 
    .getOrCreate()
)
# Set log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Kafka configuration
topic_pattern = "sqlsrv-fresh.FP-DWH.dbo.*"
# Adjust the Kafka bootstrap servers as per your setup
kafka_bootstrap_servers = "kafka:9092"
# Read from Kafka
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribePattern", topic_pattern)
    .option("startingOffsets", "earliest")  #
    .load()
)

# Select and cast the necessary fields
events_df = (
    raw_df
    .selectExpr(
        "topic",
        "CAST(key AS STRING) AS key",
        "CAST(value AS STRING) AS value",
        "timestamp"
    )
)
# HDFS paths
hdfs_base_path = "hdfs://namenode:9000/data/cdc"
# Output and checkpoint paths
output_path = f"{hdfs_base_path}/raw"
checkpoint_path = f"{hdfs_base_path}/checkpoints/raw"
# Write the stream to HDFS in Parquet format
query = (
    events_df.writeStream
    .format("parquet")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .start()
)
# Await termination of the streaming query
query.awaitTermination()
