from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("read_cdc_parquet")
    .master("spark://spark-master:7077")
    .config("spark.executor.memory", "512m")
    .config("spark.driver.memory", "512m")
    .getOrCreate()
)

hdfs_path = "hdfs://namenode:9000/data/cdc/raw"
print(f"Reading parquet files from: {hdfs_path}")

try:
    df = spark.read.parquet(hdfs_path)
    print("Schema:")
    df.printSchema()

    print("Total rows:")
    print(df.count())

    print('\n=== Rows containing Cairo ===')
    df.filter(col('value').like('%Cairo%')).select('value').show(20, truncate=False)

    print('\n=== Rows containing customer_id 902 (search JSON string) ===')
    df.filter(col('value').like('%"customer_id":902%')).select('value').show(20, truncate=False)

except Exception as e:
    print('Error reading parquet or running queries:', e)

spark.stop()
