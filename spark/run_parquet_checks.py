from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RunParquetChecks") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

parquet_path = "hdfs://namenode:9000/data/cdc/raw"
print('Reading parquet from', parquet_path)

df = spark.read.parquet(parquet_path)
df.createOrReplaceTempView("raw_cdc")

print('\n=== TOTAL ROWS ===')
spark.sql('SELECT COUNT(*) AS total_rows FROM raw_cdc').show()

print('\n=== ROWS PER TOPIC (sample) ===')
spark.sql('SELECT topic, COUNT(*) AS cnt FROM raw_cdc GROUP BY topic ORDER BY cnt DESC LIMIT 20').show(truncate=False)

print('\n=== SAMPLE ROWS ===')
spark.sql('SELECT topic, `value` FROM raw_cdc LIMIT 10').show(truncate=False)

print('\n=== ROWS CONTAINING Cairo ===')
spark.sql("SELECT `value` FROM raw_cdc WHERE `value` LIKE '%Cairo%' LIMIT 20").show(truncate=False)

print('\n=== ROWS WITH customer_id = 902 (text search) ===')
spark.sql("SELECT `value` FROM raw_cdc WHERE `value` LIKE '%\"customer_id\":902%' LIMIT 20").show(truncate=False)

spark.stop()
