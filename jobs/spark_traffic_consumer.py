from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("TrafficStreamConsumer") \
    .getOrCreate()

# schema of incoming JSON
schema = StructType() \
    .add("intersection", StringType()) \
    .add("vehicle_id", StringType()) \
    .add("speed", DoubleType()) \
    .add("timestamp", DoubleType())

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "traffic") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

query = df_parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
