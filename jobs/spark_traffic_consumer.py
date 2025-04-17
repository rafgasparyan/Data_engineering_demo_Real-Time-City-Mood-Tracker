from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pymongo import MongoClient

spark = SparkSession.builder \
    .appName("TrafficStreamConsumer") \
    .getOrCreate()

schema = StructType() \
    .add("intersection", StringType()) \
    .add("vehicle_id", StringType()) \
    .add("speed", DoubleType()) \
    .add("timestamp", TimestampType())

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "traffic") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

df_scored = df_parsed.withColumn(
    "traffic_condition",
    when(col("speed") < 30, "heavy") \
        .when(col("speed") < 60, "moderate") \
        .otherwise("light")
)


def write_to_mongo(batch_df, batch_id):


    records = batch_df.toPandas().to_dict("records")
    print(f"[BATCH {batch_id}] Writing {len(records)} records to MongoDB")

    if records:
        client = MongoClient("mongodb://mongo:27017/")
        db = client["city_mood"]
        collection = db["traffic_events"]
        collection.insert_many(records)
        client.close()


df_scored.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
