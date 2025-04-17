from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, DoubleType, StringType, TimestampType
from pymongo import MongoClient

spark = SparkSession.builder \
    .appName("WeatherStreamConsumer") \
    .getOrCreate()

schema = StructType() \
    .add("timestamp", TimestampType()) \
    .add("temp", DoubleType()) \
    .add("windspeed", DoubleType()) \
    .add("weather", StringType())

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

def write_to_mongo(batch_df, batch_id):
    records = batch_df.toPandas().to_dict("records")
    if records:
        client = MongoClient("mongodb://mongo:27017/")
        db = client["city_mood"]
        collection = db["weather_events"]
        collection.insert_many(records)
        client.close()

df_parsed.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
