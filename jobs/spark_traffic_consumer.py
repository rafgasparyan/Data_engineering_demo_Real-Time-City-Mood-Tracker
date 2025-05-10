from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pymongo import MongoClient
from stream_utils.kafka_reader import read_kafka_stream
from stream_utils.schemas import traffic_schema

spark = SparkSession.builder \
    .appName("TrafficStreamConsumer") \
    .getOrCreate()

df_parsed = read_kafka_stream(
    spark,
    topic="traffic",
    schema=traffic_schema,
    alias="data",
    select_exprs=[
        "data.intersection", "data.vehicle_id", "data.speed", "to_timestamp(data.timestamp) as event_time"
    ]
)

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
