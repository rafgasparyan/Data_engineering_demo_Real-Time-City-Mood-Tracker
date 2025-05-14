from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_trunc
import json

def read_kafka_stream(spark: SparkSession, topic: str, schema, alias: str, select_exprs: list):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias(alias))
        .selectExpr(*select_exprs)
        .withColumn("event_time", date_trunc("minute", col("event_time")))
    )


def write_to_mongo_factory(collection_name, log_prefix=""):
    def write_to_mongo(df, batch_id):
        records = [json.loads(row) for row in df.na.drop().toJSON().collect()]
        print(f"[{log_prefix} BATCH {batch_id}] Writing {len(records)} records to MongoDB")
        if records:
            client = MongoClient("mongodb://mongo:27017/")
            db = client["city_mood"]
            db[collection_name].insert_many(records)
            client.close()
    return write_to_mongo
