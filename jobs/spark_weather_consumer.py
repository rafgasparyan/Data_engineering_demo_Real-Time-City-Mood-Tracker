from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, DoubleType, StringType, TimestampType
from pymongo import MongoClient
from stream_utils.kafka_reader import read_kafka_stream
from jobs.stream_utils.schemas import weather_schema
from jobs.stream_utils.utils import write_to_mongo_factory


spark = SparkSession.builder \
    .appName("WeatherStreamConsumer") \
    .getOrCreate()


df_parsed = read_kafka_stream(
    spark,
    topic="weather",
    schema=weather_schema,
    alias="data",
    select_exprs=[
        "to_timestamp(data.timestamp) as event_time", "data.temp", "data.windspeed", "data.weather"
    ]
)


write_to_mongo = write_to_mongo_factory("weather_events", log_prefix="WEATHER")


# def write_to_mongo(batch_df, batch_id):
#     records = batch_df.toPandas().to_dict("records")
#     if records:
#         client = MongoClient("mongodb://mongo:27017/")
#         db = client["city_mood"]
#         collection = db["weather_events"]
#         collection.insert_many(records)
#         client.close()

df_parsed.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
