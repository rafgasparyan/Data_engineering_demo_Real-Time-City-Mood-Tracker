from pyspark.sql import SparkSession
from stream_utils.utils import read_kafka_stream
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

df_parsed.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
