from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

from jobs.stream_utils.utils import write_to_mongo_factory
from stream_utils.utils import read_kafka_stream
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

write_to_mongo = write_to_mongo_factory("traffic_events", log_prefix="TRAFFIC")

df_scored.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
