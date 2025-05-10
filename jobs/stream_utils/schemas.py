from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

traffic_schema = StructType() \
    .add("intersection", StringType()) \
    .add("vehicle_id", StringType()) \
    .add("speed", DoubleType()) \
    .add("timestamp", TimestampType())

weather_schema = StructType() \
    .add("timestamp", TimestampType()) \
    .add("temp", DoubleType()) \
    .add("windspeed", DoubleType()) \
    .add("weather", StringType())

news_schema = StructType() \
    .add("timestamp", TimestampType()) \
    .add("headline", StringType()) \
    .add("sentiment", StringType())
