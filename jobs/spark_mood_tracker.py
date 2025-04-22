from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, expr, date_trunc, udf
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pymongo import MongoClient

spark = SparkSession.builder \
    .appName("MoodTracker") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

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

traffic_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "traffic") \
    .option("startingOffsets", "latest") \
    .load()

traffic = traffic_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), traffic_schema).alias("t")) \
    .selectExpr("t.intersection", "t.speed", "to_timestamp(t.timestamp) as event_time") \
    .withColumn("event_time", date_trunc("minute", col("event_time")))

weather_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather") \
    .option("startingOffsets", "latest") \
    .load()

weather = weather_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), weather_schema).alias("w")) \
    .selectExpr("w.temp", "w.windspeed", "w.weather", "to_timestamp(w.timestamp) as event_time") \
    .withColumn("event_time", date_trunc("minute", col("event_time")))

news_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "news") \
    .option("startingOffsets", "latest") \
    .load()

news = news_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), news_schema).alias("n")) \
    .selectExpr("to_timestamp(n.timestamp) as event_time", "n.sentiment") \
    .withColumn("event_time", date_trunc("minute", col("event_time")))

traffic_grouped = traffic.withWatermark("event_time", "1 minute").groupBy("event_time", "intersection") \
    .agg(expr("avg(speed) as avg_speed"))

weather_grouped = weather.withWatermark("event_time", "1 minute").groupBy("event_time") \
    .agg(
    expr("avg(temp) as avg_temp"),
    expr("first(weather) as weather")
)

news_grouped = news.withWatermark("event_time", "1 minute").groupBy("event_time") \
    .agg(
    expr("first(sentiment) as sentiment")
)

joined = traffic_grouped.join(weather_grouped, on="event_time", how="left") \
    .join(news_grouped, on="event_time", how="left")

RELAXING_WEATHER = {"clear", "mainly_clear", "partly_cloudy"}
STRESSFUL_WEATHER = {
    "overcast", "fog", "depositing_rime_fog",
    "drizzle_light", "drizzle_moderate", "drizzle_dense",
    "rain_slight", "rain_moderate", "rain_heavy",
    "rain_showers_slight", "rain_showers_moderate", "rain_showers_heavy",
    "snow_slight", "snow_moderate", "snow_heavy",
    "snow_showers_slight", "snow_showers_heavy",
    "thunderstorm", "thunderstorm_with_hail"
}


def label_mood(avg_speed, weather, sentiment):
    if avg_speed is None or weather is None:
        return "unknown"

    if sentiment == "negative":
        return "tense"
    elif avg_speed > 60 and weather in RELAXING_WEATHER:
        return "relaxed"
    elif avg_speed < 30 and weather in STRESSFUL_WEATHER:
        return "stressed"
    elif avg_speed < 30 and weather in RELAXING_WEATHER:
        return "slowed_but_chill"
    elif avg_speed > 60 and weather in STRESSFUL_WEATHER:
        return "fast_but_gloomy"
    else:
        return "normal"


mood_udf = udf(label_mood, StringType())
result = joined.withColumn("mood", mood_udf("avg_speed", "weather", "sentiment"))


def write_to_mongo(df, batch_id):
    print(f"[BATCH {batch_id}] Row count: {df.count()}")
    df.printSchema()
    df.show(5, truncate=False)

    data = df.na.drop().toPandas().to_dict("records")
    print(f"[BATCH {batch_id}] Writing {len(data)} records to MongoDB")
    if data:
        client = MongoClient("mongodb://mongo:27017/")
        db = client["city_mood"]
        db["mood_events"].insert_many(data)
        client.close()


result.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
