from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, udf
from pyspark.sql.types import StructType, StringType, TimestampType
from pymongo import MongoClient

news_schema = StructType() \
    .add("timestamp", StringType()) \
    .add("headline", StringType())

spark = SparkSession.builder \
    .appName("NewsConsumer") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

news_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "news") \
    .option("startingOffsets", "latest") \
    .load()

news = news_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), news_schema).alias("n")) \
    .selectExpr("to_timestamp(n.timestamp) as event_time", "n.headline")


def classify_news(headline):
    headline = headline.lower()
    if any(w in headline for w in ["accident", "strike", "crash", "delay", "protest"]):
        return "negative"
    elif any(w in headline for w in ["smooth", "sunny", "festival", "celebration"]):
        return "positive"
    else:
        return "neutral"

news_udf = udf(classify_news, StringType())
news_labeled = news.withColumn("sentiment", news_udf("headline"))

news_labeled.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

def write_to_mongo(df, batch_id):
    data = df.na.drop().toPandas().to_dict("records")
    print(f"[BATCH {batch_id}] Writing {len(data)} news records to MongoDB")
    if data:
        client = MongoClient("mongodb://mongo:27017/")
        db = client["city_mood"]
        db["news_events"].insert_many(data)
        client.close()

news_labeled.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
