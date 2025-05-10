from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, udf
from pyspark.sql.types import StructType, StringType, TimestampType
from pymongo import MongoClient
from stream_utils.kafka_reader import read_kafka_stream
from stream_utils.schemas import news_schema

spark = SparkSession.builder \
    .appName("NewsConsumer") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

news = read_kafka_stream(spark, "news", news_schema, "n", [
    "to_timestamp(n.timestamp) as event_time", "n.headline"
])


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
