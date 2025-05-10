from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


from jobs.stream_utils.utils import write_to_mongo_factory
from stream_utils.utils import read_kafka_stream
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


write_to_mongo = write_to_mongo_factory("news_events", log_prefix="NEWS")


news_labeled.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
