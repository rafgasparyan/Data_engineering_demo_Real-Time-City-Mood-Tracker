from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, avg, count
from my_airflow.utils.slack import notify_slack_failure

default_args = {
    "start_date": datetime(2025, 4, 28),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False
}


def summarize_mood_data():
    spark = SparkSession.builder \
        .appName("Mood Summary Report") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
        .getOrCreate()

    yesterday = datetime.utcnow().date()

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow") \
        .option("dbtable", "mood_events") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    df = df.withColumn("event_date", date_format("event_time", "yyyy-MM-dd"))
    df = df.filter(col("event_date") == str(yesterday))

    summary_df = df.groupBy("intersection", "mood").agg(
        count("*").alias("records_count"),
        avg("avg_speed").alias("avg_speed"),
        avg("avg_temp").alias("avg_temp")
    )

    summary_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow") \
        .option("dbtable", "mood_summary") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    spark.stop()


with DAG(
    dag_id="daily_summary_report",
    schedule_interval="0 1 * * *",  # Every day at 01:00 AM
    default_args=default_args,
    catchup=False,
    description="Summarize yesterday's mood data by intersection and mood",
    tags=["mood-tracker", "summary"],
    on_failure_callback=notify_slack_failure
) as dag:

    generate_summary = PythonOperator(
        task_id="summarize_mood_data",
        python_callable=summarize_mood_data
    )

    generate_summary
