from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql.functions import col, to_timestamp
from pymongo import MongoClient
import json
import os
from pyspark.sql import SparkSession
import boto3
from my_airflow.utils.slack import notify_slack_failure

def cleanup_mongo_db(export_path="/opt/airflow/mounted_exports/mood_export.json"):
    print("Cleaning up MongoDB and local file...")

    client = MongoClient("mongodb://mongo:27017/")
    db = client["city_mood"]
    db["mood_events"].delete_many({})
    client.close()
    print("MongoDB cleaned.")

    try:
        os.remove(export_path)
        print(f"Deleted file: {export_path}")
    except FileNotFoundError:
        print(f"File not found: {export_path}")


def upload_to_s3():
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name="us-east-1"
    )

    bucket_name = "city-mood-storage"
    file_path = "/opt/airflow/mounted_exports/mood_export.json"
    date = datetime.utcnow().strftime("%Y-%m-%d")
    s3_key = f"exports/mood_export_{date}.json"

    s3.upload_file(file_path, bucket_name, s3_key)
    print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")


def load_to_postgres():
    spark = SparkSession.builder \
        .appName("Mood Data Validation") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
        .getOrCreate()

    df = spark.read.json("/opt/airflow/mounted_exports/mood_export.json")

    if "_corrupt_record" in df.columns:
        df = df.filter("_corrupt_record IS NULL")

    if df.count() == 0:
        print("No valid data to process.")
        return

    valid_df = df.filter(
        (df.event_time.isNotNull()) &
        (df.intersection.isNotNull()) &
        (df.weather.isNotNull()) &
        (df.avg_speed > 0)
    )

    valid_df = valid_df.withColumn("event_time", to_timestamp(col("event_time")))

    print(f"Inserting {valid_df.count()} valid records...")

    valid_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow") \
        .option("dbtable", "mood_events") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print(f"Inserted {valid_df.count()} records into PostgreSQL.")
    spark.stop()


def convert_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def export_mongo_to_file(export_path="/opt/airflow/mounted_exports/mood_export.json"):
    print("Starting Mongo export...")
    client = MongoClient("mongodb://mongo:27017/")
    db = client["city_mood"]
    collection = db["mood_events"]
    records = list(collection.find({}, {"_id": 0}))
    print(f"Exporting {len(records)} records...")

    export_dir = os.path.dirname(export_path)
    os.makedirs(export_dir, exist_ok=True)

    with open(export_path, "w") as f:
        for record in records:
            f.write(json.dumps(record, default=convert_datetime) + "\n")

    print("Export finished.")
    client.close()


default_args = {
    "start_date": datetime(2025, 4, 22),
    "catchup": False
}


with DAG(
        "mongo_to_storage",
        schedule_interval="@daily",
        default_args=default_args,
        description="Export mood data from MongoDB to file (then to S3/PostgreSQL)",
        tags=["mood-tracker"],
        on_failure_callback=notify_slack_failure
) as dag:
    export_task = PythonOperator(
        task_id="export_mongo_to_file",
        python_callable=export_mongo_to_file
    )

    load_postgres_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )

    upload_to_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_mongo_and_file",
        python_callable=cleanup_mongo_db
    )


    export_task >> load_postgres_task
    export_task >> upload_to_s3_task
    [upload_to_s3_task, load_postgres_task] >> cleanup_task




