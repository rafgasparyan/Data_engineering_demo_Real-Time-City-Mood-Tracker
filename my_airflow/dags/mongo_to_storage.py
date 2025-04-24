from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import json
import os
import psycopg2
import boto3


def cleanup_mongo_db(export_path="/opt/airflow/mounted_exports/mood_export.json"):
    print("Cleaning up MongoDB and local file...")

    # Remove MongoDB data
    client = MongoClient("mongodb://mongo:27017/")
    db = client["city_mood"]
    db["mood_events"].delete_many({})
    client.close()
    print("MongoDB cleaned.")

    # Remove exported file
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
    with open("/opt/airflow/mounted_exports/mood_export.json") as f:
        records = json.load(f)

    if not records:
        print("No data to insert.")
        return

    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mood_events (
            event_time TIMESTAMP,
            intersection TEXT,
            avg_speed FLOAT,
            avg_temp FLOAT,
            weather TEXT,
            sentiment TEXT,
            mood TEXT
        )
    """)

    for record in records:
        cur.execute("""
            INSERT INTO mood_events (event_time, intersection, avg_speed, avg_temp, weather, sentiment, mood)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            record["event_time"],
            record["intersection"],
            record["avg_speed"],
            record["avg_temp"],
            record["weather"],
            record["sentiment"],
            record["mood"]
        ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {len(records)} records into PostgreSQL.")


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
        json.dump(records, f, indent=2, default=convert_datetime)

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
