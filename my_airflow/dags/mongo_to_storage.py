from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import json
import os
import psycopg2


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
    "start_date": datetime(2025, 3, 25),
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

    export_task >> load_postgres_task


