from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import json
import os


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
