from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import json
import os


# Since DateTime type is not JSON Serilaizable we will make it string
def convert_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def export_mongo_to_file():
    print("Starting Mongo export...")  # <-- This should appear in logs
    client = MongoClient("mongodb://mongo:27017/")
    db = client["city_mood"]
    collection = db["mood_events"]
    records = list(collection.find({}, {"_id": 0}))
    print(f"Exporting {len(records)} records...")  # <-- This too

    os.makedirs("/opt/airflow/mounted_exports", exist_ok=True)
    with open("/opt/airflow/mounted_exports/mood_export.json", "w") as f:
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
