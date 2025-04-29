from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def check_mongo_data():
    client = MongoClient("mongodb://mongo:27017/")
    db = client["city_mood"]
    collection = db["mood_events"]

    count = collection.count_documents({})
    if count == 0:
        raise ValueError("No mood data found in MongoDB!")

    print(f"Found {count} records in MongoDB.")
    client.close()


def check_required_fields():
    client = MongoClient("mongodb://mongo:27017/")
    db = client["city_mood"]
    collection = db["mood_events"]

    sample = collection.find_one({
        "$or": [
            {"event_time": {"$exists": False}},
            {"intersection": {"$exists": False}},
            {"weather": {"$exists": False}},
            {"avg_speed": {"$exists": False}}
        ]
    })

    if sample:
        raise ValueError(f"Found record with missing fields: {sample}")

    print("All required fields exist in MongoDB records.")
    client.close()


default_args = {
    "start_date": datetime(2025, 4, 28),
    "catchup": False
}

with DAG(
    "mood_quality_check",
    schedule_interval="@daily",
    default_args=default_args,
    description="Check mood data quality before exporting",
    tags=["mood-tracker", "quality"],
) as dag:

    check_data_task = PythonOperator(
        task_id="check_data_exists",
        python_callable=check_mongo_data
    )

    check_fields_task = PythonOperator(
        task_id="check_required_fields",
        python_callable=check_required_fields
    )

    trigger_export_dag = TriggerDagRunOperator(
        task_id="trigger_mongo_to_storage_dag",
        trigger_dag_id="mongo_to_storage"
    )

    check_data_task >> check_fields_task >> trigger_export_dag
