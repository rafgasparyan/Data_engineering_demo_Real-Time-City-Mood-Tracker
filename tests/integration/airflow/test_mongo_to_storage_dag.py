"""
Integration test for the mongo_to_storage DAG.

🚨 WARNING:
This test triggers a real DAG and interacts with real MongoDB and Postgres instances.
It should NOT be run in production environments or shared CI pipelines unless isolated test databases are used.

✅ Usage:
- Run manually before merging major DAG changes
- Ensure MongoDB contains only test data (or clean it up before/after test)
- Expected test record: intersection = "ci_test_case"

✅ Future Plan:
To safely run this in CI, set up dedicated test containers (e.g. test MongoDB & Postgres)
and point this test to them via environment variables (MONGO_HOST, DB_HOST, etc).

To run locally:
    pytest tests/integration/test_mongo_to_storage_dag.py
"""

import os
import time
import uuid
import psycopg2
import subprocess
from pymongo import MongoClient
from datetime import datetime


def insert_test_data_to_mongo():
    mongo_host = os.getenv("MONGO_HOST", "localhost")
    client = MongoClient(f"mongodb://{mongo_host}:27017")
    db = client["city_mood"]
    collection = db["mood_events"]
    collection.insert_one({
        "event_time": datetime.utcnow().isoformat(),
        "intersection": "ci_test_case",
        "avg_speed": 42.0,
        "avg_temp": 16.0,
        "weather": "clear",
        "sentiment": "positive",
        "mood": "happy"
    })


def trigger_dag(dag_id: str, run_id: str):
    subprocess.run([
        "docker", "exec", "airflow-webserver",
        "airflow", "dags", "trigger", dag_id,
        "--run-id", run_id
    ], check=True)


def wait_for_dag_completion(dag_id: str, run_id: str, timeout=60):
    start_time = time.time()
    while True:
        result = subprocess.run(
            ["docker", "exec", "airflow-webserver", "airflow", "dags", "list-runs", "-d", dag_id],
            capture_output=True,
            text=True
        )
        output = result.stdout.strip().lower()

        for line in output.splitlines():
            if run_id.lower() in line:
                if "success" in line:
                    return True
                elif "failed" in line:
                    return False

        if time.time() - start_time > timeout:
            print("Timeout reached.")
            return False
        time.sleep(5)


def test_dag_inserts_data_to_postgres():
    insert_test_data_to_mongo()

    dag_id = "mongo_to_storage"
    run_id = f"test_run_{uuid.uuid4()}"

    trigger_dag(dag_id, run_id)
    assert wait_for_dag_completion(dag_id, run_id), "DAG run did not complete successfully"

    for _ in range(10):
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=int(os.getenv("DB_PORT", 5433)),
                database=os.getenv("DB_NAME", "airflow"),
                user=os.getenv("DB_USER", "airflow"),
                password=os.getenv("DB_PASSWORD", "airflow")
            )
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*) FROM mood_events
                WHERE intersection = 'ci_test_case'
            """)
            row_count = cur.fetchone()[0]
            conn.close()

            print(f"Found {row_count} rows in mood_events")
            if row_count > 0:
                break
        except Exception as e:
            print(f"DB query failed: {e}")
        time.sleep(2)
    else:
        raise AssertionError("No data inserted into PostgreSQL by DAG")
