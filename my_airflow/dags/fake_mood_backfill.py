from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import random


def generate_fake_mood_data():
    client = MongoClient("mongodb://mongo:27017/")
    db = client["city_mood"]
    collection = db["mood_events"]

    intersections = ["komitas", "mashtots", "barekamutyun", "kentron"]
    weather_options = ["clear", "rain", "fog", "cloudy"]
    sentiments = ["positive", "neutral", "negative"]
    moods = {
        "positive": "happy",
        "neutral": "neutral",
        "negative": "stressed"
    }

    for i in range(7):
        day = datetime.utcnow() - timedelta(days=i)
        for _ in range(random.randint(10, 50)):
            sentiment = random.choice(sentiments)
            doc = {
                "event_time": day.replace(hour=random.randint(6, 23), minute=random.randint(0, 59)),
                "intersection": random.choice(intersections),
                "avg_speed": round(random.uniform(20.0, 80.0), 1),
                "avg_temp": random.randint(-5, 35),
                "weather": random.choice(weather_options),
                "sentiment": sentiment,
                "mood": moods[sentiment]
            }
            collection.insert_one(doc)

    print("Inserted fake mood data for past 7 days.")
    client.close()


default_args = {
    "start_date": datetime(2025, 5, 1),
    "catchup": False
}

with DAG(
    "backfill_fake_mood_data",
    schedule_interval=None,
    default_args=default_args,
    description="Insert fake mood data for past 7 days into MongoDB",
    tags=["mood-tracker", "backfill"]
) as dag:

    insert_data = PythonOperator(
        task_id="insert_fake_mood_data",
        python_callable=generate_fake_mood_data
    )
