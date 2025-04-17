from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sample_news = [
    {"headline": "Massive protest in city center causes traffic delays.", "sentiment": "negative"},
    {"headline": "Sunny day brings more people outside.", "sentiment": "positive"},
    {"headline": "Police report a smooth commute this morning.", "sentiment": "positive"},
    {"headline": "Heavy rain expected later today.", "sentiment": "neutral"},
    {"headline": "Accident reported near Tumanyan intersection.", "sentiment": "negative"},
    {"headline": "Public transport strike continues for 3rd day.", "sentiment": "negative"},
    {"headline": "Festival on Mashtots draws large crowds.", "sentiment": "positive"}
]


while True:
    timestamp = datetime.utcnow().replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    selected = random.choice(sample_news)
    news_event = {
        "timestamp": timestamp,
        "headline": random.choice(sample_news),
        "sentiment": selected["sentiment"]
    }
    producer.send("news", value=news_event)
    print("Sent:", news_event)
    time.sleep(1)  # simulate real-time delivery
