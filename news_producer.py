from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sample_news = [
    "Massive protest in city center causes traffic delays.",
    "Sunny day brings more people outside.",
    "Police report a smooth commute this morning.",
    "Heavy rain expected later today.",
    "Accident reported near Tumanyan intersection.",
    "Public transport strike continues for 3rd day.",
    "Festival on Mashtots draws large crowds."
]

while True:
    news_event = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "headline": random.choice(sample_news)
    }
    producer.send("news", value=news_event)
    print("Sent:", news_event)
    time.sleep(1)  # simulate real-time delivery
