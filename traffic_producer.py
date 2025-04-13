from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# main streets in Yerevan
intersections = ['north_avenue', 'mashtots', 'komitas', 'tumanyan']


def generate_traffic_data():
    return {
        'intersection': random.choice(intersections),
        'vehicle_id': f'veh-{random.randint(1000, 9999)}',
        'speed': round(random.uniform(10, 90), 2),
        'timestamp': time.time()
    }


while True:
    traffic_event = generate_traffic_data()
    producer.send('traffic', value=traffic_event)
    print(f"Sent: {traffic_event}")
    time.sleep(1)
