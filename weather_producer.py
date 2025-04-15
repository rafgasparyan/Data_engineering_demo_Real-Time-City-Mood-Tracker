import json, time, requests
from kafka import KafkaProducer

URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=40.18&longitude=44.51"
    "&current_weather=true"
)

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


WEATHER_CODE_MAP = {
    0: "clear",
    1: "mainly_clear",
    2: "partly_cloudy",
    3: "overcast",
    45: "fog",
    48: "depositing_rime_fog",
    51: "drizzle_light",
    53: "drizzle_moderate",
    55: "drizzle_dense",
    61: "rain_slight",
    63: "rain_moderate",
    65: "rain_heavy",
    71: "snow_slight",
    73: "snow_moderate",
    75: "snow_heavy",
    95: "thunderstorm",
    96: "thunderstorm_with_hail",
}

while True:
    try:
        r = requests.get(URL, timeout=10)
        r.raise_for_status()
        data = r.json()
        cw = data["current_weather"]
        payload = {
            "timestamp": cw["time"],
            "temp": cw["temperature"],
            "windspeed": cw["windspeed"],
            "weather": WEATHER_CODE_MAP.get(cw["weathercode"], "unknown")
        }
        producer.send("weather", value=payload)
        print("Sent:", payload)
    except Exception as e:
        print("Weather API error:", e)
        time.sleep(10)
        continue

    time.sleep(30)
