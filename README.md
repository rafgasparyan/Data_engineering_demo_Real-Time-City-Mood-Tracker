
# City Mood Tracker

## Description

City Mood Tracker is a real-time data processing pipeline designed to monitor and analyze the mood of a city based on
various data sources, including traffic conditions, weather, and news. It uses technologies like Apache Kafka,
Apache Spark, MongoDB, PostgreSQL, and S3 for efficient data processing and storage.

This project is aimed at providing insights into the general mood of a city, identifying trends, and delivering 
actionable information to stakeholders, such as city planners and emergency responders.

## Features

- **Real-time mood tracking**
- **Weather and traffic data integration**
- **Sentiment analysis on news**
- **Daily reports and heatmaps**
- **Export to PostgreSQL and S3**
- **GeoJSON map-ready output**

## Architecture

- Apache Kafka
- Apache Spark
- MongoDB
- PostgreSQL
- Amazon S3
- Airflow

## Installation

### Prerequisites

- Docker
- Python 3.9+
- PostgreSQL
- MongoDB
- Apache Kafka + Zookeeper
- Apache Spark
- AWS credentials (optional)

### Steps to Run

```bash
git clone https://github.com/rafgasparyan/Data_engineering_demo_Real-Time-City-Mood-Tracker
cd city-mood-tracker
docker-compose up -d
python -m venv newenv
source newenv/bin/activate
pip install -r requirements.txt
```

Create a `.env` file:

```
MONGO_URI=mongodb://mongo:27017
DB_HOST=postgres
DB_PORT=5432
DB_NAME=airflow
DB_USER=airflow
DB_PASSWORD=airflow
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
```

Visit `http://localhost:8080` to access Airflow UI and trigger DAGs.

## Usage

### Run Order (Local Development)

1. **Start Docker environment**  
   ```bash
   docker-compose up -d
   ```

2. **Run data producers**  
   These simulate real-time city data and send messages to Kafka:

   ```bash
   python3 news_producer.py
   python3 traffic_producer.py
   python3 weather_producer.py
   ```
   
3. **Run Spark mood tracker job**  
   This Spark job consumes the Kafka messages and writes mood data to MongoDB:

   ```bash
   docker exec -it spark-master spark-submit      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0      /opt/bitnami/spark/jobs/spark_mood_tracker.py
   ```

4. **Trigger DAGs in Airflow UI**  
   Open [http://localhost:8080](http://localhost:8080) and manually trigger:
   - `mood_quality_check`: Validates MongoDB mood data
   - `mongo_to_storage`: Extracts, transforms, and stores mood data
   - `daily_summary_report`: Produces daily report about mood
   - `fake_mood_backfill`: Generates fake data for testing 


### Airflow DAGs
- `mood_quality_check`: Validates MongoDB mood data
- `mongo_to_storage`: Extracts, transforms, and stores mood data
- `daily_summary_report`: Produces daily report about mood
- `fake_mood_backfill`: Generates fake data for testing 

