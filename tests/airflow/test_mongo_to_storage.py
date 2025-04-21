import os
import json
import pytest
from unittest.mock import patch, MagicMock
from my_airflow.dags.mongo_to_storage import export_mongo_to_file

TEST_EXPORT_PATH = "./my_airflow/mounted_exports/mood_export_test.json"


class TestExportMongoToFile:
    @pytest.fixture(autouse=True)
    def cleanup_file(self):
        if os.path.exists(TEST_EXPORT_PATH):
            os.remove(TEST_EXPORT_PATH)
        yield
        if os.path.exists(TEST_EXPORT_PATH):
            os.remove(TEST_EXPORT_PATH)

    @patch("my_airflow.dags.mongo_to_storage.MongoClient")
    def test_creates_file_with_data(self, mock_mongo):
        mock_collection = MagicMock()
        mock_collection.find.return_value = [{
            "event_time": "2025-04-19T16:10:00",
            "intersection": "komitas",
            "avg_speed": 42.0,
            "avg_temp": 15.0,
            "weather": "clear",
            "sentiment": "positive",
            "mood": "relaxed"
        }]
        mock_db = {"mood_events": mock_collection}
        mock_mongo.return_value.__getitem__.side_effect = lambda name: mock_db

        export_mongo_to_file(export_path=TEST_EXPORT_PATH)

        assert os.path.exists(TEST_EXPORT_PATH)
        with open(TEST_EXPORT_PATH) as f:
            data = json.load(f)
            assert isinstance(data, list)
            assert data[0]["intersection"] == "komitas"

    @patch("my_airflow.dags.mongo_to_storage.MongoClient")
    def test_creates_file_even_if_empty(self, mock_mongo):
        mock_collection = MagicMock()
        mock_collection.find.return_value = []
        mock_db = {"mood_events": mock_collection}
        mock_mongo.return_value.__getitem__.side_effect = lambda name: mock_db

        export_mongo_to_file(export_path=TEST_EXPORT_PATH)

        assert os.path.exists(TEST_EXPORT_PATH)
        with open(TEST_EXPORT_PATH) as f:
            data = json.load(f)
            assert data == []
