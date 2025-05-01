import os
import json
import pytest
from unittest.mock import patch, MagicMock, mock_open
from my_airflow.dags.mongo_to_storage import (
    export_mongo_to_file,
    load_to_postgres,
    upload_to_s3,
    cleanup_mongo_db,
)

TEST_EXPORT_PATH = "./my_airflow/mounted_exports/mood_export_test.json"


class TestMongoToStorageDAG:
    @pytest.fixture(autouse=True)
    def cleanup_file(self):
        """Automatically cleanup the test file before and after each test."""
        if os.path.exists(TEST_EXPORT_PATH):
            os.remove(TEST_EXPORT_PATH)
        yield
        if os.path.exists(TEST_EXPORT_PATH):
            os.remove(TEST_EXPORT_PATH)

    @patch("my_airflow.dags.mongo_to_storage.MongoClient")
    def test_export_creates_file_with_data(self, mock_mongo):
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
            data = [json.loads(line) for line in f]
            assert isinstance(data, list)
            assert data[0]["intersection"] == "komitas"

    @patch("my_airflow.dags.mongo_to_storage.MongoClient")
    def test_export_creates_empty_file_when_no_data(self, mock_mongo):
        mock_collection = MagicMock()
        mock_collection.find.return_value = []
        mock_db = {"mood_events": mock_collection}
        mock_mongo.return_value.__getitem__.side_effect = lambda name: mock_db

        export_mongo_to_file(export_path=TEST_EXPORT_PATH)

        assert os.path.exists(TEST_EXPORT_PATH)
        with open(TEST_EXPORT_PATH) as f:
            data = [json.loads(line) for line in f]
            assert data == []

    @patch("my_airflow.dags.mongo_to_storage.boto3.client")
    @patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"})
    def test_upload_file_to_s3(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        upload_to_s3()

        mock_s3.upload_file.assert_called_once()
        args, kwargs = mock_s3.upload_file.call_args
        assert args[0] == "/opt/airflow/mounted_exports/mood_export.json"

    @patch("my_airflow.dags.mongo_to_storage.MongoClient")
    @patch("os.remove")
    def test_cleanup_mongo_and_local_file(self, mock_remove, mock_mongo):
        mock_db = MagicMock()
        mock_mongo.return_value.__getitem__.return_value = mock_db

        cleanup_mongo_db(export_path=TEST_EXPORT_PATH)

        mock_db["mood_events"].delete_many.assert_called_once_with({})
        mock_remove.assert_called_once_with(TEST_EXPORT_PATH)
