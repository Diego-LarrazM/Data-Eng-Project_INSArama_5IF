import unittest
import sys
import tempfile
import csv
import json
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, call
import os


# ============================================================
# AJOUT DU CHEMIN RACINE DU PROJET
# ============================================================

# Ajouter la racine du projet
ROOT = Path(__file__).resolve().parents[1]  # remonte depuis /tests/
sys.path.insert(0, str(ROOT))

# ============================================================
# IMPORT ABSOLU PROPRE
# ============================================================

from DockerETL_Images.Ingestion.IMDBCurler.scripts.mongo_loader import MongoLoader
from DockerETL_Images.Ingestion.IMDBCurler.scripts.utils.execution import (
    SUCCESS,
    FAILURE,
)


class TestMongoLoaderFilterFunc(unittest.TestCase):
    """Test cases for MongoLoader._make_filter_func"""

    def test_no_filter_columns_no_filter_values(self):
        """Test filter function with no filters applied"""
        filter_func = MongoLoader._make_filter_func(None, None)
        row = {"id": 1, "name": "Alice"}

        result = filter_func(row)
        self.assertTrue(result)

    # Should keep the row
    def test_remove_filter_columns(self):
        """Test that filter_columns are removed from rows"""
        filter_func = MongoLoader._make_filter_func(["temp_col"], None)
        row = {"id": 1, "name": "Alice", "temp_col": "remove_me"}

        filter_func(row)

        self.assertNotIn("temp_col", row)
        self.assertIn("id", row)
        self.assertIn("name", row)

    def test_remove_multiple_filter_columns(self):
        """Test removing multiple filter columns"""
        filter_func = MongoLoader._make_filter_func(["temp1", "temp2"], None)
        row = {"id": 1, "temp1": "x", "temp2": "y"}

        filter_func(row)

        self.assertNotIn("temp1", row)
        self.assertNotIn("temp2", row)
        self.assertIn("id", row)

    def test_filter_by_single_value(self):
        """Test filtering by a single value"""
        filter_func = MongoLoader._make_filter_func(None, [{"language": "en"}])

        row_en = {"language": "en", "title": "Hello"}
        row_fr = {"language": "fr", "title": "Bonjour"}

        self.assertTrue(filter_func(row_en))  # Keep row
        self.assertFalse(filter_func(row_fr))  # Skip row

    def test_filter_by_multiple_values_in_list(self):
        """Test filtering with multiple allowed values"""
        filter_func = MongoLoader._make_filter_func(None, [{"language": {"en", "fr"}}])

        row_en = {"language": "en"}
        row_fr = {"language": "fr"}
        row_es = {"language": "es"}

        self.assertTrue(filter_func(row_en))  # Keep
        self.assertTrue(filter_func(row_fr))  # Keep
        self.assertFalse(filter_func(row_es))  # Skip

    def test_filter_multiple_conditions_or_logic(self):
        """Test multiple filter conditions with OR logic"""
        filter_values_list = [{"language": "en"}, {"language": "fr"}]
        filter_func = MongoLoader._make_filter_func(None, filter_values_list)

        row_en = {"language": "en"}
        row_fr = {"language": "fr"}
        row_es = {"language": "es"}

        self.assertTrue(filter_func(row_en))  # Keep
        self.assertTrue(filter_func(row_fr))  # Keep
        self.assertFalse(filter_func(row_es))  # Skip

    def test_filter_missing_column(self):
        """Test filtering when required column doesn't exist"""
        filter_func = MongoLoader._make_filter_func(None, [{"language": "en"}])
        row = {"title": "No language field"}

        result = filter_func(row)

        self.assertFalse(result)  # Should skip row

    def test_filter_multiple_columns_and_logic(self):
        """Test filtering with multiple columns (AND logic within one filter dict)"""
        filter_values_list = [{"language": "en", "isOriginal": "1"}]
        filter_func = MongoLoader._make_filter_func(None, filter_values_list)

        row_match = {"language": "en", "isOriginal": "1"}
        row_no_lang = {"language": "fr", "isOriginal": "1"}
        row_no_original = {"language": "en", "isOriginal": "0"}

        self.assertTrue(filter_func(row_match))  # Keep
        self.assertFalse(filter_func(row_no_lang))  # Skip
        self.assertFalse(filter_func(row_no_original))  # Skip

    def test_filter_columns_and_values_together(self):
        """Test removing columns and filtering values simultaneously"""
        filter_func = MongoLoader._make_filter_func(["temp_col"], [{"language": "en"}])
        row = {"language": "en", "title": "Hello", "temp_col": "remove"}

        filter_func(row)

        self.assertNotIn("temp_col", row)
        self.assertTrue(filter_func(row))  # Row should be kept


class TestMongoLoaderCSV(unittest.TestCase):
    """Test cases for MongoLoader.load_from_csv"""

    @patch("DockerETL_Images.Ingestion.IMDBCurler.scripts.mongo_loader.MongoClient")
    def setUp(self, mock_mongo_client):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.mock_db = MagicMock()
        self.mock_collection = MagicMock()

        mock_mongo_client.return_value = self.mock_client
        self.mock_client.__getitem__.return_value = self.mock_db
        self.mock_db.__getitem__.return_value = self.mock_collection
        self.mock_client.start_session.return_value.__enter__ = MagicMock(
            return_value=MagicMock()
        )
        self.mock_client.start_session.return_value.__exit__ = MagicMock(
            return_value=None
        )

        self.loader = MongoLoader("mongodb://localhost:27017/", "test_db")

    def test_load_from_json_file_not_found(self):
        result = self.loader.load_from_json("/nonexistent/file.json")
        self.assertEqual(result, FAILURE)  # ou le code correspondant au safe_execute

    def test_load_from_csv_basic(self):
        """Test loading CSV with basic data"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name"], delimiter=";")
            writer.writeheader()
            writer.writerow({"id": "1", "name": "Alice"})
            writer.writerow({"id": "2", "name": "Bob"})
            temp_file = f.name

        try:
            self.loader.load_from_csv(temp_file, "test_collection", batch_size=2)

            # Verify insert_many was called
            self.mock_collection.insert_many.assert_called()
        finally:
            os.unlink(temp_file)

    def test_load_from_csv_with_filtering(self):
        """Test loading CSV with value filtering"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=["id", "status"], delimiter=";")
            writer.writeheader()
            writer.writerow({"id": "1", "status": "active"})
            writer.writerow({"id": "2", "status": "inactive"})
            writer.writerow({"id": "3", "status": "active"})
            temp_file = f.name

        try:
            self.loader.load_from_csv(
                temp_file,
                "test_collection",
                filter_values_list=[{"status": "active"}],
                batch_size=10,
            )

            # Verify insert_many was called with filtered data
            self.mock_collection.insert_many.assert_called()
            call_args = self.mock_collection.insert_many.call_args
            # Should only have 2 rows (the active ones)
            self.assertEqual(len(call_args[0][0]), 2)
        finally:
            os.unlink(temp_file)

    def test_load_from_csv_with_column_removal(self):
        """Test loading CSV with column removal"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "temp"], delimiter=";")
            writer.writeheader()
            writer.writerow({"id": "1", "name": "Alice", "temp": "x"})
            temp_file = f.name

        try:
            self.loader.load_from_csv(
                temp_file, "test_collection", filter_columns=["temp"], batch_size=10
            )

            # Verify insert_many was called
            call_args = self.mock_collection.insert_many.call_args
            inserted_data = call_args[0][0][0]
            self.assertNotIn("temp", inserted_data)
            self.assertIn("id", inserted_data)
        finally:
            os.unlink(temp_file)

    def test_load_from_csv_batching(self):
        """Test that CSV data is batched correctly"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name"], delimiter=";")
            writer.writeheader()
            for i in range(5):
                writer.writerow({"id": str(i), "name": f"Person{i}"})
            temp_file = f.name

        try:
            self.loader.load_from_csv(temp_file, "test_collection", batch_size=2)

            # Verify insert_many was called multiple times with batches of size 2
            self.assertEqual(self.mock_collection.insert_many.call_count, 3)

            # Check batch sizes
            calls = self.mock_collection.insert_many.call_args_list
            self.assertEqual(len(calls[0][0][0]), 2)  # First batch
            self.assertEqual(len(calls[1][0][0]), 2)  # Second batch
            self.assertEqual(len(calls[2][0][0]), 1)  # Last batch
        finally:
            os.unlink(temp_file)


class TestMongoLoaderSingleMultiple(unittest.TestCase):
    """Test cases for MongoLoader.load_single and load_multiple"""

    @patch("DockerETL_Images.Ingestion.IMDBCurler.scripts.mongo_loader.MongoClient")
    def setUp(self, mock_mongo_client):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.mock_db = MagicMock()
        self.mock_collection = MagicMock()

        mock_mongo_client.return_value = self.mock_client
        self.mock_client.__getitem__.return_value = self.mock_db
        self.mock_db.__getitem__.return_value = self.mock_collection
        self.mock_client.start_session.return_value.__enter__ = MagicMock(
            return_value=MagicMock()
        )
        self.mock_client.start_session.return_value.__exit__ = MagicMock(
            return_value=None
        )

        self.loader = MongoLoader("mongodb://localhost:27017/", "test_db")

    def test_load_single_success(self):
        """Test loading a single document"""
        data = {"id": 1, "name": "Alice"}
        self.loader.load_single(data, "test_collection")

        self.mock_collection.insert_one.assert_called()

    def test_load_multiple_success(self):
        """Test loading multiple documents"""
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        self.loader.load_multiple(data, "test_collection")

        self.mock_collection.insert_many.assert_called_with(
            data, session=unittest.mock.ANY
        )


if __name__ == "__main__":
    unittest.main()
