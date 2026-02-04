import pytest
import logging
from unittest.mock import Mock, patch, MagicMock
from src.extract.main_extract import load_client, run_extraction, setup_logging


class TestLoadClient:
    """Tests for the load_client function - error handling and logging."""

    def test_load_client_import_error_logs_error(self, caplog):
        """Test that ImportError is caught and logged."""
        with caplog.at_level(logging.ERROR):
            result = load_client("nonexistent.module", "SomeClass")

        assert result is None
        assert "Failed to load nonexistent.module.SomeClass" in caplog.text

    def test_load_client_attribute_error_logs_error(self, caplog):
        """Test that AttributeError is caught and logged when class doesn't exist."""
        with caplog.at_level(logging.ERROR):
            result = load_client("src.extract.main_extract", "NonExistentClass")

        assert result is None
        assert "Failed to load src.extract.main_extract.NonExistentClass" in caplog.text

    @patch("src.extract.main_extract.importlib.import_module")
    def test_load_client_success(self, mock_import):
        """Test successful client loading."""
        mock_class = Mock(return_value="client_instance")
        mock_module = Mock()
        mock_module.TestClient = mock_class
        mock_import.return_value = mock_module

        result = load_client("some.module", "TestClient")

        assert result == "client_instance"
        mock_import.assert_called_once_with("some.module")


class TestRunExtraction:
    """Tests for the run_extraction function - logging and error handling."""

    @patch("src.extract.main_extract.psycopg2.connect")
    @patch("src.extract.main_extract.WEB_SOURCES", [])
    def test_run_extraction_no_sources_logs_completion(self, mock_connect, caplog):
        """Test that extraction with no sources logs completion."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        with caplog.at_level(logging.INFO):
            run_extraction()

        assert "Extraction completed successfully" in caplog.text
        mock_conn.commit.assert_called_once()
        mock_conn.cursor().close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("src.extract.main_extract.psycopg2.connect")
    @patch("src.extract.main_extract.load_client")
    @patch("src.extract.main_extract.WEB_SOURCES", [
        {"name": "TestSource", "module": "test.module", "class": "TestClass", "table": "test_table"}
    ])
    def test_run_extraction_client_load_fails_continues(self, mock_load_client, mock_connect, caplog):
        """Test that extraction continues when client fails to load."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_load_client.return_value = None

        with caplog.at_level(logging.INFO):
            run_extraction()

        assert "Extraction completed successfully" in caplog.text

    @patch("src.extract.main_extract.psycopg2.connect")
    @patch("src.extract.main_extract.load_client")
    @patch("src.extract.main_extract.WEB_SOURCES", [
        {"name": "TestSource", "module": "test.module", "class": "TestClass", "table": "test_table"}
    ])
    def test_run_extraction_no_data_logs_warning(self, mock_load_client, mock_connect, caplog):
        """Test that warning is logged when no data is returned."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_client = Mock()
        mock_client.scrape_site.return_value = None
        mock_load_client.return_value = mock_client

        with caplog.at_level(logging.WARNING):
            run_extraction()

        assert "No data from TestSource" in caplog.text

    @patch("src.extract.main_extract.psycopg2.connect")
    @patch("src.extract.main_extract.load_client")
    @patch("src.extract.main_extract.WEB_SOURCES", [
        {"name": "TestSource", "module": "test.module", "class": "TestClass", "table": "test_table"}
    ])
    def test_run_extraction_empty_data_logs_warning(self, mock_load_client, mock_connect, caplog):
        """Test that warning is logged when empty list is returned."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_client = Mock()
        mock_client.scrape_site.return_value = []
        mock_load_client.return_value = mock_client

        with caplog.at_level(logging.WARNING):
            run_extraction()

        assert "No data from TestSource" in caplog.text

    @patch("src.extract.main_extract.psycopg2.connect")
    @patch("src.extract.main_extract.load_client")
    @patch("src.extract.main_extract.VALID_TABLES", {"valid_table"})
    @patch("src.extract.main_extract.WEB_SOURCES", [
        {"name": "TestSource", "module": "test.module", "class": "TestClass", "table": "invalid_table"}
    ])
    def test_run_extraction_invalid_table_logs_error(self, mock_load_client, mock_connect, caplog):
        """Test that error is logged for invalid table names."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_client = Mock()
        mock_client.scrape_site.return_value = [{"data": "test"}]
        mock_load_client.return_value = mock_client

        with caplog.at_level(logging.ERROR):
            run_extraction()

        assert "Invalid table name: invalid_table" in caplog.text

    @patch("src.extract.main_extract.psycopg2.connect")
    @patch("src.extract.main_extract.load_client")
    @patch("src.extract.main_extract.VALID_TABLES", {"test_table"})
    @patch("src.extract.main_extract.WEB_SOURCES", [
        {"name": "TestSource", "module": "test.module", "class": "TestClass", "table": "test_table"}
    ])
    def test_run_extraction_success_logs_info(self, mock_load_client, mock_connect, caplog):
        """Test that successful insertion logs info message."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        mock_client = Mock()
        mock_client.scrape_site.return_value = [{"key": "value1"}, {"key": "value2"}]
        mock_load_client.return_value = mock_client

        with caplog.at_level(logging.INFO):
            run_extraction()

        assert "Scraping TestSource..." in caplog.text
        assert "Inserted 2 rows into test_table" in caplog.text
        assert "Extraction completed successfully" in caplog.text

    @patch("src.extract.main_extract.psycopg2.connect")
    @patch("src.extract.main_extract.load_client")
    @patch("src.extract.main_extract.VALID_TABLES", {"test_table"})
    @patch("src.extract.main_extract.WEB_SOURCES", [
        {"name": "TestSource", "module": "test.module", "class": "TestClass", "table": "test_table"}
    ])
    def test_run_extraction_db_error_logs_and_raises(self, mock_load_client, mock_connect, caplog):
        """Test that database errors are logged and re-raised."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Database error")
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        mock_client = Mock()
        mock_client.scrape_site.return_value = [{"key": "value"}]
        mock_load_client.return_value = mock_client

        with caplog.at_level(logging.ERROR):
            with pytest.raises(Exception, match="Database error"):
                run_extraction()

        assert "Extraction failed: Database error" in caplog.text
        mock_conn.rollback.assert_called_once()

    @patch("src.extract.main_extract.psycopg2.connect")
    def test_run_extraction_connection_cleanup_on_success(self, mock_connect):
        """Test that database connections are properly closed on success."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        with patch("src.extract.main_extract.WEB_SOURCES", []):
            run_extraction()

        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("src.extract.main_extract.psycopg2.connect")
    @patch("src.extract.main_extract.load_client")
    @patch("src.extract.main_extract.VALID_TABLES", {"test_table"})
    @patch("src.extract.main_extract.WEB_SOURCES", [
        {"name": "TestSource", "module": "test.module", "class": "TestClass", "table": "test_table"}
    ])
    def test_run_extraction_connection_cleanup_on_error(self, mock_load_client, mock_connect):
        """Test that database connections are properly closed on error."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("DB Error")
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        mock_client = Mock()
        mock_client.scrape_site.return_value = [{"key": "value"}]
        mock_load_client.return_value = mock_client

        with pytest.raises(Exception):
            run_extraction()

        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()


class TestSetupLogging:
    """Tests for the setup_logging function."""

    @patch("src.extract.main_extract.os.makedirs")
    def test_setup_logging_creates_directory(self, mock_makedirs):
        """Test that setup_logging creates the logs directory."""
        setup_logging()
        mock_makedirs.assert_called_once_with("logs/extract", exist_ok=True)
