"""Test logging."""

from datetime import datetime, timedelta
from pathlib import Path

from hbnmigration.utility_functions.logging import (
    _invalid_fields_logger,
    invalid_fields_log_dir,
    log_invalid_fields,
)


def test_log_invalid_fields_creates_log_file(tmp_path, monkeypatch):
    """Test that log_invalid_fields creates daily log file."""
    # Set the log root BEFORE clearing cache
    monkeypatch.setattr(
        "hbnmigration.utility_functions.logging.log_root_path", lambda: tmp_path
    )

    # Clear cache to ensure fresh test
    _invalid_fields_logger.clear_cache()
    _invalid_fields_logger._log_path = None
    _invalid_fields_logger._current_date = None

    invalid_fields = ["field1", "field2"]
    new_fields = log_invalid_fields(invalid_fields)

    assert new_fields == invalid_fields
    today = datetime.now().strftime("%Y-%m-%d")
    log_dir = tmp_path / "invalid_fields"
    log_file = log_dir / f"{today}.txt"
    assert log_file.exists()

    content = log_file.read_text()
    assert "field1" in content
    assert "field2" in content


def test_log_invalid_fields_avoids_duplicates(tmp_path, monkeypatch):
    """Test that duplicate fields are not logged twice."""
    monkeypatch.setattr(
        "hbnmigration.utility_functions.logging.log_root_path", lambda: tmp_path
    )
    _invalid_fields_logger.clear_cache()
    _invalid_fields_logger._log_path = None
    _invalid_fields_logger._current_date = None

    # Log same fields twice
    first_result = log_invalid_fields(["field1", "field2"])
    second_result = log_invalid_fields(["field1", "field2"])

    assert first_result == ["field1", "field2"]
    assert second_result == []  # Already logged


def test_log_invalid_fields_partial_duplicates(tmp_path, monkeypatch):
    """Test logging with partial duplicates."""
    monkeypatch.setattr(
        "hbnmigration.utility_functions.logging.log_root_path", lambda: tmp_path
    )
    _invalid_fields_logger.clear_cache()
    _invalid_fields_logger._log_path = None
    _invalid_fields_logger._current_date = None

    first_result = log_invalid_fields(["field1", "field2"])
    second_result = log_invalid_fields(["field2", "field3"])

    assert first_result == ["field1", "field2"]
    assert second_result == ["field3"]


def test_log_invalid_fields_cleanup_old_logs(tmp_path, monkeypatch):
    """Test that logs older than 7 days are deleted."""
    log_dir = tmp_path / "invalid_fields"
    log_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "hbnmigration.utility_functions.logging.log_root_path", lambda: tmp_path
    )

    # Create old log file
    old_date = (datetime.now() - timedelta(days=8)).strftime("%Y-%m-%d")
    old_log = log_dir / f"{old_date}.txt"
    old_log.write_text("old_field\n")

    # Create recent log
    recent_date = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    recent_log = log_dir / f"{recent_date}.txt"
    recent_log.write_text("recent_field\n")

    _invalid_fields_logger.clear_cache()
    _invalid_fields_logger._log_path = None
    _invalid_fields_logger._current_date = None

    log_invalid_fields(["new_field"])

    # Old log should be deleted, recent kept
    assert not old_log.exists()
    assert recent_log.exists()


def test_log_invalid_fields_empty_list(tmp_path, monkeypatch):
    """Test logging with empty list."""
    monkeypatch.setattr(
        "hbnmigration.utility_functions.logging.log_root_path", lambda: tmp_path
    )
    _invalid_fields_logger.clear_cache()
    _invalid_fields_logger._log_path = None
    _invalid_fields_logger._current_date = None

    result = log_invalid_fields([])
    assert result == []


def test_invalid_fields_log_dir_returns_path():
    """Test that invalid_fields_log_dir returns correct path."""
    result = invalid_fields_log_dir()
    assert isinstance(result, Path)
    assert result.name == "invalid_fields"
