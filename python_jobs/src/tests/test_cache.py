"""Tests for cache utility functions."""

import logging

import polars as pl
import pytest

from hbnmigration.utility_functions import (
    add_cache_keys,
    compute_content_hash,
    compute_dataframe_hash,
    create_composite_cache_key,
    DataCache,
    filter_by_cache,
    log_cache_statistics,
)


class TestCompositeKeys:
    """Test composite cache key creation."""

    def test_create_composite_key_basic(self):
        """Test basic key creation."""
        result = create_composite_cache_key("12345", "3", True)
        assert result == "12345:3:1"

    def test_create_composite_key_strings(self):
        """Test with string components."""
        result = create_composite_cache_key("alert_123", "abc456")
        assert result == "alert_123:abc456"

    def test_create_composite_key_bool_conversion(self):
        """Test boolean conversion to int."""
        result = create_composite_cache_key("key", False)
        assert result == "key:0"

    def test_create_composite_key_mixed_types(self):
        """Test with mixed types including integers."""
        result = create_composite_cache_key("mrn", 12345, "hash123", True)
        assert result == "mrn:12345:hash123:1"


class TestContentHashing:
    """Test content hashing utilities."""

    def test_compute_content_hash_string(self):
        """Test hashing string content."""
        result = compute_content_hash("hello world")
        assert len(result) == 12
        assert isinstance(result, str)

    def test_compute_content_hash_bytes(self):
        """Test hashing byte content."""
        result = compute_content_hash(b"hello world", length=8)
        assert len(result) == 8

    def test_compute_content_hash_consistency(self):
        """Test hash consistency."""
        hash1 = compute_content_hash("test")
        hash2 = compute_content_hash("test")
        assert hash1 == hash2

    def test_compute_dataframe_hash(self):
        """Test DataFrame hashing."""
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
        result = compute_dataframe_hash(df)
        assert len(result) == 12

    def test_dataframe_hash_consistency(self):
        """Test DataFrame hash consistency."""
        df1 = pl.DataFrame({"a": [1, 2]})
        df2 = pl.DataFrame({"a": [1, 2]})
        assert compute_dataframe_hash(df1) == compute_dataframe_hash(df2)

    def test_dataframe_hash_different_data(self):
        """Test that different data produces different hashes."""
        df1 = pl.DataFrame({"a": [1, 2]})
        df2 = pl.DataFrame({"a": [3, 4]})
        assert compute_dataframe_hash(df1) != compute_dataframe_hash(df2)


class TestAddCacheKeys:
    """Test adding cache keys to DataFrames."""

    def test_add_cache_keys_basic(self):
        """Test basic cache key addition."""
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})

        def build_key(a, b):
            return f"{a}:{b}"

        result = add_cache_keys(df, ["a", "b"], build_key)
        assert "cache_key" in result.columns
        assert result["cache_key"].to_list() == ["1:3", "2:4"]

    def test_add_cache_keys_custom_column_name(self):
        """Test with custom result column name."""
        df = pl.DataFrame({"x": [1, 2]})

        def build_key(x):
            return f"key_{x}"

        result = add_cache_keys(df, ["x"], build_key, result_column="my_key")
        assert "my_key" in result.columns
        assert "cache_key" not in result.columns

    def test_add_cache_keys_preserves_original_columns(self):
        """Test that original columns are preserved."""
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})

        def build_key(a, b):
            return f"{a}:{b}"

        result = add_cache_keys(df, ["a", "b"], build_key)
        assert "a" in result.columns
        assert "b" in result.columns
        assert "c" in result.columns

    def test_add_cache_keys_with_temporary_column(self):
        """Test that temporary columns used for key building don't pollute output."""
        # This simulates the invitations use case
        df = pl.DataFrame(
            {
                "record_id": ["12345", "67890"],
                "status": ["3", "2"],
                "response_field": ["yes", None],
            }
        )

        # Add temporary column
        df = df.with_columns(
            pl.col("response_field").is_not_null().alias("has_response")
        )

        def build_key(record_id, status, has_response):
            return create_composite_cache_key(record_id, status, has_response)

        # Build cache keys
        result = add_cache_keys(df, ["record_id", "status", "has_response"], build_key)

        # Verify cache keys were created correctly
        assert "cache_key" in result.columns
        assert result["cache_key"].to_list() == ["12345:3:1", "67890:2:0"]

        # Now simulate cleanup - drop the temporary column
        result = result.drop("has_response")

        # Verify temporary column is gone but cache_key remains
        assert "has_response" not in result.columns
        assert "cache_key" in result.columns
        assert "record_id" in result.columns
        assert "status" in result.columns


class TestCacheFiltering:
    """Test DataFrame cache filtering."""

    @pytest.fixture
    def test_cache(self, tmp_path):
        """Create test cache."""
        return DataCache("test_filter", ttl_minutes=5, cache_dir=str(tmp_path))

    @pytest.fixture
    def test_df(self):
        """Create test DataFrame."""
        return pl.DataFrame(
            {
                "id": [1, 2, 3],
                "cache_key": ["key1", "key2", "key3"],
            }
        )

    @pytest.fixture
    def test_logger(self):
        """Create test logger."""
        return logging.getLogger("test_cache_filtering")

    def test_filter_by_cache_no_processed(self, test_df, test_cache, test_logger):
        """Test filtering with no processed records."""
        result = filter_by_cache(test_df, test_cache, "cache_key", test_logger, "test")
        assert len(result) == 3

    def test_filter_by_cache_some_processed(
        self, test_df, test_cache, test_logger, caplog
    ):
        """Test filtering with some processed records."""
        test_cache.mark_processed("key1")

        with caplog.at_level(logging.INFO, logger="test_cache_filtering"):
            result = filter_by_cache(
                test_df, test_cache, "cache_key", test_logger, "test"
            )

        assert len(result) == 2
        assert "key1" not in result["cache_key"].to_list()
        assert "Skipping" in caplog.text

    def test_filter_by_cache_all_processed(
        self, test_df, test_cache, test_logger, caplog
    ):
        """Test filtering with all processed."""
        for key in test_df["cache_key"].to_list():
            test_cache.mark_processed(key)

        with caplog.at_level(logging.INFO, logger="test_cache_filtering"):
            result = filter_by_cache(
                test_df, test_cache, "cache_key", test_logger, "test"
            )

        assert len(result) == 0
        assert "already processed" in caplog.text

    def test_filter_by_cache_preserves_other_columns(
        self, test_df, test_cache, test_logger
    ):
        """Test that filtering preserves all other columns."""
        test_cache.mark_processed("key1")
        result = filter_by_cache(test_df, test_cache, "cache_key", test_logger, "test")

        # Should have same columns
        assert set(result.columns) == set(test_df.columns)
        # But fewer rows
        assert len(result) == 2


class TestCacheLogging:
    """Test cache logging utilities."""

    @pytest.fixture
    def test_logger(self):
        """Create test logger."""
        return logging.getLogger("test_cache_logging")

    def test_log_cache_statistics(self, tmp_path, test_logger, caplog):
        """Test logging cache statistics."""
        cache = DataCache("test_log", ttl_minutes=5, cache_dir=str(tmp_path))
        cache.mark_processed("test1")

        with caplog.at_level(logging.INFO, logger="test_cache_logging"):
            log_cache_statistics(cache, test_logger)

        assert "Cache statistics" in caplog.text
        assert "1 entries" in caplog.text

    def test_log_cache_statistics_empty(self, tmp_path, test_logger, caplog):
        """Test logging statistics for empty cache."""
        cache = DataCache("test_log_empty", ttl_minutes=5, cache_dir=str(tmp_path))

        with caplog.at_level(logging.INFO, logger="test_cache_logging"):
            log_cache_statistics(cache, test_logger)

        assert "Cache statistics" in caplog.text
        assert "0 entries" in caplog.text


class TestInvitationCacheKeyWorkflow:
    """Test the complete workflow for invitation cache keys."""

    @pytest.fixture
    def invitation_df(self):
        """Create a sample invitation DataFrame."""
        return pl.DataFrame(
            {
                "record_id": ["12345", "67890", "11111"],
                "curious_account_created_invite_status": ["1", "3", "3"],
                "curious_account_created_responder_account_created_response": [
                    None,
                    None,
                    "I confirm that I have created a Curious account",
                ],
                "redcap_event_name": ["admin_arm_1", "admin_arm_1", "admin_arm_1"],
                "instrument": [
                    "curious_account_created_responder",
                    "curious_account_created_responder",
                    "curious_account_created_responder",
                ],
            }
        )

    def test_invitation_cache_key_workflow(self, invitation_df):
        """Test full workflow of adding cache keys and cleaning up."""
        # Step 1: Add has_response column
        response_field = "curious_account_created_responder_account_created_response"
        df_with_temp = invitation_df.with_columns(
            pl.col(response_field).is_not_null().alias("has_response")
        )

        # Verify has_response was added correctly
        assert "has_response" in df_with_temp.columns
        assert df_with_temp["has_response"].to_list() == [False, False, True]

        # Step 2: Build cache keys
        def build_invitation_key(record_id, status, has_response):
            return create_composite_cache_key(record_id, status, has_response)

        df_with_keys = add_cache_keys(
            df_with_temp,
            ["record_id", "curious_account_created_invite_status", "has_response"],
            build_invitation_key,
        )

        # Verify cache keys are correct
        assert "cache_key" in df_with_keys.columns
        expected_keys = ["12345:1:0", "67890:3:0", "11111:3:1"]
        assert df_with_keys["cache_key"].to_list() == expected_keys

        # Step 3: Remove temporary column
        df_clean = df_with_keys.drop("has_response")

        # Verify has_response is gone but cache_key remains
        assert "has_response" not in df_clean.columns
        assert "cache_key" in df_clean.columns

        # Step 4: Simulate preparing for REDCap push - remove metadata columns
        metadata_columns = ["instrument", "cache_key"]
        df_for_redcap = df_clean.drop(
            [col for col in metadata_columns if col in df_clean.columns]
        )

        # Verify only REDCap fields remain
        assert "has_response" not in df_for_redcap.columns
        assert "cache_key" not in df_for_redcap.columns
        assert "instrument" not in df_for_redcap.columns
        assert "record_id" in df_for_redcap.columns
        assert "redcap_event_name" in df_for_redcap.columns

    def test_cache_key_distinguishes_states(self, invitation_df):
        """Test that cache keys properly distinguish different invitation states."""
        response_field = "curious_account_created_responder_account_created_response"

        def build_invitation_key(record_id, status, has_response):
            return create_composite_cache_key(record_id, status, has_response)

        # Same record, different states
        df1 = invitation_df.filter(pl.col("record_id") == "12345").with_columns(
            pl.col(response_field).is_not_null().alias("has_response")
        )
        df1_with_keys = add_cache_keys(
            df1,
            ["record_id", "curious_account_created_invite_status", "has_response"],
            build_invitation_key,
        )

        # Change status for same record
        df2 = df1.with_columns(
            pl.lit("3").alias("curious_account_created_invite_status")
        )
        df2_with_keys = add_cache_keys(
            df2,
            ["record_id", "curious_account_created_invite_status", "has_response"],
            build_invitation_key,
        )

        # Keys should be different
        key1 = df1_with_keys["cache_key"][0]
        key2 = df2_with_keys["cache_key"][0]
        assert key1 != key2
        assert key1 == "12345:1:0"  # Not sent
        assert key2 == "12345:3:0"  # Accepted, no response
