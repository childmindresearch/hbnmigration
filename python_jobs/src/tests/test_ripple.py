"""Tests for Ripple sourced data migration."""

from unittest.mock import patch

import pandas as pd
import pytest
import requests

from hbnmigration.exceptions import NoData
from hbnmigration.from_ripple.to_redcap import (
    get_redcap_subjects_to_update,
    main,
    prepare_redcap_data,
    push_to_redcap,
    request_potential_participants,
    set_redcap_columns,
    set_status_in_ripple,
)

from .conftest import assert_cleanup_called, assert_valid_redcap_columns


class TestGetRedcapSubjectsToUpdate:
    """Test :py:func:`hbnmigration.from_ripple.to_redcap.get_redcap_subjects_to_update`."""  # noqa: E501

    @patch("hbnmigration.from_ripple.to_redcap.fetch_api_data")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_splits_existing_and_new_subjects(
        self,
        mock_vars,
        mock_fetch,
        incoming_subjects_mixed,
        mock_redcap_existing_subjects,
    ):
        """Test subjects are correctly split into update vs new."""
        mock_vars.Tokens.pid247 = "prod_token"
        mock_vars.headers = {"Content-Type": "application/json"}
        mock_fetch.return_value = mock_redcap_existing_subjects

        to_update, new_subjects = get_redcap_subjects_to_update(incoming_subjects_mixed)

        assert len(to_update) == 2
        assert len(new_subjects) == 1
        assert 99001 in new_subjects["mrn"].values
        assert 12345 in to_update["mrn"].values
        assert 67890 in to_update["mrn"].values

    @patch("hbnmigration.from_ripple.to_redcap.fetch_api_data")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_update_subjects_get_correct_record_ids(
        self,
        mock_vars,
        mock_fetch,
        incoming_subjects_mixed,
        mock_redcap_existing_subjects,
    ):
        """Test that existing subjects get their REDCap record_id correctly merged."""
        mock_vars.Tokens.pid247 = "prod_token"
        mock_vars.headers = {"Content-Type": "application/json"}
        mock_fetch.return_value = mock_redcap_existing_subjects

        to_update, _ = get_redcap_subjects_to_update(incoming_subjects_mixed)

        # Check that record_ids from REDCap are correctly assigned
        alec_row = to_update[to_update["mrn"] == 12345]
        assert alec_row["record_id"].iloc[0] == 1

        abby_row = to_update[to_update["mrn"] == 67890]
        assert abby_row["record_id"].iloc[0] == 2

    @patch("hbnmigration.from_ripple.to_redcap.fetch_api_data")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_update_has_correct_columns(
        self,
        mock_vars,
        mock_fetch,
        incoming_subjects_mixed,
        mock_redcap_existing_subjects,
    ):
        """Test to_update DataFrame has correct column order."""
        mock_vars.Tokens.pid247 = "prod_token"
        mock_vars.headers = {"Content-Type": "application/json"}
        mock_fetch.return_value = mock_redcap_existing_subjects

        to_update, _ = get_redcap_subjects_to_update(incoming_subjects_mixed)

        assert list(to_update.columns) == ["record_id", "mrn", "email_consent"]

    @patch("hbnmigration.from_ripple.to_redcap.fetch_api_data")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_all_subjects_are_new(self, mock_vars, mock_fetch, incoming_subjects_mixed):
        """Test when no subjects exist in REDCap."""
        mock_vars.Tokens.pid247 = "prod_token"
        mock_vars.headers = {"Content-Type": "application/json"}
        mock_fetch.return_value = pd.DataFrame({"mrn": [], "record_id": []})

        to_update, new_subjects = get_redcap_subjects_to_update(incoming_subjects_mixed)

        assert len(to_update) == 0
        assert len(new_subjects) == 3

    @patch("hbnmigration.from_ripple.to_redcap.fetch_api_data")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_all_subjects_exist(self, mock_vars, mock_fetch, incoming_subjects_mixed):
        """Test when all subjects already exist in REDCap."""
        mock_vars.Tokens.pid247 = "prod_token"
        mock_vars.headers = {"Content-Type": "application/json"}
        mock_fetch.return_value = pd.DataFrame(
            {
                "mrn": [12345, 67890, 99001],
                "record_id": [1, 2, 3],
            }
        )

        to_update, new_subjects = get_redcap_subjects_to_update(incoming_subjects_mixed)

        assert len(to_update) == 3
        assert len(new_subjects) == 0

    @patch("hbnmigration.from_ripple.to_redcap.fetch_api_data")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_uses_correct_api_parameters(
        self, mock_vars, mock_fetch, incoming_subjects_mixed
    ):
        """Test that correct parameters are sent to REDCap API."""
        mock_vars.Tokens.pid247 = "prod_token_gardener"
        mock_vars.headers = {"Content-Type": "application/json"}
        mock_fetch.return_value = pd.DataFrame({"mrn": [], "record_id": []})

        get_redcap_subjects_to_update(incoming_subjects_mixed)

        # Check fetch_api_data was called with correct params
        call_args = mock_fetch.call_args
        assert call_args[0][2]["token"] == "prod_token_gardener"
        assert call_args[0][2]["fields"] == "mrn,record_id"
        assert call_args[0][2]["action"] == "export"


class TestRequestPotentialParticipants:
    """Test :py:func:`hbnmigration.from_ripple.to_redcap.request_potential_participants`."""  # noqa: E501

    @patch("hbnmigration.from_ripple.to_redcap.Endpoints")
    @patch("hbnmigration.from_ripple.to_redcap.ripple_variables")
    def test_successful_request_with_valid_data(
        self, mock_vars, mock_endpoints, sample_ripple_data
    ):
        """Test successful API request returns filtered DataFrame."""
        mock_vars.study_ids = {
            "HBN - Main": "main_study_id",
            "HBN - Waitlist": "waitlist_study_id",
        }
        mock_vars.column_dict.return_value = {}
        # Return only the rows for each specific study
        main_data = sample_ripple_data[
            sample_ripple_data["importType"] == "HBN - Main"
        ]  # 2 rows
        waitlist_data = sample_ripple_data[
            sample_ripple_data["importType"] == "HBN - Waitlist"
        ]  # 2 rows
        mock_endpoints.Ripple.export_from_ripple.side_effect = [
            main_data,
            waitlist_data,
        ]
        result = request_potential_participants()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4  # 2 from Main + 2 from Waitlist
        assert all(result["cv.consent_form"] == "Send to RedCap")

    @patch("hbnmigration.from_ripple.to_redcap.Endpoints")
    @patch("hbnmigration.from_ripple.to_redcap.ripple_variables")
    def test_empty_dataframe_raises_no_data(self, mock_vars, mock_endpoints):
        """Test empty DataFrame raises NoData exception."""
        mock_vars.study_ids = {"HBN - Main": "main_study_id"}
        mock_vars.column_dict.return_value = {}
        mock_endpoints.Ripple.export_from_ripple.return_value = pd.DataFrame()
        with pytest.raises(NoData):
            request_potential_participants()

    @patch("hbnmigration.from_ripple.to_redcap.Endpoints")
    @patch("hbnmigration.from_ripple.to_redcap.ripple_variables")
    def test_no_send_to_redcap_raises_no_data(
        self, mock_vars, mock_endpoints, anton_arcane_corrupted_data
    ):
        """Test no 'Send to RedCap' records raises NoData."""
        mock_vars.study_ids = {"HBN - Main": "main_study_id"}
        mock_vars.column_dict.return_value = {}
        mock_endpoints.Ripple.export_from_ripple.return_value = (
            anton_arcane_corrupted_data
        )
        with pytest.raises(NoData):
            request_potential_participants()

    @patch("hbnmigration.from_ripple.to_redcap.Endpoints")
    @patch("hbnmigration.from_ripple.to_redcap.ripple_variables")
    def test_filters_consent_form_correctly(self, mock_vars, mock_endpoints):
        """Test filtering by consent form status."""
        mock_vars.study_ids = {"HBN - Main": "main_study_id"}
        mock_vars.column_dict.return_value = {}
        mock_df = pd.DataFrame(
            {
                "globalId": ["ST001", "AA001", "ANT001"],
                "firstName": ["Alec", "Abby", "Anton"],
                "cv.consent_form": ["Send to RedCap", "Send to RedCap", "Pending"],
                "customId": [1, 2, 3],
            }
        )
        mock_endpoints.Ripple.export_from_ripple.return_value = mock_df
        result = request_potential_participants()
        assert len(result) == 2
        assert "ANT001" not in result["globalId"].values

    @patch("hbnmigration.from_ripple.to_redcap.Endpoints")
    @patch("hbnmigration.from_ripple.to_redcap.ripple_variables")
    def test_multiple_studies_concatenation(self, mock_vars, mock_endpoints):
        """Test data from multiple studies is properly concatenated."""
        mock_vars.study_ids = {
            "HBN - Main": "main_study_id",
            "HBN - Waitlist": "waitlist_study_id",
        }
        mock_vars.column_dict.return_value = {}
        mock_df_main = pd.DataFrame(
            {
                "globalId": ["ST001"],
                "firstName": ["Alec"],
                "cv.consent_form": ["Send to RedCap"],
                "customId": [1],
            }
        )
        mock_df_waitlist = pd.DataFrame(
            {
                "globalId": ["AA001"],
                "firstName": ["Abby"],
                "cv.consent_form": ["Send to RedCap"],
                "customId": [2],
            }
        )
        mock_endpoints.Ripple.export_from_ripple.side_effect = [
            mock_df_main,
            mock_df_waitlist,
        ]
        result = request_potential_participants()
        assert len(result) == 2
        assert set(result["globalId"].values) == {"ST001", "AA001"}

    @patch("hbnmigration.from_ripple.to_redcap.Endpoints")
    @patch("hbnmigration.from_ripple.to_redcap.ripple_variables")
    def test_single_record_processing(self, mock_vars, mock_endpoints):
        """Test processing with exactly one record."""
        mock_vars.study_ids = {"HBN - Main": "main_study_id"}
        mock_vars.column_dict.return_value = {}
        mock_df = pd.DataFrame(
            {
                "globalId": ["SGL001"],
                "firstName": ["Single"],
                "cv.consent_form": ["Send to RedCap"],
                "customId": [1],
            }
        )
        mock_endpoints.Ripple.export_from_ripple.return_value = mock_df
        result = request_potential_participants()
        assert len(result) == 1


class TestSetRedcapColumns:
    """Test :py:func:`hbnmigration.from_ripple.to_redcap.set_redcap_columns`."""

    def test_basic_column_renaming(self, swamp_thing_participant):
        """Test basic column renaming and selection."""
        result = set_redcap_columns(swamp_thing_participant)
        assert_valid_redcap_columns(result)
        assert result["record_id"].iloc[0] == 12345
        assert result["mrn"].iloc[0] == 12345

    def test_email_extraction_from_contacts(self):
        """Test email extraction from contact fields."""
        ripple_df = pd.DataFrame(
            {
                "customId": [67890],
                "globalId": ["AA001"],
                "contact.1.infos.1.contactType": ["phone"],
                "contact.1.infos.1.information": ["504-555-0101"],
                "contact.2.infos.1.contactType": ["email"],
                "contact.2.infos.1.information": ["abby.arcane@parliament.org"],
            }
        )
        result = set_redcap_columns(ripple_df)
        assert result["email_consent"].iloc[0] == "abby.arcane@parliament.org"

    def test_multiple_email_contacts_first_wins(self):
        """Test that when multiple emails exist, first one is selected."""
        ripple_df = pd.DataFrame(
            {
                "customId": [77001],
                "globalId": ["WOO001"],
                "firstName": ["Tefé"],
                "contact.1.infos.1.contactType": ["email"],
                "contact.1.infos.1.information": ["tefe.holland@swamp.com"],
                "contact.2.infos.1.contactType": ["email"],
                "contact.2.infos.1.information": ["tefe.alt@parliament.org"],
            }
        )
        result = set_redcap_columns(ripple_df)
        assert result["email_consent"].iloc[0] == "tefe.holland@swamp.com"

    def test_no_email_contact_results_in_nan(self):
        """Test that missing email results in NaN."""
        ripple_df = pd.DataFrame(
            {
                "customId": [66001],
                "globalId": ["SAU001"],
                "firstName": ["Sunderland"],
                "contact.1.infos.1.contactType": ["phone"],
                "contact.1.infos.1.information": ["504-555-0666"],
            }
        )
        result = set_redcap_columns(ripple_df)
        assert pd.isna(result["email_consent"].iloc[0])

    def test_unicode_characters_in_names(self):
        """Test handling of Unicode characters in participant names."""
        ripple_df = pd.DataFrame(
            {
                "customId": [77777],
                "globalId": ["UNI001"],
                "firstName": ["François"],
                "lastName": ["Müller-Göthe"],
                "contact.1.infos.1.contactType": ["email"],
                "contact.1.infos.1.information": ["françois@müller.com"],
            }
        )
        result = set_redcap_columns(ripple_df)
        assert result["record_id"].iloc[0] == 77777

    def test_empty_string_vs_none_handling(self):
        """Test distinction between empty strings and None values."""
        ripple_df = pd.DataFrame(
            {
                "customId": [66666, 66667],
                "globalId": ["EMP001", "EMP002"],
                "firstName": ["", None],
                "contact.1.infos.1.contactType": ["email", "email"],
                "contact.1.infos.1.information": ["", "none@test.com"],
            }
        )
        result = set_redcap_columns(ripple_df)
        assert len(result) == 2

    def test_contact_info_with_multiple_types(self):
        """Test extraction of email from mixed contact types."""
        ripple_df = pd.DataFrame(
            {
                "customId": [88888],
                "globalId": ["MCT001"],
                "firstName": ["MultiContact"],
                "contact.1.infos.1.contactType": ["phone"],
                "contact.1.infos.1.information": ["504-555-8888"],
                "contact.2.infos.1.contactType": ["email"],
                "contact.2.infos.1.information": ["multi@swamp.com"],
                "contact.3.infos.1.contactType": ["address"],
                "contact.3.infos.1.information": ["123 Swamp Lane"],
            }
        )
        result = set_redcap_columns(ripple_df)
        assert result["email_consent"].iloc[0] == "multi@swamp.com"


class TestPrepareRedcapData:
    """Test :py:func:`hbnmigration.from_ripple.to_redcap.prepare_redcap_data`."""

    @patch("hbnmigration.from_ripple.to_redcap.get_redcap_subjects_to_update")
    @patch("hbnmigration.from_ripple.to_redcap.set_redcap_columns")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_saves_both_update_and_new_files(
        self,
        mock_vars,
        mock_set_columns,
        mock_get_updates,
        temp_dir,
        bella_garten_participant,
    ):
        """Test that both update and new CSV files are created."""
        mock_vars.redcap_update_file = temp_dir / "update.csv"
        mock_vars.redcap_import_file = temp_dir / "import.csv"

        mock_set_columns.return_value = bella_garten_participant

        # Mock returning both updates and new subjects
        update_df = pd.DataFrame(
            {
                "record_id": [1],
                "mrn": [12345],
                "email_consent": ["alec@swamp.com"],
            }
        )
        new_df = pd.DataFrame(
            {
                "record_id": [99001],
                "mrn": [99001],
                "email_consent": ["bella@garden.green"],
            }
        )
        mock_get_updates.return_value = (update_df, new_df)

        prepare_redcap_data(pd.DataFrame())

        assert mock_vars.redcap_update_file.exists()
        assert mock_vars.redcap_import_file.exists()

        update_result = pd.read_csv(mock_vars.redcap_update_file)
        new_result = pd.read_csv(mock_vars.redcap_import_file)

        assert len(update_result) == 1
        assert len(new_result) == 1

    @patch("hbnmigration.from_ripple.to_redcap.get_redcap_subjects_to_update")
    @patch("hbnmigration.from_ripple.to_redcap.set_redcap_columns")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_only_creates_file_when_data_exists(
        self, mock_vars, mock_set_columns, mock_get_updates, temp_dir
    ):
        """Test files are only created when there's data to write."""
        mock_vars.redcap_update_file = temp_dir / "update.csv"
        mock_vars.redcap_import_file = temp_dir / "import.csv"

        mock_set_columns.return_value = pd.DataFrame(
            {
                "record_id": [1],
                "mrn": [1],
                "email_consent": ["test@test.com"],
            }
        )

        # Only new subjects, no updates
        empty_update = pd.DataFrame(columns=["record_id", "mrn", "email_consent"])
        new_df = pd.DataFrame(
            {
                "record_id": [99001],
                "mrn": [99001],
                "email_consent": ["bella@garden.green"],
            }
        )
        mock_get_updates.return_value = (empty_update, new_df)

        prepare_redcap_data(pd.DataFrame())

        assert not mock_vars.redcap_update_file.exists()
        assert mock_vars.redcap_import_file.exists()

    @patch("hbnmigration.from_ripple.to_redcap.get_redcap_subjects_to_update")
    @patch("hbnmigration.from_ripple.to_redcap.set_redcap_columns")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_csv_saved_without_index(
        self, mock_vars, mock_set_columns, mock_get_updates, temp_dir
    ):
        """Test CSV is saved without index column."""
        mock_vars.redcap_update_file = temp_dir / "update.csv"
        mock_vars.redcap_import_file = temp_dir / "import.csv"

        mock_df = pd.DataFrame(
            {
                "record_id": [12345],
                "mrn": [12345],
                "email_consent": ["alec@swamp.com"],
            }
        )
        mock_set_columns.return_value = mock_df
        mock_get_updates.return_value = (
            pd.DataFrame(columns=["record_id", "mrn", "email_consent"]),
            mock_df,
        )

        prepare_redcap_data(pd.DataFrame())

        result = pd.read_csv(mock_vars.redcap_import_file)
        assert "Unnamed: 0" not in result.columns

    @patch("hbnmigration.from_ripple.to_redcap.get_redcap_subjects_to_update")
    @patch("hbnmigration.from_ripple.to_redcap.set_redcap_columns")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_handles_large_dataset(
        self, mock_vars, mock_set_columns, mock_get_updates, temp_dir
    ):
        """Test handling of larger datasets - Full garden assembly."""
        mock_vars.redcap_update_file = temp_dir / "update.csv"
        mock_vars.redcap_import_file = temp_dir / "import.csv"

        large_df = pd.DataFrame(
            {
                "record_id": range(10000, 10100),
                "mrn": range(10000, 10100),
                "email_consent": [f"plant{i}@garden.green" for i in range(100)],
            }
        )
        mock_set_columns.return_value = large_df
        mock_get_updates.return_value = (
            pd.DataFrame(columns=["record_id", "mrn", "email_consent"]),
            large_df,
        )

        prepare_redcap_data(pd.DataFrame())

        result = pd.read_csv(mock_vars.redcap_import_file)
        assert len(result) == 100

    @patch("hbnmigration.from_ripple.to_redcap.get_redcap_subjects_to_update")
    @patch("hbnmigration.from_ripple.to_redcap.set_redcap_columns")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_handles_special_characters_in_email(
        self, mock_vars, mock_set_columns, mock_get_updates, temp_dir
    ):
        """Test handling of special characters in email addresses."""
        mock_vars.redcap_update_file = temp_dir / "update.csv"
        mock_vars.redcap_import_file = temp_dir / "import.csv"

        mock_df = pd.DataFrame(
            {
                "record_id": [44001],
                "mrn": [44001],
                "email_consent": ["bella.o'garden@green.org"],
            }
        )
        mock_set_columns.return_value = mock_df
        mock_get_updates.return_value = (
            pd.DataFrame(columns=["record_id", "mrn", "email_consent"]),
            mock_df,
        )

        prepare_redcap_data(pd.DataFrame())

        result = pd.read_csv(mock_vars.redcap_import_file)
        assert "bella.o'garden@green.org" in result["email_consent"].values


class TestPushToRedcap:
    """Test :py:func:`hbnmigration.from_ripple.to_redcap.push_to_redcap`."""

    @pytest.fixture
    def csv_with_content(self, temp_csv_file):
        """CSV file with test content."""
        temp_csv_file.write_text(
            "record_id,mrn,email_consent\n12345,12345,alec@swamp.com"
        )
        return temp_csv_file

    @pytest.fixture
    def csv_with_update_content(self, temp_csv_file):
        """CSV file with update content."""
        temp_csv_file.write_text(
            "record_id,mrn,email_consent\n1,12345,alec.updated@swamp.com"
        )
        return temp_csv_file

    @patch("hbnmigration.from_ripple.to_redcap.requests.post")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_successful_push_new_records(
        self, mock_vars, mock_post, csv_with_content, mock_redcap_response
    ):
        """Test successful push of new records to REDCap."""
        mock_vars.redcap_import_file = csv_with_content
        mock_vars.redcap_update_file = csv_with_content.parent / "nonexistent.csv"
        mock_post.return_value = mock_redcap_response

        push_to_redcap("test_token_swamp_thing", update=False)

        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert call_args[1]["data"]["token"] == "test_token_swamp_thing"
        assert call_args[1]["data"]["forceAutoNumber"] == "true"

    @patch("hbnmigration.from_ripple.to_redcap.requests.post")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_successful_push_update_records(
        self, mock_vars, mock_post, csv_with_update_content, mock_redcap_response
    ):
        """Test successful push of record updates to REDCap."""
        mock_vars.redcap_update_file = csv_with_update_content
        mock_vars.redcap_import_file = (
            csv_with_update_content.parent / "nonexistent.csv"
        )
        mock_post.return_value = mock_redcap_response

        push_to_redcap("test_token_garden", update=True)

        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert call_args[1]["data"]["forceAutoNumber"] == "false"

    @patch("hbnmigration.from_ripple.to_redcap.requests.post")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_push_both_when_update_none(
        self, mock_vars, mock_post, temp_dir, mock_redcap_response
    ):
        """Test pushing both update and new when update=None."""
        update_file = temp_dir / "update.csv"
        import_file = temp_dir / "import.csv"

        update_file.write_text("record_id,mrn,email_consent\n1,12345,alec@swamp.com")
        import_file.write_text(
            "record_id,mrn,email_consent\n99001,99001,bella@garden.green"
        )

        mock_vars.redcap_update_file = update_file
        mock_vars.redcap_import_file = import_file
        mock_post.return_value = mock_redcap_response

        push_to_redcap("test_token", update=None)

        assert mock_post.call_count == 2

    @pytest.mark.parametrize(
        "exception_class,error_message",
        [
            (requests.exceptions.Timeout, "Request timed out"),
            (requests.exceptions.ConnectionError, "Cannot connect"),
            (requests.exceptions.HTTPError, "500 Error"),
        ],
    )
    @patch("hbnmigration.from_ripple.to_redcap.requests.post")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_push_to_redcap_handles_errors(
        self, mock_vars, mock_post, csv_with_content, exception_class, error_message
    ):
        """Test push_to_redcap handles various exceptions."""
        mock_vars.redcap_import_file = csv_with_content
        mock_vars.redcap_update_file = csv_with_content.parent / "nonexistent.csv"
        mock_post.side_effect = exception_class(error_message)

        with pytest.raises(exception_class):
            push_to_redcap("test_token", update=False)

    @patch("hbnmigration.from_ripple.to_redcap.requests.post")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_file_not_found_handles_gracefully(
        self, mock_vars, mock_post, temp_csv_file
    ):
        """Test handles missing file gracefully."""
        temp_csv_file.unlink(missing_ok=True)
        mock_vars.redcap_import_file = temp_csv_file
        mock_vars.redcap_update_file = temp_csv_file.parent / "nonexistent.csv"

        push_to_redcap("test_token", update=False)

        mock_post.assert_not_called()

    @patch("hbnmigration.from_ripple.to_redcap.requests.post")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_empty_file_handles_gracefully(self, mock_vars, mock_post, temp_csv_file):
        """Test handles empty file gracefully."""
        temp_csv_file.write_text("")
        mock_vars.redcap_import_file = temp_csv_file
        mock_vars.redcap_update_file = temp_csv_file.parent / "nonexistent.csv"

        push_to_redcap("test_token", update=False)

        mock_post.assert_not_called()


class TestSetStatusInRipple:
    """Test :py:func:`hbnmigration.from_ripple.to_redcap.set_status_in_ripple`."""

    @patch("hbnmigration.from_ripple.to_redcap.requests.post")
    @patch("hbnmigration.from_ripple.to_redcap.Endpoints")
    @patch("hbnmigration.from_ripple.to_redcap.ripple_variables")
    def test_successful_status_update(
        self,
        mock_vars,
        mock_endpoints,
        mock_post,
        excel_file_with_data,
        mock_ripple_response,
    ):
        """Test successful status update in Ripple."""
        mock_endpoints.Ripple.import_data.return_value = (
            "https://ripple.swamp.org/import"
        )
        mock_post.return_value = mock_ripple_response

        set_status_in_ripple("HBN - Main", str(excel_file_with_data))

        mock_post.assert_called_once()

    @patch("hbnmigration.from_ripple.to_redcap.ripple_variables")
    def test_empty_file_no_request(self, mock_vars, temp_excel_file):
        """Test empty Excel file doesn't trigger API request."""
        empty_df = pd.DataFrame()
        empty_df.to_excel(temp_excel_file, index=False)

        set_status_in_ripple("HBN - Main", str(temp_excel_file))

    def test_file_not_found_raises_exception(self, temp_excel_file):
        """Test FileNotFoundError is raised for missing file."""
        temp_excel_file.unlink(missing_ok=True)

        with pytest.raises(FileNotFoundError):
            set_status_in_ripple("HBN - Main", str(temp_excel_file))

    @pytest.mark.parametrize(
        "study_name",
        [
            "HBN - Main",
            "HBN - Waitlist",
        ],
    )
    @patch("hbnmigration.from_ripple.to_redcap.requests.post")
    @patch("hbnmigration.from_ripple.to_redcap.Endpoints")
    @patch("hbnmigration.from_ripple.to_redcap.ripple_variables")
    def test_different_studies(
        self,
        mock_vars,
        mock_endpoints,
        mock_post,
        excel_file_with_data,
        mock_ripple_response,
        study_name,
    ):
        """Test importing to different study types."""
        mock_endpoints.Ripple.import_data.return_value = (
            "https://ripple.swamp.org/import"
        )
        mock_post.return_value = mock_ripple_response

        set_status_in_ripple(study_name, str(excel_file_with_data))

        mock_post.assert_called_once()

    @pytest.mark.parametrize(
        "exception_class",
        [
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ],
    )
    @patch("hbnmigration.from_ripple.to_redcap.requests.post")
    @patch("hbnmigration.from_ripple.to_redcap.Endpoints")
    @patch("hbnmigration.from_ripple.to_redcap.ripple_variables")
    def test_handles_ripple_api_errors(
        self,
        mock_vars,
        mock_endpoints,
        mock_post,
        excel_file_with_data,
        exception_class,
    ):
        """Test handling of various Ripple API errors."""
        mock_endpoints.Ripple.import_data.return_value = (
            "https://ripple.swamp.org/import"
        )
        mock_post.side_effect = exception_class("API error")

        with pytest.raises((exception_class, requests.exceptions.RequestException)):
            set_status_in_ripple("HBN - Main", str(excel_file_with_data))

    @patch("hbnmigration.from_ripple.to_redcap.requests.post")
    @patch("hbnmigration.from_ripple.to_redcap.Endpoints")
    @patch("hbnmigration.from_ripple.to_redcap.ripple_variables")
    def test_large_excel_file_processing(
        self,
        mock_vars,
        mock_endpoints,
        mock_post,
        temp_excel_file,
        mock_ripple_response,
    ):
        """Test processing of large Excel file."""
        large_df = pd.DataFrame(
            {
                "globalId": [f"LRG{i:03d}" for i in range(1000)],
                "status": ["Sent to RedCap"] * 1000,
            }
        )
        large_df.to_excel(temp_excel_file, index=False)

        mock_endpoints.Ripple.import_data.return_value = (
            "https://ripple.swamp.org/import"
        )
        mock_post.return_value = mock_ripple_response

        set_status_in_ripple("HBN - Main", str(temp_excel_file))

        mock_post.assert_called_once()


class TestMain:
    """Test :py:func:`hbnmigration.from_ripple.to_redcap.main`."""

    @pytest.mark.parametrize(
        "project_status,token_attr,token_value",
        [
            ("dev", "pid757", "dev_token_swamp"),
            ("prod", "pid247", "prod_token_parliament"),
        ],
    )
    @patch("hbnmigration.from_ripple.to_redcap.cleanup")
    @patch("hbnmigration.from_ripple.to_redcap.set_status_in_ripple")
    @patch("hbnmigration.from_ripple.to_redcap.push_to_redcap")
    @patch("hbnmigration.from_ripple.to_redcap.prepare_ripple_to_ripple")
    @patch("hbnmigration.from_ripple.to_redcap.prepare_redcap_data")
    @patch("hbnmigration.from_ripple.to_redcap.request_potential_participants")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_workflow_by_project_status(
        self,
        mock_vars,
        mock_request,
        mock_prep_redcap,
        mock_prep_ripple,
        mock_push,
        mock_status,
        mock_cleanup,
        temp_excel_file,
        project_status,
        token_attr,
        token_value,
    ):
        """Test workflow for different project statuses."""
        setattr(mock_vars.Tokens, token_attr, token_value)
        mock_df = pd.DataFrame({"globalId": ["ST001"]})
        mock_request.return_value = mock_df
        mock_prep_ripple.return_value = {"HBN - Main": str(temp_excel_file)}

        main(project_status=project_status)

        mock_request.assert_called_once()
        mock_prep_redcap.assert_called_once()
        mock_prep_ripple.assert_called_once()
        mock_push.assert_called_once_with(token_value)  # Remove the None parameter
        mock_status.assert_called_once()
        assert_cleanup_called(mock_cleanup)

    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    @patch("hbnmigration.from_ripple.to_redcap.cleanup")
    @patch("hbnmigration.from_ripple.to_redcap.request_potential_participants")
    def test_no_data_calls_cleanup(self, mock_request, mock_cleanup, mock_redcap_vars):
        """Test cleanup is called even when NoData is raised."""
        mock_redcap_vars.Tokens.pid757 = "dev_token"
        mock_request.side_effect = NoData

        main(project_status="dev")

        assert_cleanup_called(mock_cleanup)

    @patch("hbnmigration.from_ripple.to_redcap.cleanup")
    @patch("hbnmigration.from_ripple.to_redcap.set_status_in_ripple")
    @patch("hbnmigration.from_ripple.to_redcap.push_to_redcap")
    @patch("hbnmigration.from_ripple.to_redcap.prepare_ripple_to_ripple")
    @patch("hbnmigration.from_ripple.to_redcap.prepare_redcap_data")
    @patch("hbnmigration.from_ripple.to_redcap.request_potential_participants")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_empty_ripple_prep_dict(
        self,
        mock_vars,
        mock_request,
        mock_prep_redcap,
        mock_prep_ripple,
        mock_push,
        mock_status,
        mock_cleanup,
    ):
        """Test when prepare_ripple_to_ripple returns empty dict."""
        mock_vars.Tokens.pid757 = "dev_token"
        mock_df = pd.DataFrame({"globalId": ["EMPT001"]})
        mock_request.return_value = mock_df
        mock_prep_ripple.return_value = {}

        main(project_status="dev")

        mock_push.assert_called_once()
        mock_status.assert_not_called()
        assert_cleanup_called(mock_cleanup)

    @pytest.mark.parametrize(
        "failing_function,exception_type",
        [
            ("mock_prep_redcap", ValueError),
            ("mock_prep_ripple", RuntimeError),
            ("mock_push", requests.exceptions.ConnectionError),
        ],
    )
    @patch("hbnmigration.from_ripple.to_redcap.cleanup")
    @patch("hbnmigration.from_ripple.to_redcap.set_status_in_ripple")
    @patch("hbnmigration.from_ripple.to_redcap.push_to_redcap")
    @patch("hbnmigration.from_ripple.to_redcap.prepare_ripple_to_ripple")
    @patch("hbnmigration.from_ripple.to_redcap.prepare_redcap_data")
    @patch("hbnmigration.from_ripple.to_redcap.request_potential_participants")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_exception_triggers_cleanup(
        self,
        mock_vars,
        mock_request,
        mock_prep_redcap,
        mock_prep_ripple,
        mock_push,
        mock_status,
        mock_cleanup,
        failing_function,
        exception_type,
    ):
        """Test cleanup is called when any function in the workflow fails."""
        mock_vars.Tokens.pid757 = "dev_token"
        mock_df = pd.DataFrame({"globalId": ["ERR001"]})
        mock_request.return_value = mock_df
        mock_prep_ripple.return_value = {}

        failing_mock = locals()[failing_function]
        failing_mock.side_effect = exception_type("Operation failed")

        with pytest.raises(exception_type):
            main(project_status="dev")

        assert_cleanup_called(mock_cleanup)

    @patch("hbnmigration.from_ripple.to_redcap.cleanup")
    @patch("hbnmigration.from_ripple.to_redcap.set_status_in_ripple")
    @patch("hbnmigration.from_ripple.to_redcap.push_to_redcap")
    @patch("hbnmigration.from_ripple.to_redcap.prepare_ripple_to_ripple")
    @patch("hbnmigration.from_ripple.to_redcap.prepare_redcap_data")
    @patch("hbnmigration.from_ripple.to_redcap.request_potential_participants")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_partial_ripple_update_failure(
        self,
        mock_vars,
        mock_request,
        mock_prep_redcap,
        mock_prep_ripple,
        mock_push,
        mock_status,
        mock_cleanup,
        temp_excel_file,
    ):
        """Test when one Ripple update succeeds but another fails."""
        mock_vars.Tokens.pid757 = "dev_token"
        mock_df = pd.DataFrame({"globalId": ["PRT001", "PRT002"]})
        mock_request.return_value = mock_df

        excel_file_2 = temp_excel_file.parent / "ripple_waitlist.xlsx"
        df2 = pd.DataFrame({"globalId": ["PRT002"]})
        df2.to_excel(excel_file_2, index=False)

        mock_prep_ripple.return_value = {
            "HBN - Main": str(temp_excel_file),
            "HBN - Waitlist": str(excel_file_2),
        }

        mock_status.side_effect = [
            None,
            requests.exceptions.RequestException("Second update failed"),
        ]

        with pytest.raises(requests.exceptions.RequestException):
            main(project_status="dev")

        assert_cleanup_called(mock_cleanup)

    @patch("hbnmigration.from_ripple.to_redcap.cleanup")
    @patch("hbnmigration.from_ripple.to_redcap.set_status_in_ripple")
    @patch("hbnmigration.from_ripple.to_redcap.push_to_redcap")
    @patch("hbnmigration.from_ripple.to_redcap.prepare_ripple_to_ripple")
    @patch("hbnmigration.from_ripple.to_redcap.prepare_redcap_data")
    @patch("hbnmigration.from_ripple.to_redcap.request_potential_participants")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_multiple_studies_in_ripple_prep(
        self,
        mock_vars,
        mock_request,
        mock_prep_redcap,
        mock_prep_ripple,
        mock_push,
        mock_status,
        mock_cleanup,
        temp_excel_file,
    ):
        """Test workflow with multiple studies requiring Ripple updates."""
        mock_vars.Tokens.pid757 = "dev_token_parliament"
        mock_df = pd.DataFrame(
            {
                "globalId": ["WND001", "GRS001"],
                "firstName": ["Woodrue", "Constantine"],
            }
        )
        mock_request.return_value = mock_df

        excel_file_2 = temp_excel_file.parent / "ripple_waitlist.xlsx"
        df2 = pd.DataFrame({"globalId": ["GRS001"]})
        df2.to_excel(excel_file_2, index=False)

        mock_prep_ripple.return_value = {
            "HBN - Main": str(temp_excel_file),
            "HBN - Waitlist": str(excel_file_2),
        }

        main(project_status="dev")

        assert mock_status.call_count == 2
        assert_cleanup_called(mock_cleanup)

    @patch("hbnmigration.from_ripple.to_redcap.cleanup")
    @patch("hbnmigration.from_ripple.to_redcap.set_status_in_ripple")
    @patch("hbnmigration.from_ripple.to_redcap.push_to_redcap")
    @patch("hbnmigration.from_ripple.to_redcap.prepare_ripple_to_ripple")
    @patch("hbnmigration.from_ripple.to_redcap.prepare_redcap_data")
    @patch("hbnmigration.from_ripple.to_redcap.request_potential_participants")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    def test_cleanup_receives_correct_files(
        self,
        mock_vars,
        mock_request,
        mock_prep_redcap,
        mock_prep_ripple,
        mock_push,
        mock_status,
        mock_cleanup,
        temp_excel_file,
    ):
        """Test cleanup is called with correct list of Ripple files."""
        mock_vars.Tokens.pid757 = "dev_token"
        mock_df = pd.DataFrame({"globalId": ["BG001"]})
        mock_request.return_value = mock_df

        excel_file_2 = temp_excel_file.parent / "ripple_waitlist.xlsx"
        df2 = pd.DataFrame({"globalId": ["BG001"]})
        df2.to_excel(excel_file_2, index=False)

        mock_prep_ripple.return_value = {
            "HBN - Main": str(temp_excel_file),
            "HBN - Waitlist": str(excel_file_2),
        }

        main(project_status="dev")

        cleanup_call_args = mock_cleanup.call_args[0][0]
        assert str(temp_excel_file) in cleanup_call_args
        assert str(excel_file_2) in cleanup_call_args


class TestIntegration:
    """Integration tests for the full workflow."""

    @patch("hbnmigration.from_ripple.to_redcap.cleanup")
    @patch("hbnmigration.from_ripple.to_redcap.requests.post")
    @patch("hbnmigration.from_ripple.to_redcap.Endpoints")
    @patch("hbnmigration.from_ripple.to_redcap.ripple_variables")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    @patch("hbnmigration.from_ripple.to_redcap.fetch_api_data")
    def test_full_workflow_end_to_end(
        self,
        mock_fetch,
        mock_rc_vars,
        mock_rp_vars,
        mock_endpoints,
        mock_post,
        mock_cleanup,
        temp_dir,
        mock_redcap_response,
    ):
        """Test complete end-to-end workflow with Swamp Thing and Gardener data."""
        mock_rc_vars.Tokens.pid757 = "dev_token"
        mock_rc_vars.redcap_import_file = temp_dir / "redcap_new.csv"
        mock_rc_vars.redcap_update_file = temp_dir / "redcap_update.csv"
        mock_rp_vars.ripple_import_file = temp_dir / "ripple.xlsx"
        mock_rp_vars.study_ids = {"HBN - Main": "main_study_id"}
        mock_rp_vars.column_dict.return_value = {}
        mock_endpoints.Ripple.import_data.return_value = "https://ripple.test/import"

        # Mock existing REDCap subjects
        mock_fetch.return_value = pd.DataFrame(
            {
                "mrn": [12345],
                "record_id": [1],
            }
        )

        ripple_data = pd.DataFrame(
            {
                "globalId": ["ST001", "BG001"],
                "customId": [12345, 99001],
                "firstName": ["Alec", "Bella"],
                "lastName": ["Holland", "Garten"],
                "cv.consent_form": ["Send to RedCap", "Send to RedCap"],
                "contact.1.infos.1.contactType": ["email", "email"],
                "contact.1.infos.1.information": [
                    "alec@swamp.com",
                    "bella@garden.green",
                ],
                "importType": ["HBN - Main", "HBN - Main"],
            }
        )
        mock_endpoints.Ripple.export_from_ripple.return_value = ripple_data
        mock_post.return_value = mock_redcap_response

        main(project_status="dev")

        # Should have called post at least twice (update + new for REDCap, plus Ripple)
        assert mock_post.call_count >= 2
        assert_cleanup_called(mock_cleanup)

    @patch("hbnmigration.from_ripple.to_redcap.cleanup")
    @patch("hbnmigration.from_ripple.to_redcap.requests.post")
    @patch("hbnmigration.from_ripple.to_redcap.Endpoints")
    @patch("hbnmigration.from_ripple.to_redcap.ripple_variables")
    @patch("hbnmigration.from_ripple.to_redcap.redcap_variables")
    @patch("hbnmigration.from_ripple.to_redcap.fetch_api_data")
    def test_workflow_with_only_updates(
        self,
        mock_fetch,
        mock_rc_vars,
        mock_rp_vars,
        mock_endpoints,
        mock_post,
        mock_cleanup,
        temp_dir,
        mock_redcap_response,
    ):
        """Test workflow when all subjects already exist (updates only)."""
        mock_rc_vars.Tokens.pid757 = "dev_token"
        mock_rc_vars.redcap_import_file = temp_dir / "redcap_new.csv"
        mock_rc_vars.redcap_update_file = temp_dir / "redcap_update.csv"
        mock_rp_vars.ripple_import_file = temp_dir / "ripple.xlsx"
        mock_rp_vars.study_ids = {"HBN - Main": "main_study_id"}
        mock_rp_vars.column_dict.return_value = {}
        mock_endpoints.Ripple.import_data.return_value = "https://ripple.test/import"

        # All subjects already exist
        mock_fetch.return_value = pd.DataFrame(
            {
                "mrn": [12345, 99001],
                "record_id": [1, 2],
            }
        )

        ripple_data = pd.DataFrame(
            {
                "globalId": ["ST001", "BG001"],
                "customId": [12345, 99001],
                "firstName": ["Alec", "Bella"],
                "cv.consent_form": ["Send to RedCap", "Send to RedCap"],
                "contact.1.infos.1.contactType": ["email", "email"],
                "contact.1.infos.1.information": [
                    "alec.new@swamp.com",
                    "bella.new@garden.green",
                ],
                "importType": ["HBN - Main", "HBN - Main"],
            }
        )
        mock_endpoints.Ripple.export_from_ripple.return_value = ripple_data
        mock_post.return_value = mock_redcap_response

        main(project_status="dev")

        # Verify update file was created
        assert mock_rc_vars.redcap_update_file.exists()
        # Verify import file was not created (no new subjects)
        assert not mock_rc_vars.redcap_import_file.exists()
        assert_cleanup_called(mock_cleanup)
