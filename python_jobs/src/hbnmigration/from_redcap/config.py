"""Nonsesitive for REDCap API calls."""

from collections import UserList
from typing import Final
from warnings import deprecated

from ..utility_functions import (
    ColumnRenameMapping,
    FieldDescriptor,
    RangeConstraint,
    ValueClass,
)

RedcapComplete = FieldDescriptor(
    {"Incomplete": "0", "Unverified": "1", "Complete": "2"}
)
"""Common `*_complete` values."""


class Constraints:
    """Return constraints on REDCap fields."""

    class PID625:
        """Constraints for REDCap PID 625."""

        class permission_audiovideo_participant:
            """Constraints for `permission_audiovideo_participant`."""

            age = RangeConstraint(11, 18, True, False)
            """Applicable age range."""


class FieldList(UserList):
    """Return list of REDCap fields with overloaded `__str__`."""

    def __str__(self) -> str:
        """Return as expected for REDCap API."""
        return ",".join(self.data)


class Fields:
    """Return fields for REDCap API calls."""

    class _ForCuriousMeta(type):
        """Metaclass for deprecating `for_curious` attribute."""

        _for_curious = FieldList(
            [
                "record_id",
                "mrn",
                "adult_enrollment_form_complete",
                "parent_involvement",
                "email",
                "parentfirstname",
                "parent_last_name",
                "prefname",
                "consent1",
                "consent5",
                "prefame_1821",
                "consent1_1821",
                "consent5_1821",
                "email_1821",
                "email_consent",
                "parentfirstname_1821",
                "parent_last_name_1821",
            ]
        )
        """Fields to export from REDCap PID 247 for import into Curious."""

        @property
        @deprecated(
            "Deprecated in v1.10.0. Use `Fields.export_operations.for_curious`."
        )
        def for_curious(cls) -> FieldList:
            """Fields to export from REDCap PID 247 for import into Curious."""
            return cls._for_curious

    class export_247(metaclass=_ForCuriousMeta):
        """Fields to export from REDCap PID 247."""

        for_redcap_operations: Final[FieldList] = FieldList(
            [
                "additional",
                "additional_1821",
                "address1",
                "address1_1821",
                "address2",
                "address_2",
                "adult_address",
                "adult_apt",
                "adult_city",
                "adult_email",
                "adult_state",
                "adult_zip",
                "aptnumber",
                "aptnumber_1821",
                "aptnumber_2",
                "aptnumber_2_1821",
                "biosex",
                "biosex_1821",
                "ci_date",
                "ci_forms_due",
                "city",
                "city_1821",
                "city_2",
                "city_2_1821",
                "consent1",
                "consent1_1821",
                "consent5",
                "consent5_1821",
                "dob",
                "dob_1821",
                "email",
                "email_1821",
                "email_2",
                "email_2_1821",
                # "enroll_date",
                "futurecontact",
                "futurecontact_2",
                "gender",
                "gender_1821",
                "gender_other",
                "gender_other_1821",
                "genderpronoun",
                "genderpronoun_1821",
                "genderpronoun_2_1821",
                "genderpronoun_parent1",
                "genderpronoun_parent2",
                # "genderpronoun_parent2_1821",
                "genderpronounother",
                "genderpronounother_1821",
                "guardian2_consent",
                "guardian2_consent_due",
                "guardian_relation",
                "guardian_relation_2",
                "guardian_relation_other",
                "guardian_relation_other_2",
                "intake_ready",
                "middlename_2_1821",
                "middlename_y",
                "mrn",
                "par_rel",
                "par_rel_2",
                "parent_last_name",
                "parent_last_name_1821",
                "parent_last_name_2",
                "parent_last_name_2_1821",
                "parent_second_guardian_consent_complete",
                "parentfirstname",
                "parentfirstname_1821",
                "parentfirstname_2",
                "parentfirstname_2_1821",
                "permission_audiovideo",
                "permission_audiovideo_1113",
                "permission_audiovideo_1417",
                "permission_collab",
                "phone",
                "phone_1821",
                "phone_2",
                "phone_2_1821",
                "prefname",
                "q_devices",
                "q_devices_1821",
                "q_devices_info",
                "q_devices_info_1821",
                "q_silverallergy",
                "q_silverallergy_1821",
                "record_id",
                "role_consent",
                "same_address",
                "sibling",
                "sibling_1821",
                "siblingdob",
                "siblingdob_1821",
                "siblingfirstname",
                "siblingfirstname_1821",
                "siblinglastname",
                "siblinglastname_1821",
                "state",
                "state_1821",
                "state_2",
                "state_2_1821",
                "zipcode",
                "zipcode_1821",
                "zipcode_2",
                "zipcode_2_1821",
            ]
        )
        """Fields to export from REDCap PID 247 for import into REDCap PID 625."""
        for_redcap_responder_tracking: Final[FieldList] = FieldList(
            [
                "additional",
                "additional_1821",
                "consent1",
                "consent1_1821",
                "dob",
                "dob_1821",
                "email",
                "email_2",
                "email_2_1821",
                "email_1821",
                "mrn",
                "parentfirstname",
                "parentfirstname_2",
                "parentfirstname_2_1821",
                "parentfirstname_1821",
                "parent_last_name",
                "parent_last_name_2",
                "parent_last_name_2_1821",
                "parent_last_name_1821",
                "phone",
                "phone_2_1821",
                "phone_1821",
            ]
        )
        """Fields to export from REDCap PID 247 for import into REDCap PID 879."""

    class export_operations:
        """Fields to export from REDCap PID 625."""

        for_curious: Final[FieldList] = FieldList(
            [
                "additional",
                "address1",
                "address_2",
                "aptnumber1",
                "aptnumber_2",
                "city1",
                "city_2",
                "complete_parent_second_guardian_consent",
                "curious_email_child",
                "curious_password_child",
                "dob",
                "email",
                "email_2",
                "enrollment_complete",
                "first_name",
                "gender",
                "gender_other",
                "genderpronoun",
                "genderpronoun_parent1",
                "genderpronoun_parent2",
                "genderpronounother",
                "guardian_relation",
                "guardian_relation_2",
                "last_name",
                "middlename_y",
                "mrn",
                "parent_involvement",
                "parent_last_name_2",
                "parentfirstname",
                "parentfirstname_2",
                "parentlastname",
                "permission_audiovideo_participant",
                "phone",
                "phone_2",
                "prefname",
                "record_id",
                "r_id",
                "sex",
                "sibling",
                "siblingdob",
                "siblingfirstname",
                "siblinglastname",
                "state1",
                "state_2",
                "zipcode1",
                "zipcode_2",
            ]
        )
        """Fields to export from REDCap PID 625 for import into Curious."""

        for_mrn_lookup: Final[FieldList] = FieldList(
            [
                "record_id",
                "mrn",
                "r_id",
            ]
        )
        """Fields to export from REDCap PID 625 for MRN lookup by r_id."""

    class import_curious:
        """Fields to import into Curious."""

        child: Final[dict[str, int | str | None]] = {
            "nickname": None,
            "role": "respondent",
            "tag": "Child",
            "accountType": "limited",
            "email": None,
            "firstName": None,
            "lastName": None,
            "secretUserId": None,
            "language": "en",
            "parent_involvement": None,
            "password": None,
        }
        """Fields to import into Curious for child accounts."""
        parent: Final[dict[str, int | str | None]] = {
            "email": None,
            "nickname": None,
            "role": "respondent",
            "tag": "Parent",
            "accountType": "full",
            "firstName": None,
            "lastName": None,
            "secretUserId": None,
            "language": "en",
            "parent_involvement": None,
        }
        """Fields to import into Curious for parent accounts."""

    import_625: Final[FieldList] = FieldList(
        [
            "additional",
            "address1",
            "address_2",
            "adult_address",
            "adult_apt",
            "adult_city",
            "adult_state",
            "adult_zip",
            "aptnumber1",
            "aptnumber_2",
            "ci_date",
            "ci_forms_due",
            "city1",
            "city_2",
            "complete_parent_second_guardian_consent",
            "dob",
            "email",
            "email_2",
            "enroll_date",
            "first_name",
            "futurecontact",
            "futurecontact_2",
            "gender",
            "gender_other",
            "genderpronoun",
            "genderpronoun_parent1",
            "genderpronoun_parent2",
            "genderpronounother",
            "guardian2_consent_due",
            "guardian_relation",
            "guardian_relation_2",
            "guardian_relation_other",
            "guardian_relation_other_2",
            "last_name",
            "middlename_y",
            "mrn",
            "parent_last_name_2",
            "parentfirstname",
            "parentfirstname_2",
            "parentlastname",
            "permission_audiovideo",
            "permission_audiovideo_participant",
            "permission_collab",
            "phone",
            "phone_2",
            "prefname",
            "q_devices",
            "q_devices_info",
            "q_silverallergy",
            "record_id",
            "role_consent",
            "same_address",
            "sex",
            "sibling",
            "siblingdob",
            "siblingfirstname",
            "siblinglastname",
            "state1",
            "state_2",
            "zipcode1",
            "zipcode_2",
        ]
    )
    """Fields to import into REDCap PID 625."""

    class rename:
        """Mappings to rename from one DataFrame to another."""

        @deprecated(
            "Deprecated in v1.10.0. Use `Fields.rename.redcap_operations_to_curious`"
        )
        class redcap247_to_curious(ColumnRenameMapping):
            """Columns to rename from REDCap PID 247 to Curious."""

            child: Final[dict[str, str]] = {
                "prefname": "nickname",
                "prefame_1821": "nickname",
                "consent1": "firstName",
                "consent1_1821": "firstName",
                "consent5": "lastName",
                "consent5_1821": "lastName",
                "mrn": "secretUserId",
            }
            """Columns to rename for child accounts from REDCap PID 247 to Curious."""
            parent: Final[dict[str, str]] = {
                "email_1821": "email",
                "parentfirstname": "firstName",
                "parent_last_name": "lastName",
                "parent_last_name_1821": "lastName",
                "mrn": "secretUserId",
            }
            """Columns to rename for parent accounts from REDCap PID 247 to Curious."""

        class redcap_operations_to_curious(ColumnRenameMapping):
            """Columns to rename from REDCap PID 625 to Curious."""

            child: Final[dict[str, str]] = {
                "curious_email_child": "email",
                "email": "_parent_email",
                "prefname": "nickname",
                "first_name": "firstName",
                "last_name": "lastName",
                "curious_password_child": "password",
                "mrn": "secretUserId",
            }
            """Columns to rename for child accounts from REDCap PID 625 to Curious."""
            parent: Final[dict[str, str]] = {
                "email_1821": "email",
                "parentfirstname": "firstName",
                "parentlastname": "lastName",
                "r_id": "secretUserId",
            }
            """Columns to rename for parent accounts from REDCap PID 247 to Curious."""

        class redcap_consent_to_redcap_responder_tracking(ColumnRenameMapping):
            """Columns to rename for REDCap PID 247 to PID 879."""

            child: Final[dict[str, str]] = {
                "dob": "child_dob",
                "consent1": "child_fname",
                "mrn": "mrn",
            }
            """Columns for child participant."""

            adult_participant: Final[dict[str, str]] = {
                "dob_1821": "child_dob",
                "consent1_1821": "child_fname",
                "mrn": "mrn",
            }
            """Columns for adult participant."""

            responder: Final[dict[str, str]] = {
                "email": "resp_email",
                "parentfirstname": "resp_fname",
                "parent_last_name": "resp_lname",
                "phone": "resp_phone",
            }
            """Columns for parent / responder 1 of child participant."""

            responder2: Final[dict[str, str]] = {
                "email_2": "resp_email",
                "parentfirstname_2": "resp_fname",
                "parent_last_name_2": "resp_lname",
                "phone_2": "resp_phone",
            }
            """Columns for parent / responder 2 of child participant"""

            responder_adult1: Final[dict[str, str]] = {
                "email_1821": "email",
                "parentfirstname_1821": "resp_fname",
                "parent_last_name_1821": "resp_lname",
                "phone_1821": "resp_phone",
            }
            """Columns for parent / responder 1 of adult participant."""

            responder_adult2: Final[dict[str, str]] = {
                "email_2_1821": "resp_email",
                "parentfirstname_2_1821": "resp_fname",
                "parent_last_name_2_1821": "resp_lname",
                "phone_2_1821": "resp_phone",
            }
            """Columns for parent / responder 2 of adult participant."""

        redcap_consent_to_redcap_operations: Final[dict[str, str]] = {
            "additional_1821": "additional",
            "address1_1821": "address1",
            "address2": "address_2",
            "aptnumber": "aptnumber1",
            "aptnumber_1821": "aptnumber1",
            "aptnumber_2": "aptnumber_2",
            "aptnumber_2_1821": "aptnumber_2",
            "city": "city1",
            "city_1821": "city1",
            "city_2_1821": "city_2",
            "parent_second_guardian_"
            "consent_complete": "complete_parent_second_guardian_consent",
            "dob_1821": "dob",
            "adult_email": "email",
            "email_1821": "email",
            "email_2_1821": "email_2",
            "consent1": "first_name",
            "consent1_1821": "first_name",
            "gender_1821": "gender",
            "gender_other_1821": "gender_other",
            "genderpronoun_1821": "genderpronoun",
            "genderpronounother_1821": "genderpronounother",
            "genderpronoun_2_1821": "genderpronoun_parent1",
            "genderpronoun_parent2": "genderpronoun_parent2",
            "genderpronoun_parent2_1821": "genderpronoun_parent2",
            "par_rel": "guardian_relation",
            "par_rel_2": "guardian_relation_2",
            "consent5": "last_name",
            "consent5_1821": "last_name",
            "middlename_2_1821": "middlename_y",
            "parent_last_name_2_1821": "parent_last_name_2",
            "parentfirstname_1821": "parentfirstname",
            "parentfirstname_2_1821": "parentfirstname_2",
            "parent_last_name": "parentlastname",
            "parent_last_name_1821": "parentlastname",
            "permission_audiovideo_1113": "permission_audiovideo_participant",
            "permission_audiovideo_1417": "permission_audiovideo_participant",
            "phone_1821": "phone",
            "phone_2_1821": "phone_2",
            "prefname_1821": "prefname",
            "biosex": "sex",
            "biosex_1821": "sex",
            "q_devices_1821": "q_devices",
            "q_devices_info_1821": "q_devices_info",
            "q_silverallergy_1821": "q_silverallergy",
            "sibling_1821": "sibling",
            "siblingdob_1821": "siblingdob",
            "siblingfirstname_1821": "siblingfirstname",
            "siblinglastname_1821": "siblinglastname",
            "state": "state1",
            "state_1821": "state1",
            "state_2_1821": "state_2",
            "zipcode": "zipcode1",
            "zipcode_1821": "zipcode1",
            "zipcode_2_1821": "zipcode_2",
        }
        """Columns to rename from REDCap PID 247 to REDCap PID 625."""


class Values:
    """Values for REDCap fields."""

    class PID247(ValueClass):
        """Values for PID 247 ― Healthy Brain Network Study Consent (IRB Approved)."""

        guardian2_consent = FieldDescriptor(
            {
                "No": "0",
                "Yes": "1",
                "Yes, but email not yet available": "2",
                "Not Applicable (Adult Participant)": "3",
            }
        )
        """Second guardian consent required?"""

        intake_ready = FieldDescriptor(
            {
                "Not sent": "0",
                "Ready to Send to Intake Redcap": "1",
                "Participant information already sent to HBN - "
                "Intake Redcap project": "2",
            }
        )
        """
        Is parent ready to receive the intake survey?

        This will create the participant profile in the HBN - Intake and Curious (TEMP for Transition) project and send out the survey.
        """  # noqa: E501

        parent_second_guardian_consent_complete = RedcapComplete
        """Form status: Complete?"""

        permission_collab = FieldDescriptor(
            {
                "YES, you may share my child's records.": "1",
                "NO, you may not share my child's records.": "2",
            }
        )
        """Please indicate whether or not we may receive your child's records from, and share your child's records with partnering scientific institution(s)."""  # noqa: E501

    class PID625(ValueClass):
        """Values for PID 625 HBN - Operations and Data Collection."""

        complete_parent_second_guardian_consent = FieldDescriptor(
            {
                **RedcapComplete.value_dict,
                "Not Required": "3",
                "Not Applicable (Adult Participant)": "4",
            }
        )
        """Second guardian consent form complete?"""

        curious_account_created_account_created_response = FieldDescriptor(
            {"I confirm that I have created a Curious account": "1"}
        )
        """Please click below to confirm that you have created a Curious account"""

        curious_account_created_complete = RedcapComplete
        """Form status: Complete?"""
        curious_account_created_responder_complete = RedcapComplete
        curious_account_created_child_complete = RedcapComplete

        enrollment_complete = FieldDescriptor(
            {
                "Not Sent": "0",
                "Ready to Send to Curious": "1",
                "Parent and Participant information already sent to Curious": "2",
            }
        )
        """Is enrollment complete and we can create parent and participant profiles in Curious?"""  # noqa: E501

        permission_audiovideo_participant = FieldDescriptor(
            {
                "Not Applicable: no assent required": "0",
                "YES, you may record me during participation": "1",
                "NO, you may not record me during participation": "2",
            }
        )
        """Participant giving permission to audio and/or video record during study activities."""  # noqa: E501

        permission_collab = FieldDescriptor({"Yes": "0", "No": "1"})
        """Permission to share your child's records with partnering scientific institution(s)."""  # noqa: E501
