"""Nonsesitive for REDCap API calls."""

from collections import UserList
from typing import Final

from ..utility_functions import FieldDescriptor, ValueClass

RedcapComplete = FieldDescriptor(
    {"Incomplete": "0", "Unverified": "1", "Complete": "2"}
)
"""Common `*_complete` values."""


class FieldList(UserList):
    """Return list of REDCap fields with overloaded `__str__`."""

    def __str__(self) -> str:
        """Return as expected for REDCap API."""
        return ",".join(self.data)


class Fields:
    """Return fields for REDCap API calls."""

    class export_247:
        """Fields to export from REDCap PID 247."""

        for_curious: Final[FieldList] = FieldList(
            [
                "enrollment_complete",
                "record_id",
                "mrn",
                "adult_enrollment_form_complete",
                "parent_involvement",
                "parent_involvement___1",
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
        for_redcap744: Final[FieldList] = FieldList(
            [
                "record_id",
                "mrn",
                "consent1",
                "consent5",
                "prefname",
                "biosex",
                "gender",
                "gender_other",
                "genderpronoun",
                "genderpronounother",
                "dob",
                "enroll_date",
                "email",
                "parentfirstname",
                "parent_last_name",
                "guardian_relation",
                "guardian_relation_other",
                "phone",
                "permission_audiovideo",
                "permission_audiovideo_1113",
                "permission_audiovideo_1417",
                "permission_collab",
                "middlename_y",
                "futurecontact",
                "futurecontact_2",
                "parentfirstname_2",
                "parent_last_name_2",
                "guardian_relation_2",
                "guardian_relation_other_2",
                "email_2",
                "phone_2",
                "parent_second_guardian_consent_complete",
                "intake_ready",
                "consent1_1821",
                "consent5_1821",
                "biosex_1821",
                "gender_1821",
                "gender_other_1821",
                "genderpronoun_1821",
                "genderpronounother_1821",
                "dob_1821",
                "adult_email",
                "parentfirstname_1821",
                "parent_last_name_1821",
                "par_rel",
                "middlename_2_1821",
                "email_1821",
                "phone_1821",
                "parentfirstname_2_1821",
                "parent_last_name_2_1821",
                "par_rel_2",
                "email_2_1821",
                "phone_2_1821",
                "ci_date",
                "ci_forms_due",
                "guardian2_consent_due",
                "role_consent",
                "guardian2_consent",
            ]
        )
        """Fields to export from REDCap PID 247 for import into REDCap PID 744."""

    class import_curious:
        """Fields to import into Curious."""

        child: Final[dict[str, int | str | None]] = {
            "nickname": None,
            "role": "respondent",
            "tag": "child",
            "accountType": "limited",
            "firstName": None,
            "lastName": None,
            "secretUserId": None,
            "language": "en",
            "parent_involvement": None,
        }
        """Fields to import into Curious for child accounts."""
        parent: Final[dict[str, int | str | None]] = {
            "email": None,
            "nickname": None,
            "role": "respondent",
            "tag": "parent",
            "accountType": "full",
            "firstName": None,
            "lastName": None,
            "secretUserId": None,
            "language": "en",
            "parent_involvement": None,
        }
        """Fields to import into Curious for parent accounts."""

    import_744: Final[FieldList] = FieldList(
        [
            "record_id",
            "mrn",
            "first_name",
            "last_name",
            "prefname",
            "sex",
            "gender",
            "gender_other",
            "genderpronoun",
            "genderpronounother",
            "dob",
            "enroll_date",
            "email",
            "parentfirstname",
            "parentlastname",
            "guardian_relation",
            "guardian_relation_other",
            "phone",
            "permission_audiovideo",
            "permission_audiovideo_participant",
            "permission_collab",
            "middlename_y",
            "futurecontact",
            "futurecontact_2",
            "parentfirstname_2",
            "parent_last_name_2",
            "guardian_relation_2",
            "guardian_relation_other_2",
            "email_2",
            "phone_2",
            "complete_parent_second_guardian_consent",
            "ci_date",
            "ci_forms_due",
            "guardian2_consent_due",
            "role_consent",
        ]
    )
    """Fields to import into REDCap PID 744."""

    class rename:
        """Mappings to rename from one DataFrame to another."""

        class redcap247_to_curious:
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
                "parentfirstname_1821": "firstName",
                "parent_last_name": "lastName",
                "parent_last_name_1821": "lastName",
                "mrn": "secretUserId",
            }
            """Columns to rename for parent accounts from REDCap PID 247 to Curious."""

        redcap247_to_redcap744: Final[dict[str, str]] = {
            "consent1": "first_name",
            "consent5": "last_name",
            "biosex": "sex",
            "parent_last_name": "parentlastname",
            "parent_second_guardian"
            "_consent_complete": "complete_parent_second_guardian_consent",
            "permission_audiovideo_1113": "permission_audiovideo_participant",
            "permission_audiovideo_1417": "permission_audiovideo_participant",
            "consent1_1821": "first_name",
            "consent5_1821": "last_name",
            "prefname_1821": "prefname",
            "biosex_1821": "sex",
            "gender_1821": "gender",
            "gender_other_1821": "gender_other",
            "genderpronoun_1821": "genderpronoun",
            "genderpronounother_1821": "genderpronounother",
            "dob_1821": "dob",
            "adult_email": "email",
            "parentfirstname_1821": "parentfirstname",
            "parent_last_name_1821": "parentlastname",
            "par_rel": "guardian_relation",
            "middlename_2_1821": "middlename_y",
            "email_1821": "email",
            "phone_1821": "phone",
            "parentfirstname_2_1821": "parentfirstname_2",
            "parent_last_name_2_1821": "parent_last_name_2",
            "par_rel_2": "guardian_relation_2",
            "email_2_1821": "email_2",
            "phone_2_1821": "phone_2",
        }
        """Columns to rename from REDCap PID 247 to REDCap PID 744."""


class Values:
    """Values for REDCap fields."""

    class PID247(ValueClass):
        """Values for PID 247 ― Healthy Brain Network Study Consent (IRB Approved)."""

        enrollment_complete = FieldDescriptor(
            {
                "Not Sent": "0",
                "Ready to Send to Curious": "1",
                "Parent and Participant information already sent to Curious": "2",
            }
        )
        """Is enrollment complete and we can create parent and participant profiles in Curious?"""  # noqa: E501

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

    class PID744(ValueClass):
        """Values for PID 744 HBN - Intake and Curious (TEMP for Transition)."""

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

        permission_collab = FieldDescriptor({"Yes": "0", "No": "1"})
        """Permission to share your child's records with partnering scientific institution(s)."""  # noqa: E501
