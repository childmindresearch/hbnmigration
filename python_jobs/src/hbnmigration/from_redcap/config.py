"""Nonsesitive for REDCap API calls."""

from collections import UserList
from typing import Final


class FieldList(UserList):
    """Return list of REDCap fields with overloaded `__str__`."""

    def __str__(self) -> str:
        """Return as expected for REDCap API."""
        return ",".join(self.data)


class Fields:
    """Return fields for REDCap API calls."""

    export_247: Final[FieldList] = FieldList(
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
        ]
    )
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
        ]
    )
    rename_247_to_744: Final[dict[str, str]] = {
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


class _FlippedDescriptor:
    """Descriptor that returns a flipped version of the class."""

    def __get__(self, _obj, owner):
        if not hasattr(owner, "_flipped_cache"):
            flipped_attrs = {}
            for attr in dir(owner):
                if not attr.startswith("__") and attr not in (
                    "flipped",
                    "_flipped_cache",
                ):
                    value = getattr(owner, attr)
                    if isinstance(value, dict):
                        flipped_attrs[attr] = {v: k for k, v in value.items()}

            owner._flipped_cache = type(
                f"{owner.__name__}Flipped", (ValueClass,), flipped_attrs
            )

        return owner._flipped_cache


class ValueClass:
    """Base class for value classes."""

    flipped = _FlippedDescriptor()


class Values:
    """Values for REDCap fields."""

    class PID247(ValueClass):
        """Values for PID 247 ― Healthy Brain Network Study Consent (IRB Approved)."""

        intake_ready: Final[dict[str, str]] = {
            "Not sent": "0",
            "Ready to Send to Intake Redcap": "1",
            "Participant information already sent to HBN - Intake Redcap project": "2",
        }
        """
        Is parent ready to receive the intake survey?

        This will create the participant profile in the HBN - Intake and Curious (TEMP for Transition) project and send out the survey.
        """  # noqa: E501

        permission_collab: Final[dict[str, str]] = {
            "YES, you may share my child's records.": "1",
            "NO, you may not share my child's records.": "2",
        }
        """Please indicate whether or not we may receive your child's records from, and share your child's records with partnering scientific institution(s)."""  # noqa: E501

    class PID744(ValueClass):
        """Values for PID 744 HBN - Intake and Curious (TEMP for Transition)."""

        permission_collab: Final[dict[str, str]] = {"Yes": "0", "No": "1"}
        """Permission to share your child's records with partnering scientific institution(s)."""  # noqa: E501
