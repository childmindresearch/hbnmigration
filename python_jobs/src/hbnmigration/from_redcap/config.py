"""Nonsesitive for REDCap API calls."""

from collections import UserList
from dataclasses import dataclass, field


class FieldList(UserList):
    """Return list of REDCap fields with overloaded `__str__`."""

    def __str__(self) -> str:
        """Return as expected for REDCap API."""
        return ",".join(self.data)


@dataclass
class Fields:
    """Return fields for REDCap API calls."""

    export_247: UserList[str] = field(
        default_factory=lambda: FieldList(
            [
                "record_id",
                "mrn",
                "consent1",
                "consent2",
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
    )
    import_744: UserList[str] = field(
        default_factory=lambda: FieldList(
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
    )
    rename_247_to_744: dict[str, str] = field(
        default_factory=lambda: {
            "consent1": "first_name",
            "consent2": "last_name",
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
    )
