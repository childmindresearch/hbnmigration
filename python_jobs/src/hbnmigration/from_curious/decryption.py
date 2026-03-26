"""Curious decryption."""

from typing import Any, cast, Literal, TypeAlias

import requests

from .._config_variables import curious_variables
from ..config import Config
from ..utility_functions import (
    CuriousAppletEncryption,
    CuriousDecryptedAnswer,
    CuriousEncryptedAnswer,
    tsx,
)

ThingToDecrypt: TypeAlias = Literal["answer", "events"]
THINGS_TO_DECRYPT: list[ThingToDecrypt] = ["answer", "events"]


def pair_qanda(
    decrypted_answer: CuriousDecryptedAnswer,
) -> list[dict[Literal["item", "answer"], dict[str, Any]]]:
    """Return a list of paired questions and answers."""
    return [
        {"item": a, "answer": b}
        for a, b in zip(decrypted_answer["items"], decrypted_answer["answer"])
    ]


def _decrypt(
    what: ThingToDecrypt,
    script,
    encrypted_answer: CuriousEncryptedAnswer,
    password: str,
    applet_encryption: CuriousAppletEncryption,
) -> list[dict]:
    """Decrypt single answer or events list."""
    return cast(
        list[dict],
        tsx(
            script,
            encrypted_answer[what],
            encrypted_answer["userPublicKey"],
            password,
            applet_encryption["accountId"],
            applet_encryption["prime"],
            applet_encryption["base"],
        ),
    )


def decrypt_single(
    encrypted_answer: CuriousEncryptedAnswer,
    applet_encryption: CuriousAppletEncryption,
    password: str,
) -> CuriousDecryptedAnswer:
    """Return a decrypted answer."""
    script = str(
        Config.PROJECT_ROOT / "javascript_jobs/autoexport/src/decryptSingleAnswer.ts"
    )
    return cast(
        CuriousDecryptedAnswer,
        {
            **encrypted_answer,
            **{
                what: _decrypt(
                    what, script, encrypted_answer, password, applet_encryption
                )
                for what in THINGS_TO_DECRYPT
            },
        },
    )


def get_applet_encryption(endpoint: str, token) -> CuriousAppletEncryption:
    """Get encryption for Curious applet."""
    response = requests.get(
        endpoint,
        headers=curious_variables.headers(token),
    )
    if response.status_code == requests.codes["okay"]:
        return response.json()["result"]["encryption"]
    response.raise_for_status()
    raise requests.HTTPError
