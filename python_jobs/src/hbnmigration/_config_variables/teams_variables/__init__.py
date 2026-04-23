"""Variables for Microsoft Teams."""


class Webhooks:
    """Webhooks for Microsoft Teams."""

    def __init__(self) -> None:
        """Initialize webhooks."""
        self.links: dict[str, str] = {
            "Send webhook alerts to 🔴 MS Fabric - Failures": "https://"
            "default7e9101c66f8942a180ba1af9e680cc.3a.environment.api.powerplatform.com:"
            "443/powerautomate/automations/direct/workflows/"
            "ce868372d48e4470862224377774aaca/triggers/manual/paths/invoke?"
            "api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&"
            "sig=mT8y-L3tElw06lJ2jfiknvM4ypsokdR16zJfJTfqMSY"
        }
        """Links for Microsoft Teams webhooks, keyed by name."""
