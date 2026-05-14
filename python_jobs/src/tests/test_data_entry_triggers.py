"""Test data entry triggers."""

from fastapi.testclient import TestClient

from hbnmigration.from_redcap.data_entry_triggers import app

client = TestClient(app)


def test_root_endpoint():
    """Test the root endpoint returns the correct message."""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "REDCap to Curious Migration Service"}


def test_health_endpoint():
    """Test the health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_redcap_data_access_trigger_accepted():
    """Test the REDCap Data Entry Trigger endpoint accepts valid payloads."""
    # REDCap sends POST parameters via form data, which FastAPI Depends()
    # maps to query parameters when using a Pydantic model by default.
    payload = {
        "project_id": 625,
        "instrument": "enrollment_internal_use_only",
        "record": "12345",
        "redcap_event_name": "event_1_arm_1",
        "redcap_url": "https://redcap.example.com/",
        "project_url": "https://redcap.example.com/pid=625",
    }

    response = client.post("/redcap-data-access-trigger", params=payload)

    assert response.status_code == 200
    assert response.json() == {
        "status": "accepted",
        "message": "Trigger accepted for instrument enrollment_internal_use_only",
        "project": 625,
    }


def test_redcap_data_access_trigger_unhandled_project():
    """Test the trigger handles projects that don't match the case statements."""
    payload = {
        "project_id": 999,  # Not 625
        "instrument": "some_other_instrument",
        "record": "99999",
    }

    response = client.post("/redcap-data-access-trigger", params=payload)

    assert response.status_code == 200
    assert response.json() == {
        "status": "accepted",
        "message": "Trigger accepted for instrument some_other_instrument",
        "project": 999,
    }
