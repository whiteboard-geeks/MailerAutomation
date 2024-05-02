import pytest
from app import app
import json

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

@pytest.fixture
def tracker_updated_payload():
    with open('tests/webhook_payloads/tracker.updated.json') as f:
        return json.load(f)

def test_handle_package_delivery_update_out_for_delivery(client, tracker_updated_payload):
    # Modify the payload to simulate "out_for_delivery" status
    tracker_updated_payload['result']['status'] = 'out_for_delivery'
    
    response = client.post('/delivery_status', json=tracker_updated_payload)
    assert response.status_code == 200
    assert response.json['status'] == 'success'

def test_handle_package_delivery_update_delivered(client, tracker_updated_payload):
    # Modify the payload to simulate "delivered" status
    tracker_updated_payload['result']['status'] = 'delivered'
    
    response = client.post('/delivery_status', json=tracker_updated_payload)
    assert response.status_code == 200
    assert response.json['status'] == 'success'

def test_handle_package_delivery_update_error(client, tracker_updated_payload):
    # Introduce an error in the payload
    del tracker_updated_payload['result']['tracking_code']  # Remove required field to simulate error
    
    response = client.post('/delivery_status', json=tracker_updated_payload)
    assert response.status_code == 400
    assert response.json['status'] == 'error'
