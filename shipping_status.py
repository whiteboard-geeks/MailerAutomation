import os
import requests
from base64 import b64encode

# First, we want to get the leads that are undelivered and see if they've been delivered

# Get the Close API key from the environment variable
CLOSE_API_KEY = os.environ['CLOSE_API_KEY']
CLOSE_ENCODED_KEY = b64encode(f"{CLOSE_API_KEY}:".encode()).decode()
# Define the query
query = {
    "query": {
        "negate": False,
        "queries": [
            {
                "_comment": "Setting the object type to lead",
                "negate": False,
                "object_type": "lead",
                "type": "object_type"
            },
            {
                "_comment": "Adding the filters",
                "negate": False,
                "queries": [
                    {
                        "negate": False,
                        "queries": [
                            {
                                "_comment": "Tracking Number present = True",
                                "condition": {
                                    "type": "exists"
                                },
                                "field": {
                                    "custom_field_id": "cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii",
                                    "type": "custom_field"
                                },
                                "negate": False,
                                "type": "field_condition"
                            },
                            {
                                "_comment": "Carrier present = True",
                                "condition": {
                                    "type": "exists"
                                },
                                "field": {
                                    "custom_field_id": "cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l",
                                    "type": "custom_field"
                                },
                                "negate": False,
                                "type": "field_condition"
                            },
                            {
                                "_comment": "Package Delivered not present = True",
                                "condition": {
                                    "type": "exists"
                                },
                                "field": {
                                    "custom_field_id": "cf_wkZ5ptOR1Ro3YPxJPYipI35M7ticuYvJHFgp2y4fzdQ",
                                    "type": "custom_field"
                                },
                                "negate": True,
                                "type": "field_condition"
                            },
                            {
                                "_comment": "lead_source = Mailer",
                                "condition": {
                                    "type": "term",
                                    "values": [
                                        "Mailer"
                                    ]
                                },
                                "field": {
                                    "custom_field_id": "lcf_m8vYwl21cyOo53d97DYSMQDzFnt6cxoSMQ84pAKIN0e",
                                    "type": "custom_field"
                                },
                                "negate": False,
                                "type": "field_condition"
                            }
                        ],
                        "type": "and"
                    }
                ],
                "type": "and"
            }
        ],
        "type": "and"
    },
    "_fields": {
        "lead": ["name", "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii"]
    },
    "include_counts": True
}

# Define the headers
headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Basic {CLOSE_ENCODED_KEY}'
}

# Make the request
response = requests.post('https://api.close.com/api/v1/lead/', json=query, headers=headers)
print(response.json())
