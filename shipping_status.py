import os
import requests
import requests_cache
from base64 import b64encode


requests_cache.install_cache('cache')
cache = requests_cache.get_cache()

CLOSE_API_KEY = os.environ['CLOSE_API_KEY']
CLOSE_ENCODED_KEY = b64encode(f'{CLOSE_API_KEY}:'.encode()).decode()
EASYPOST_API_KEY = os.environ['EASYPOST_API_KEY']
EASYPOST_ENCODED_KEY = b64encode(f'{EASYPOST_API_KEY}:'.encode()).decode()
# Define the query for leads that are undelivered
query_leads_with_undelivered_packages_in_close = {
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
def post_query_to_close(query):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {CLOSE_ENCODED_KEY}'
    }

    data_to_return = []
    while True:
        # Make the request
        response = requests.post('https://api.close.com/api/v1/data/search/', json=query, headers=headers)
        response_data = response.json()

        if 'data' in response_data:
            data_to_return.extend(response_data['data'])  # Use extend to flatten the list

        # Update the cursor from the response, or break if no cursor is present
        cursor = response_data.get('cursor')
        if not cursor:
            break  # Exit the loop if there's no cursor, indicating no more pages
        query['cursor'] = cursor  # Update the cursor for the next request

    return data_to_return  # Return the aggregated results


def get_shipping_status_from_easypost(tracking_number, carrier):
    url = "https://api.easypost.com/v2/trackers"

    payload = {
        "tracker": {
            "tracking_code": tracking_number,
            "carrier": carrier
        }
    }

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {EASYPOST_ENCODED_KEY}'
    }

    response = requests.post(url, headers=headers, json=payload)
    print(response.json)
    return response.json()


query_leads_with_undelivered_packages_in_close = {
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
leads_with_package_undelivered_in_close = post_query_to_close(query_leads_with_undelivered_packages_in_close)
print(leads_with_package_undelivered_in_close)

tracking_number = "9400136105536731108085"
carrier = "USPS"
shipping_status = get_shipping_status_from_easypost(tracking_number, carrier)
