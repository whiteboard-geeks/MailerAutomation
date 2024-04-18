import os
import logging
from datetime import datetime
from base64 import b64encode

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def parse_delivery_information(tracking_data):
    delivery_information = {}
    delivery_tracking_data = tracking_data['tracking_details'][-1]
    delivery_information['delivery_city'] = delivery_tracking_data['tracking_location']['city'].title()
    delivery_information['delivery_state'] = delivery_tracking_data['tracking_location']['state'].upper()

    delivery_datetime = datetime.strptime(delivery_tracking_data['datetime'], '%Y-%m-%dT%H:%M:%SZ')
    delivery_information['delivery_date'] = delivery_datetime.date()
    delivery_information['delivery_date_readable'] = delivery_datetime.strftime('%a %-m/%-d')
    delivery_information["date_and_location_of_mailer_delivered"] = f"{delivery_information['delivery_date_readable']} to {delivery_information['delivery_city']}, {delivery_information['delivery_state']}"
    delivery_information["location_delivered"] = f"{delivery_information['delivery_city']}, {delivery_information['delivery_state']}"

    logger.info(f"Delivery information parsed: {delivery_information}")
    return delivery_information


def post_query_to_close(query):
    CLOSE_API_KEY = os.environ['CLOSE_API_KEY']
    CLOSE_ENCODED_KEY = b64encode(f'{CLOSE_API_KEY}:'.encode()).decode()
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
            logger.info("No more pages to fetch from Close API.")
            break  # Exit the loop if there's no cursor, indicating no more pages
        query['cursor'] = cursor  # Update the cursor for the next request

    logger.info(f"Data returned from Close API: {data_to_return}")
    return data_to_return  # Return the aggregated results


def update_delivery_information_for_lead(lead_id, delivery_information):
    def verify_delivery_information_updated(response_data, lead_update_data):
        for key, value in lead_update_data.items():
            if key not in response_data or response_data[key] != value:
                return False
        return True
    CLOSE_API_KEY = os.environ['CLOSE_API_KEY']
    CLOSE_ENCODED_KEY = b64encode(f'{CLOSE_API_KEY}:'.encode()).decode()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {CLOSE_ENCODED_KEY}'
    }

    custom_field_ids = {
        "date_and_location_of_mailer_delivered": {
            "type": "text",
            "value": "custom.cf_DTgmXXPozUH3707H1MYu2PhhDznJjWbtmDcb7zme5a9"
        },
        "package_delivered": {
            "type": "dropdown_single",
            "value": "custom.cf_wkZ5ptOR1Ro3YPxJPYipI35M7ticuYvJHFgp2y4fzdQ"
        },
        "state_delivered": {
            "type": "text",
            "value": "custom.cf_vxfsYfTrFk6oYrnSx0ViYrUMpE7y5sxi0NnRgTyOf30"
        },
        "city_delivered": {
            "type": "text",
            "value": "custom.cf_1hWUFxiA6QhUXrYT3lDh96JSWKxVBBAKCB3XO8EXGUW"
        },
        "date_delivered": {
            "type": "date",
            "value": "custom.cf_jVU4YFLX5bDq2dRjvBapPYAJxGP0iQWid9QC7cQjSCR"
        },
        "date_delivered_readable": {
            "type": "text",
            "value": "custom.cf_jGC3O9doWfvwFV49NBIRGwA0PFIcKMzE0h8Zf65XLCQ"
        },
        "location_delivered": {
            "type": "text",
            "value": "custom.cf_hPAtbaFuztYBQcYVqsk4pIFV0hKvnlb696TknlzEERS"
        }
    }
    lead_update_data = {
        custom_field_ids["date_and_location_of_mailer_delivered"]["value"]: delivery_information["date_and_location_of_mailer_delivered"],
        custom_field_ids["package_delivered"]["value"]: "Yes",
        custom_field_ids["state_delivered"]["value"]: delivery_information["delivery_state"],
        custom_field_ids["city_delivered"]["value"]: delivery_information["delivery_city"],
        custom_field_ids["date_delivered"]["value"]: delivery_information["delivery_date"].isoformat(),
        custom_field_ids["date_delivered_readable"]["value"]: delivery_information["delivery_date_readable"],
        custom_field_ids["location_delivered"]["value"]: delivery_information["location_delivered"]
    }

    response = requests.put(f'https://api.close.com/api/v1/lead/{lead_id}', json=lead_update_data, headers=headers)
    response_data = response.json()
    data_updated = verify_delivery_information_updated(response_data, lead_update_data)
    if not data_updated:
        logger.error(f"Delivery information update failed for lead {lead_id}.")
        raise Exception("Close accepted the lead, but the fields did not update.")
    logger.info(f"Delivery information updated for lead {lead_id}: {data_updated}")
    return response_data


def create_package_delivered_custom_activity_in_close(lead_id, delivery_information):
    CLOSE_API_KEY = os.environ['CLOSE_API_KEY']
    CLOSE_ENCODED_KEY = b64encode(f'{CLOSE_API_KEY}:'.encode()).decode()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {CLOSE_ENCODED_KEY}'
    }

    custom_activity_field_ids = {
        "date_and_location_of_mailer_delivered": {
            "type": "text",
            "value": "custom.cf_f652JX1NlPz5P5h7Idqs0uOosb9nomncygP3pJ8GcOS"
        },
        "state_delivered": {
            "type": "text",
            "value": "custom.cf_7wWKPs9vnRZTpgJRdJ79S3NYeT9kq8dCSgRIrVvYe8S"
        },
        "city_delivered": {
            "type": "text",
            "value": "custom.cf_OJXwT7BAZi0qCfdFvzK0hTcPoUUGTxP6bWGIUpEGqOE"
        },
        "date_delivered": {
            "type": "date",
            "value": "custom.cf_wS7icPETKthDz764rkbuC1kQYzP0l88CzlMxoJAlOkO"
        },
        "date_delivered_readable": {
            "type": "text",
            "value": "custom.cf_gUsxB1J9TG1pWG8iC3XYZR9rRXBcHQ86aEJUIFme6LA"
        },
        "location_delivered": {
            "type": "text",
            "value": "custom.cf_Wzp0dZ2D8PQTCKUiKMGsYUVDnURtisF6g9Lwz72WM8m"
        }
    }
    lead_activity_data = {
        "lead_id": lead_id,
        "custom_activity_type_id": "custom.actitype_3KhBfWgjtVfiGYbczbgOWv",  # Activity Type: Mailer Delivered
        custom_activity_field_ids["date_and_location_of_mailer_delivered"]["value"]: delivery_information["date_and_location_of_mailer_delivered"],
        custom_activity_field_ids["state_delivered"]["value"]: delivery_information["delivery_state"],
        custom_activity_field_ids["city_delivered"]["value"]: delivery_information["delivery_city"],
        custom_activity_field_ids["date_delivered"]["value"]: delivery_information["delivery_date"].isoformat(),
        custom_activity_field_ids["date_delivered_readable"]["value"]: delivery_information["delivery_date_readable"],
        custom_activity_field_ids["location_delivered"]["value"]: delivery_information["location_delivered"]
    }

    response = requests.post('https://api.close.com/api/v1/activity/custom/', json=lead_activity_data, headers=headers)
    response_data = response.json()
    logger.info(f"Delivery activity updated for lead {lead_id}: {response.json()}")
    return response_data


def enroll_lead_in_sequence(lead_id):
    CLOSE_API_KEY = os.environ['CLOSE_API_KEY']
    CLOSE_ENCODED_KEY = b64encode(f'{CLOSE_API_KEY}:'.encode()).decode()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {CLOSE_ENCODED_KEY}'
    }
    lead = requests.get(f'https://api.close.com/api/v1/lead/{lead_id}', headers=headers).json()
    first_contact_id = lead['contacts'][0]['id']  # cont_ELEYWa3c32W9J1O6SJbIOhK4M0FIOeM6LMyifXwrpZZ
    consultant = lead['custom.lcf_TRIulkQaxJArdGl2k89qY6NKR0ZTYkzjRdeILo1h5fi']  # Barbara Pigg
    # fn to find consultant's user id - this is who the calls will be assigned to
    # fn to find consultant's email id (the cold email one)
    # call to enroll them in the sequence
    # verify the sequence subscription response somehow

    sequence_subscription_payload = {
        "sequence_id": "seq_1BTljGuCooX0nbFoPihl07",
        "contact_id": first_contact_id,
        "contact_email": "contact@example.org",
        "sender_account_id": "emailacct_0oCdHYhxtl5sV9j3ZwrJI1sUhbOK4FoZjDuTG9I2hej",
        "sender_name": "John Doe",
        "sender_email": "john@salesteam.com",
        "calls_assigned_to": ["user_5nBeMSPhq2qP2sSQMiqupxdEUkRetu79kqXubkhPh22"],
    }

    response = requests.post('https://api.close.com/api/v1/data/search/', json=sequence_subscription_payload, headers=headers)
    response_data = response.json()
    logger.info(f"Sequence subscription response: {response_data}")
    return response_data


@app.route('/delivery_status', methods=['POST'])
def webhook():
    tracking_data = request.json
    if tracking_data.get('status') != "delivered":
        logger.info("Tracking status is not 'delivered'; webhook did not run.")
        return jsonify({"status": "success", "message": "Tracking status is not 'delivered' so did not run."}), 200

    logger.info(f"Received webhook data: {tracking_data}")
    delivery_information = parse_delivery_information(tracking_data)
    close_query_to_find_leads_with_tracking_number = {
        "limit": None,
        "query": {
            "negate": False,
            "queries": [
                {
                    "negate": False,
                    "object_type": "lead",
                    "type": "object_type"
                },
                {
                    "negate": False,
                    "queries": [
                        {
                            "negate": False,
                            "queries": [
                                {
                                    "condition": {
                                        "mode": "exact_value",
                                        "type": "text",
                                        "value": "123456"
                                    },
                                    "field": {
                                        "custom_field_id": "cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii",
                                        "type": "custom_field"
                                    },
                                    "negate": False,
                                    "type": "field_condition"
                                },
                                {
                                    "condition": {
                                        "type": "term",
                                        "values": [
                                            "USPS"
                                        ]
                                    },
                                    "field": {
                                        "custom_field_id": "cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l",
                                        "type": "custom_field"
                                    },
                                    "negate": False,
                                    "type": "field_condition"
                                },
                                {
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
        "results_limit": None,
        "sort": []
    }
    close_leads = post_query_to_close(close_query_to_find_leads_with_tracking_number)
    try:
        if len(close_leads) > 1:  # this would mean there are two leads with the same tracking number
            logger.error("More than one lead found with the same tracking number")
            raise Exception("More than one lead found with the same tracking number")
        update_close_lead = update_delivery_information_for_lead(close_leads[0]["id"], delivery_information)
        logger.info(f"Close lead update: {update_close_lead}")
        create_package_delivered_custom_activity_in_close(close_leads[0]["id"], delivery_information)
        lead_enrollment_response = enroll_lead_in_sequence(close_leads[0]["id"])
        return jsonify({"status": "success", "close_lead_update": update_close_lead, "lead_enrollment_response": lead_enrollment_response}), 200
    except Exception as e:
        logger.error(f"Error updating Close lead: {e}")
        return jsonify({"status": "error", "message": str(e)}), 400


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(debug=True, host='0.0.0.0', port=port)
