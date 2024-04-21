import json
import os
import logging
from datetime import datetime
from base64 import b64encode
from urllib.parse import urlencode

import requests
from flask import Flask, request, jsonify
from celery import Celery
import pytz

app = Flask(__name__)

REDISCLOUD_URL = os.environ.get('REDISCLOUD_URL')
app.config['CELERY_BROKER_URL'] = REDISCLOUD_URL
app.config['CELERY_RESULT_BACKEND'] = REDISCLOUD_URL

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# API Keys
MAILGUN_API_KEY = os.environ.get('MAILGUN_API_KEY')
CLOSE_API_KEY = os.environ['CLOSE_API_KEY']
CLOSE_ENCODED_KEY = b64encode(f'{CLOSE_API_KEY}:'.encode()).decode()
SKYLEAD_API_KEY = os.environ.get('SKYLEAD_API_KEY')


def send_error_email(error_message):
    central_time_zone = pytz.timezone('America/Chicago')
    central_time_now = datetime.now(central_time_zone)
    time_now_formatted = central_time_now.strftime("%Y-%m-%d %H:%M:%S%z")

    mailgun_email_response = requests.post(
        "https://api.mailgun.net/v3/sandbox66451c576acc426db15db39f4a76b250.mailgun.org/messages",
        auth=("api", MAILGUN_API_KEY),
        data={
            "from": "MailerAutomation App <postmaster@sandbox66451c576acc426db15db39f4a76b250.mailgun.org>",
            "to": "Lance Johnson <lance@whiteboardgeeks.com>",
            "subject": f"Package Delivery Webhook Error {time_now_formatted}",
            "text": error_message
        }
    )

    return mailgun_email_response.json()


def send_processing_email(tracking_data):
    central_time_zone = pytz.timezone('America/Chicago')
    central_time_now = datetime.now(central_time_zone)
    time_now_formatted = central_time_now.strftime("%Y-%m-%d %H:%M:%S%z")

    mailgun_email_response = requests.post(
        "https://api.mailgun.net/v3/sandbox66451c576acc426db15db39f4a76b250.mailgun.org/messages",
        auth=("api", MAILGUN_API_KEY),
        data={
            "from": "MailerAutomation App <postmaster@sandbox66451c576acc426db15db39f4a76b250.mailgun.org>",
            "to": "Lance Johnson <lance@whiteboardgeeks.com>",
            "subject": f"Package Delivered {time_now_formatted}: Check Manually",
            "text": tracking_data
        }
    )

    return mailgun_email_response.json()


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
    try:
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

        return data_to_return  # Return the aggregated results
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to post query to Close: {e}")
        send_error_email(f"Failed to post query to Close: {e}")  # Hypothetical function to send error emails
        return None


def update_delivery_information_for_lead(lead_id, delivery_information):
    def verify_delivery_information_updated(response_data, lead_update_data):
        for key, value in lead_update_data.items():
            if key not in response_data or response_data[key] != value:
                return False
        return True
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
        error_message = f"Delivery information update failed for lead {lead_id}."
        logger.error(error_message)
        send_error_email(error_message)  # Send an email when an error occurs
        raise Exception("Close accepted the lead, but the fields did not update.")
    logger.info(f"Delivery information updated for lead {lead_id}: {data_updated}")
    return response_data


def create_package_delivered_custom_activity_in_close(lead_id, delivery_information):
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


@app.route('/delivery_status', methods=['POST'])
def handle_package_delivery_update():
    try:
        tracking_data = request.json['result']
        easy_post_event_id = request.json['id']
        logger.info(f"EasyPost Event ID: {easy_post_event_id}")
        if tracking_data['status'] != "delivered":
            logger.info("Tracking status is not 'delivered'; webhook did not run.")
            return jsonify({"status": "success", "message": "Tracking status is not 'delivered' so did not run."}), 200
        if tracking_data['tracking_details'][-1]['message'] == "Delivered, To Original Sender":
            logger.info("Tracking status is 'delivered', but it is delivered to the original sender; webhook did not run.")
            return jsonify({"status": "success", "message": "Tracking status is 'delivered', but it is delivered to the original sender; webhook did not run."}), 200
        send_processing_email(json.dumps(request.json))
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
                                            "value": tracking_data["tracking_code"]
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
                                                tracking_data['carrier']
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
            return jsonify({"status": "success", "close_lead_update": update_close_lead}), 200
        except Exception as e:
            error_message = f"Error updating Close lead: {e}, lead_id={close_leads[0]['id']}"
            logger.error(error_message)
            send_error_email(error_message)  # Send an email when an error occurs
            return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        error_message = f"Error. {e}, tracking_code={tracking_data['tracking_code']}, carrier={tracking_data['carrier']}"
        logger.error(error_message)
        send_error_email(error_message)  # Send an email when an error occurs
        return jsonify({"status": "error", "message": str(e)}), 400


@celery.task
def check_skylead_for_viewed_profile(contact):
    pass


@app.route('/check_linkedin_connection_status', methods=['POST'])
def check_linkedin_connection_status():
    def add_contact_to_view_profile_campaign_in_skylead(contact):
        linkedin_url = contact['custom.cf_OKNCGTl08BZyjbiPdhBSrWDTmV4bhEaPmVYFURxQphZ']
        email = contact['emails'][0]['email']

        # Skylead request
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': SKYLEAD_API_KEY
        }
        body = {
            'email': email,
            'profileUrl': linkedin_url
        }
        encoded_body = urlencode(body)
        url = 'https://api.multilead.io/api/open-api/v1/campaign/234808/leads'  # 234808 is the campaign number for View Profile
        skylead_response = requests.post(
            url=url,
            headers=headers,
            data=encoded_body
        )
        return skylead_response
    data = request.json
    contact = data['event']['data']
    contact_add_resp_status = add_contact_to_view_profile_campaign_in_skylead(contact)
    # TO-DO FN figure out how long to wait before checking skylead for the profile
    # Must be at least 30 minutes after the intial call
    # Must be during the hours the profile is active (7-5pm CT)
    # On weekdays (unless in debug mode)
    # If the request isn’t received on a weekday

    # TO-DO FN call check_skylead_for_viewed_profile
    # query the specific campaign for leads.
    # find the one lead that has the same linkedinUrl as the contact
    # parse connection information (1st, 2nd, 3rd+)

    # TO-DO FN update Close with connection status
    # update Close with connection status

    # TO-DO Success message. Log the success message somwhere.
    # Maybe email me the success?

    if contact_add_resp_status.status_code == 204:
        return jsonify({"status": "success", "message": "Contact added to Skylead campaign"}), 200
    else:
        return jsonify({"status": "error", "message": "Error adding contact to Skylead campaign"}), 400


@celery.task
def add_and_log_numbers(a, b):
    result = a + b
    logging.info(f"The sum of {a} and {b} is {result}")


@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    a = data.get('number1')
    b = data.get('number2')
    if a is not None and b is not None:
        add_and_log_numbers.apply_async((a, b), countdown=15)
        return {"message": "Numbers will be added and logged."}, 200
    else:
        return {"error": "Invalid data. Please send 'number1' and 'number2'."}, 400


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(debug=True, host='0.0.0.0', port=port)
