import os
from datetime import datetime
from base64 import b64encode

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)


def parse_delivery_information(tracking_data):
    delivery_information = {}
    delivery_tracking_data = tracking_data['tracking_details'][-1]
    delivery_information['delivery_city'] = delivery_tracking_data['tracking_location']['city'].title()
    delivery_information['delivery_state'] = delivery_tracking_data['tracking_location']['state'].upper()

    delivery_datetime = datetime.strptime(delivery_tracking_data['datetime'], '%Y-%m-%dT%H:%M:%SZ')
    delivery_information['delivery_date'] = delivery_datetime.date()
    delivery_information['delivery_date_readable'] = delivery_datetime.strftime('%a %-m/%-d')
    delivery_information['']

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
            break  # Exit the loop if there's no cursor, indicating no more pages
        query['cursor'] = cursor  # Update the cursor for the next request

    return data_to_return  # Return the aggregated results


@app.route('/delivery_status', methods=['POST'])
def webhook():
    tracking_data = request.json
    print("Received webhook data:", tracking_data)
    carrier = tracking_data['carrier']
    tracking_number = tracking_data['tracking_code']
    if tracking_data['status'] == "delivered":
        delivery_information = parse_delivery_information(tracking_data)
        status = "delivered"
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
        if len(close_leads) > 1:  # this would mean there are two leads with the same tracking number
            raise Exception("More than one lead found with the same tracking number")

        print(close_leads)
    else:
        delivery_information = "in transit"
    return jsonify({"status": "success", "carrier": carrier, "tracking_number": tracking_number, "delivery_status": status, "delivery_information": delivery_information}), 200


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(debug=True, host='0.0.0.0', port=port)
