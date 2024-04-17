import os
from datetime import datetime

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

    return delivery_information


@app.route('/delivery_status', methods=['POST'])
def webhook():
    tracking_data = request.json
    print("Received webhook data:", tracking_data)
    carrier = tracking_data['carrier']
    tracking_number = tracking_data['tracking_code']
    if tracking_data['status'] == "delivered":
        delivery_information = parse_delivery_information(tracking_data)
        status = "delivered"
    else:
        delivery_information = "in transit"
    return jsonify({"status": "success", "carrier": carrier, "tracking_number": tracking_number, "delivery_status": status, "delivery_information": delivery_information}), 200


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(debug=True, host='0.0.0.0', port=port)
