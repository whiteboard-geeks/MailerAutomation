import os

from flask import Flask, request, jsonify

app = Flask(__name__)


def transform_data(data):
    return data['user'].upper()


@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    print("Received webhook data:", data)
    transformed_data = transform_data(data)
    return jsonify({"status": "success", "message": "Data received", "transformed_data": transformed_data}), 200


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(debug=False, host='0.0.0.0', port=port)
