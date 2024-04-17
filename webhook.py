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
    app.run(debug=True, port=8080)
