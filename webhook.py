from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    print("Received webhook data:", data)
    return jsonify({"status": "success", "message": "Data received"}), 200


if __name__ == '__main__':
    app.run(debug=True, port=8080)
