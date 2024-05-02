@app.before_request
def before_request():
    # Assign a unique ID to each request
    request.id = uuid.uuid4().hex
    # Optionally, you can also add the request ID to the Flask g object if you plan to access it globally in other parts of your application
    g.request_id = request.id

class RequestFormatter(logging.Formatter):
    def format(self, record):
        # Add the request ID to each log record
        record.request_id = getattr(request, 'id', 'unknown')
        self.adsf = 'asdf'
        return super().format(record)

    def func():
        self.adsf


# Configure logging
handler = logging.StreamHandler()
formatter = RequestFormatter('[%(asctime)s] %(request_id)s %(levelname)s in %(module)s: %(message)s')
handler.setFormatter(formatter)
app.logger.addHandler(handler)
app.logger.setLevel(logging.INFO)

# use like:
@app.route('/')
def index():
    app.logger.info('Processing the main page')
    return "Hello, World!"