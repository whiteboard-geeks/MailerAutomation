web: python app.py
worker: celery -A app.celery worker --loglevel=info
flower: celery -A app.celery flower --port=$PORT