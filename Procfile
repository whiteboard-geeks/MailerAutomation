web: gunicorn app:flask_app
worker: celery -A celery_worker.celery worker --loglevel=info
worker_temporal: python -m temporal.worker
# Add beat process if you want to use scheduled tasks
# beat: celery -A celery_worker.celery beat --loglevel=info