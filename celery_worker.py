"""
Celery worker configuration.
This module creates and configures the Celery instance for the application.
"""

import os
import logging
from celery import Celery
import structlog
from celery.signals import setup_logging


# Configure structlog for Celery
@setup_logging.connect
def setup_celery_logging(**kwargs):
    """Configure structlog for Celery workers."""
    # Set up structlog processors
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    # Configure structlog based on environment
    if os.environ.get("ENV_TYPE") in ["production", "staging"]:
        # JSON logging for production/staging
        structlog.configure(
            processors=shared_processors
            + [
                structlog.processors.dict_tracebacks,
                structlog.processors.JSONRenderer(),
            ],
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
    else:
        # Dev-friendly console logging for local development
        structlog.configure(
            processors=shared_processors + [structlog.dev.ConsoleRenderer()],
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

    # Set up stdlib logging to work with structlog
    root_logger = logging.getLogger()

    # Only add handler if one doesn't already exist (to prevent duplicates)
    if not root_logger.handlers:
        handler = logging.StreamHandler()

        # Format as JSON for production/staging environments
        if os.environ.get("ENV_TYPE") in ["production", "staging"]:
            formatter = logging.Formatter("%(message)s")
            handler.setFormatter(formatter)

        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)

    # Suppress excessive logging from third-party libraries
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # Don't let Celery override our logging config
    return True


# Get Redis URL from environment
REDISCLOUD_URL = os.environ.get("REDISCLOUD_URL")
if not REDISCLOUD_URL:
    REDISCLOUD_URL = "redis://localhost:6379/0"
    print(f"WARNING: REDISCLOUD_URL not set, using default: {REDISCLOUD_URL}")

# Create Celery instance
celery = Celery(
    "mailer_automation",
    broker=REDISCLOUD_URL,
    backend=REDISCLOUD_URL,
    include=[
        "blueprints.easypost",
        "blueprints.instantly",
        "app",
    ],
)

# Configure Celery
celery.conf.update(
    result_expires=3600,  # Results expire after 1 hour
    timezone="America/Chicago",
    broker_connection_retry_on_startup=True,
    worker_hijack_root_logger=False,  # Don't hijack the root logger
    worker_redirect_stdouts=True,  # Redirect stdout/stderr to the logger
    worker_redirect_stdouts_level="INFO",  # Level for stdout/stderr logs
)

if __name__ == "__main__":
    celery.start()
