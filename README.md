# MailerAutomation

A comprehensive system that integrates with Close CRM, EasyPost, Gmail, and Instantly to automate package tracking and delivery notification workflows.

## Overview

MailerAutomation is a Flask-based application that helps track package shipments and delivery status for leads in Close CRM. The system monitors shipments, updates delivery statuses, and automates follow-up communications based on package delivery events.

## Key Features

- **Close CRM Integration**: Searches and updates leads, creates tasks and activities
- **EasyPost Integration**: Creates and monitors package trackers, processes delivery status webhooks  
- **Gmail Integration**: Processes email notifications and updates
- **Instantly Integration**: Handles email campaign tracking
- **Background Processing**: Uses Celery for asynchronous task processing
- **Robust Logging**: Structured logging with different formats for development and production

## System Architecture

- **Flask Web Application**: Handles HTTP requests and webhook integrations
- **Celery Workers** (deprecated): Process background and scheduled tasks
- **Redis**: Used for Celery task queue and data caching
- **Temporal**: Process background and scheduled tasks (replaces Celery)
  - `WebhookCreateTrackerWorkflow` orchestrates EasyPost tracker creation and Close lead updates
- **Blueprints**:
  - `easypost.py`: Handles package tracking and delivery status updates
  - `gmail.py`: Manages email notification processing
  - `instantly.py`: Integrates with Instantly email campaigns

## Setup Instructions

### Prerequisites

- Python 3.8+
- Redis server
- Close CRM account
- EasyPost account
- Gmail API credentials (for email integration)
- Instantly account (for email campaigns)
- Temporal Cloud account (for background processing)

### Environment Variables

Create a `.env` file with the following variables:

```properties
CLOSE_API_KEY=your_close_api_key
EASYPOST_PROD_API_KEY=your_easypost_production_key
EASYPOST_TEST_API_KEY=your_easypost_test_key
REDISCLOUD_URL=redis://localhost:6379/0
ENV_TYPE=development  # or production, staging
INSTANTLY_API_KEY=your_instantly_api_key
GMAIL_SERVICE_ACCOUNT_INFO=service_account_in_json_string
GMAIL_WEBHOOK_PASSWORD=user_generated_for_sending_emails_with_endpoint
TEMPORAL_API_KEY=your_temporal_api_key
TEMPORAL_NAMESPACE=your_temporal_namespace
TEMPORAL_ADDRESS=your_temporal_address
```

Note: Only set `TEMPORAL_*` variables when you want Flask to connect to Temporal Cloud. Otherwise leave them unset so that Flask connects to the local Temporal Cluster.

### Installation

    python3 -m venv env
    source env/bin/activate
    pip install -r requirements-dev.txt
    # in VSCode: set Python interpreter path to env/bin/python

### Linting

```bash
flake8 .
```

## Usage

### API Endpoints

#### EasyPost Related

- `POST /create_tracker`: Create a new EasyPost tracker
- `POST /delivery_status`: Handle package delivery status updates from EasyPost
- `GET /sync_delivery_status`: Sync delivery statuses from EasyPost

#### Contact Management

- `POST /prepare_contact_list_for_address_verification`: Process contact lists for address verification

#### Gmail Related

- Various endpoints for processing email notifications

#### Instantly Related

- Endpoints for email campaign tracking and management

## Development

### Running the Application Locally

Temporal:

    make temporal-start

Temporal Worker:

    source env/bin/activate
    set -a; source .env; set +a
    make temporal-add-search-attributes
    python -m temporal.worker

Redis:

    docker pull redis
    docker run -p 6379:6379 redis

Celery:

    source env/bin/activate
    set -a; source .env; set +a
    celery -A celery_worker.celery worker --loglevel=info

Flask:

    source env/bin/activate
    set -a; source .env; set +a
    USE_TEMPORAL_FOR_EASYPOST_DELIVERY_STATUS=true FLASK_APP=app.py FLASK_ENV=development flask run --port=8080

### Project Structure

- `app.py`: Main application file
- `close_utils.py`: Utility functions for Close CRM
- `celery_worker.py`: Celery configuration and setup
- `blueprints/`: Modular components of the application
  - `easypost.py`: EasyPost integration
  - `gmail.py`: Gmail integration
  - `instantly.py`: Instantly integration
- `close_queries/`: JSON files containing Close CRM query templates
- `tests/`: Test suite

### Running Tests

```bash
pytest
```

## Deployment

The application is configured to be deployed on platforms like Heroku:

- `Procfile` contains the commands needed to run the web and worker processes
- Configure the necessary environment variables on your hosting platform

## Troubleshooting

- Check logs for detailed error information
- Verify environment variables are correctly set
- Ensure Redis is running and accessible
- For webhook issues, check the webhook tracker status at `/webhooks/status`
