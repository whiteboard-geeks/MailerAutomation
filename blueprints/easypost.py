"""
Blueprint for EasyPost integration.
This module handles EasyPost webhook tracking and delivery status updates.
"""

from flask import Blueprint, request, jsonify, g
import structlog
import uuid
from temporal.service import temporal
from temporal.shared import TASK_QUEUE_NAME
from temporal.workflows.easypost.webhook_create_tracker_workflow import WebhookCreateTrackerPayload, WebhookCreateTrackerWorkflow
from temporal.workflows.easypost.webhook_delivery_status_workflow import WebhookDeliveryStatusPayload, WebhookDeliveryStatusWorkflow
from utils.easypost import get_easypost_client


# Initialize Blueprint
easypost_bp = Blueprint("easypost", __name__)

# Initialize logger
logger = structlog.get_logger()

# Initialize EasyPost Client (default with production API key)
easypost_client = get_easypost_client()

@easypost_bp.route("/create_tracker", methods=["POST"])
def create_easypost_tracker():
    json_payload = request.get_json(silent=True)
    if json_payload is None:
        response_data = {
            "status": "error",
            "message": "Invalid request format",
        }
        return jsonify(response_data), 400

    g_run_id = getattr(g, "request_id", str(uuid.uuid4()))
    logger.info(
        "create_tracker_temporal_enqueue",
        run_id=g_run_id,
    )

    try:
        workflow_input = WebhookCreateTrackerPayload(json_payload=json_payload)
    except Exception as exc:
        response_data = {
            "status": "error",
            "message": f"Invalid payload: {exc}",
        }
        return jsonify(response_data), 400
    
    try:
        temporal.ensure_started()
        start_coro = temporal.client.start_workflow(
            WebhookCreateTrackerWorkflow.run,
            workflow_input,
            id=g_run_id,
            task_queue=TASK_QUEUE_NAME,
        )
        temporal.run(start_coro)
    except Exception as exc:
        logger.exception(
            "create_tracker_temporal_enqueue_failed",
            run_id=g_run_id,
            error=str(exc),
        )
        return (
            jsonify(
                {
                    "status": "error",
                    "message": "Error enqueuing Temporal tracker workflow",
                }
            ),
            500,
        )

    response_data = {
        "status": "accepted",
        "message": "Tracker creation workflow queued for background processing",
        "workflow_id": g_run_id,
    }
    return jsonify(response_data), 202


@easypost_bp.route("/delivery_status", methods=["POST"])
def handle_package_delivery_update():
    """Handle package delivery status updates from EasyPost webhook - Async Processing."""
    json_payload = request.get_json(silent=True)
    if json_payload is None:
        response_data = {
            "status": "error",
            "message": "Invalid request format",
        }
        return jsonify(response_data), 400
    
    if "result" not in json_payload:
        response_data = {
            "status": "error",
            "message": "Invalid request format",
        }
        return jsonify(response_data), 400

    tracking_data = json_payload["result"]
    easy_post_event_id = json_payload["id"]
    logger.info(f"EasyPost Event ID: {easy_post_event_id}")
    if tracking_data["status"] != "delivered":
        logger.info("Tracking status is not 'delivered'; webhook did not run.")
        return jsonify(
            {
                "status": "success",
                "message": "Tracking status is not 'delivered' so did not run.",
            }
        ), 200

    g_run_id = getattr(g, "request_id", str(uuid.uuid4()))
    logger.info(
        "create_tracker_temporal_enqueue",
        run_id=g_run_id,
    )

    try:
        workflow_input = WebhookDeliveryStatusPayload(json_payload=json_payload)
    except Exception as exc:
        response_data = {
            "status": "error",
            "message": f"Invalid payload: {exc}",
        }
        return jsonify(response_data), 400

    try:
        temporal.ensure_started()
        start_coro = temporal.client.start_workflow(
            WebhookDeliveryStatusWorkflow.run,
            workflow_input,
            id=g_run_id,
            task_queue=TASK_QUEUE_NAME,
        )
        temporal.run(start_coro)
    except Exception as exc:
        logger.exception(
            "delivery_status_temporal_enqueue_failed",
            run_id=g_run_id,
            error=str(exc),
        )
        return (
            jsonify(
                {
                    "status": "error",
                    "message": "Error enqueuing Temporal delivery status workflow",
                }
            ),
            500,
        )

    response_data = {
        "status": "accepted",
        "message": "Delivery status processing workflow queued for background processing",
        "temporal_workflow_id": g_run_id,
    }
    return jsonify(response_data), 202
