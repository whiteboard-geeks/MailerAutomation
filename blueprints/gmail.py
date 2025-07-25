"""
Blueprint for Gmail API integration.

This blueprint provides endpoints for sending emails and checking for received emails.
"""

import os
import json
import traceback
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import structlog
import uuid
from datetime import datetime

from flask import Blueprint, request, jsonify, g
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Set up blueprint
gmail_bp = Blueprint("gmail", __name__)

# Configure logging using structlog
logger = structlog.get_logger("gmail")

# Constants
DEFAULT_SENDER = "lance@whiteboardgeeks.com"  # Default sender email address

# Webhook authentication
GMAIL_WEBHOOK_PASSWORD = os.environ.get("GMAIL_WEBHOOK_PASSWORD")


def get_service_account_credentials(impersonate_user=DEFAULT_SENDER):
    """
    Get service account credentials for Gmail API.

    Args:
        impersonate_user (str): Email of the user to impersonate

    Returns:
        Credentials object for the service account
    """
    scopes = [
        "https://www.googleapis.com/auth/gmail.send",
        "https://www.googleapis.com/auth/gmail.readonly",
    ]

    try:
        # Get the credentials at runtime instead of module import time
        service_account_info = os.environ.get("GMAIL_SERVICE_ACCOUNT_INFO")
        service_account_file = os.environ.get("GMAIL_SERVICE_ACCOUNT_FILE")

        # Try to load credentials from environment variable first
        if service_account_info:
            try:
                service_account_info_dict = json.loads(service_account_info)
                credentials = service_account.Credentials.from_service_account_info(
                    service_account_info_dict, scopes=scopes, subject=impersonate_user
                )
                return credentials
            except json.JSONDecodeError as json_error:
                logger.error(
                    "Error parsing GMAIL_SERVICE_ACCOUNT_INFO JSON",
                    error=str(json_error),
                    info_length=len(service_account_info)
                    if service_account_info
                    else 0,
                )
                return None

        # Fall back to file if environment variable not available
        elif service_account_file:
            credentials = service_account.Credentials.from_service_account_file(
                service_account_file, scopes=scopes, subject=impersonate_user
            )
            return credentials
        else:
            logger.error("No service account credentials found")
            return None
    except Exception as e:
        logger.error(
            "Error loading service account credentials",
            error=str(e),
            traceback=traceback.format_exc(),
        )
        return None


def create_gmail_service(impersonate_user=DEFAULT_SENDER):
    """
    Create Gmail API service.

    Args:
        impersonate_user (str): Email of the user to impersonate

    Returns:
        Gmail API service object or None if error
    """
    try:
        credentials = get_service_account_credentials(impersonate_user)
        if not credentials:
            return None

        service = build("gmail", "v1", credentials=credentials)
        return service
    except Exception as e:
        logger.error(
            "Error creating Gmail service",
            error=str(e),
            traceback=traceback.format_exc(),
        )
        return None


def create_message(
    sender, to, subject, html_content, text_content=None, cc=None, bcc=None
):
    """
    Create a MIME message object for sending via Gmail API.

    Args:
        sender (str): Sender email address
        to (str or list): Recipient email address(es)
        subject (str): Email subject
        html_content (str): HTML body of the email
        text_content (str, optional): Plain text body of the email
        cc (str or list, optional): CC recipients
        bcc (str or list, optional): BCC recipients

    Returns:
        dict: A dictionary containing the encoded message
    """
    message = MIMEMultipart("alternative")
    message["From"] = sender

    # Handle to, cc, and bcc as lists or strings
    if isinstance(to, list):
        message["To"] = ", ".join(to)
    else:
        message["To"] = to

    if cc:
        if isinstance(cc, list):
            message["Cc"] = ", ".join(cc)
        else:
            message["Cc"] = cc

    if bcc:
        if isinstance(bcc, list):
            message["Bcc"] = ", ".join(bcc)
        else:
            message["Bcc"] = bcc

    message["Subject"] = subject

    # Add plain text part if provided
    if text_content:
        part1 = MIMEText(text_content, "plain")
        message.attach(part1)

    # Add HTML part
    part2 = MIMEText(html_content, "html")
    message.attach(part2)

    # Encode the message
    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

    return {"raw": encoded_message}


def send_gmail(sender, to, subject, html_content, text_content=None, cc=None, bcc=None):
    """
    Send an email using Gmail API.

    Args:
        sender (str): Sender email address
        to (str or list): Recipient email address(es)
        subject (str): Email subject
        html_content (str): HTML body of the email
        text_content (str, optional): Plain text body of the email
        cc (str or list, optional): CC recipients
        bcc (str or list, optional): BCC recipients

    Returns:
        dict: The response from the Gmail API or error information
    """
    try:
        # Create Gmail service
        service = create_gmail_service(impersonate_user=sender)
        if not service:
            return {"status": "error", "message": "Failed to create Gmail service"}

        # Create message
        message = create_message(
            sender=sender,
            to=to,
            subject=subject,
            html_content=html_content,
            text_content=text_content,
            cc=cc,
            bcc=bcc,
        )

        # Send message
        sent_message = (
            service.users().messages().send(userId="me", body=message).execute()
        )

        return {
            "status": "success",
            "message": "Email sent successfully",
            "message_id": sent_message.get("id"),
            "thread_id": sent_message.get("threadId"),
        }

    except HttpError as e:
        error_message = f"Gmail API HTTP error: {str(e)}"
        logger.error(error_message, error_code=e.resp.status, error_reason=e.reason)
        return {
            "status": "error",
            "message": error_message,
            "error_code": e.resp.status,
            "error_reason": e.reason,
        }

    except Exception as e:
        error_message = f"Error sending email: {str(e)}"
        logger.error(error_message, traceback=traceback.format_exc())
        return {"status": "error", "message": error_message}


def check_for_emails(user_email, query=None, max_results=10, include_content=False):
    """
    Check for emails in the specified user's inbox.

    Args:
        user_email (str): Email of the user to check messages for
        query (str, optional): Search query (Gmail search syntax)
        max_results (int, optional): Maximum number of messages to return
        include_content (bool, optional): Whether to include full message content

    Returns:
        dict: The list of messages or error information
    """
    try:
        # Create Gmail service
        service = create_gmail_service(impersonate_user=user_email)
        if not service:
            return {"status": "error", "message": "Failed to create Gmail service"}

        # Get message list
        messages_response = (
            service.users()
            .messages()
            .list(userId="me", q=query, maxResults=max_results)
            .execute()
        )

        messages = messages_response.get("messages", [])

        if not messages:
            return {"status": "success", "message": "No messages found", "messages": []}

        # Get details for each message
        detailed_messages = []
        for msg in messages:
            msg_id = msg["id"]

            # Get message details
            if include_content:
                message_data = (
                    service.users()
                    .messages()
                    .get(userId="me", id=msg_id, format="full")
                    .execute()
                )
            else:
                message_data = (
                    service.users()
                    .messages()
                    .get(
                        userId="me",
                        id=msg_id,
                        format="metadata",
                        metadataHeaders=["From", "To", "Subject", "Date"],
                    )
                    .execute()
                )

            # Extract headers
            headers = {}
            for header in message_data["payload"].get("headers", []):
                headers[header["name"]] = header["value"]

            # Create simplified message structure
            message_info = {
                "id": msg_id,
                "thread_id": message_data.get("threadId"),
                "date": headers.get("Date"),
                "from": headers.get("From"),
                "to": headers.get("To"),
                "subject": headers.get("Subject"),
                "snippet": message_data.get("snippet"),
            }

            # Add message body if requested
            if include_content:
                # This is a simplified approach - a real implementation would
                # need to handle multipart messages, attachments, etc.
                message_info["body"] = {"html": None, "text": None}

                # Try to extract message parts
                payload = message_data.get("payload", {})
                if "parts" in payload:
                    for part in payload["parts"]:
                        mime_type = part.get("mimeType")
                        if mime_type == "text/plain":
                            data = part.get("body", {}).get("data")
                            if data:
                                message_info["body"]["text"] = base64.urlsafe_b64decode(
                                    data
                                ).decode("utf-8")
                        elif mime_type == "text/html":
                            data = part.get("body", {}).get("data")
                            if data:
                                message_info["body"]["html"] = base64.urlsafe_b64decode(
                                    data
                                ).decode("utf-8")
                elif "body" in payload and "data" in payload["body"]:
                    # Handle single part messages
                    mime_type = payload.get("mimeType")
                    data = payload["body"].get("data")
                    if data:
                        decoded_data = base64.urlsafe_b64decode(data).decode("utf-8")
                        if mime_type == "text/plain":
                            message_info["body"]["text"] = decoded_data
                        elif mime_type == "text/html":
                            message_info["body"]["html"] = decoded_data
                        else:
                            # If we can't determine the type, set it as text
                            message_info["body"]["text"] = decoded_data

            detailed_messages.append(message_info)

        return {
            "status": "success",
            "message": f"Found {len(detailed_messages)} messages",
            "messages": detailed_messages,
        }

    except HttpError as e:
        error_message = f"Gmail API HTTP error: {str(e)}"
        logger.error(error_message, error_code=e.resp.status, error_reason=e.reason)
        return {
            "status": "error",
            "message": error_message,
            "error_code": e.resp.status,
            "error_reason": e.reason,
        }

    except Exception as e:
        error_message = f"Error checking emails: {str(e)}"
        logger.error(error_message, traceback=traceback.format_exc())
        return {"status": "error", "message": error_message}


def validate_api_request():
    """
    Validate the API request by checking the webhook password.

    Returns:
        tuple: (is_valid, error_response)
    """
    if not GMAIL_WEBHOOK_PASSWORD:
        error = "Gmail webhook password not configured"
        logger.error(error)
        return False, (jsonify({"status": "error", "message": error}), 500)

    # Get the password from the request headers
    auth_header = request.headers.get("Authorization")

    if not auth_header:
        error = "Authorization header missing"
        logger.warning(error)
        return False, (jsonify({"status": "error", "message": error}), 401)

    # Check if it's a Bearer token
    if not auth_header.startswith("Bearer "):
        error = "Invalid authorization format"
        logger.warning(error)
        return False, (jsonify({"status": "error", "message": error}), 401)

    token = auth_header.replace("Bearer ", "")

    # Validate the token
    if token != GMAIL_WEBHOOK_PASSWORD:
        error = "Invalid webhook password"
        logger.warning(error)
        return False, (jsonify({"status": "error", "message": error}), 401)

    return True, None


@gmail_bp.route("/send_email", methods=["POST"])
def send_email_endpoint():
    """
    Endpoint to send an email using Gmail API.

    Required params:
    - to: Recipient email address(es)
    - subject: Email subject
    - html_content: HTML content of the email

    Optional params:
    - text_content: Plain text content of the email
    - cc: CC recipients
    - bcc: BCC recipients
    """
    try:
        # Validate API request
        is_valid, error_response = validate_api_request()
        if not is_valid:
            return error_response

        # Get parameters from request
        data = request.get_json()

        # Validate required parameters
        required_params = ["to", "subject", "html_content"]
        for param in required_params:
            if param not in data:
                return (
                    jsonify(
                        {
                            "status": "error",
                            "message": f"Missing required parameter: {param}",
                        }
                    ),
                    400,
                )

        # Send email
        result = send_gmail(
            sender="lance@whiteboardgeeks.com",
            to=data["to"],
            subject=data["subject"],
            html_content=data["html_content"],
            text_content=data.get("text_content"),
            cc=data.get("cc"),
            bcc=data.get("bcc"),
        )

        return jsonify(result)

    except Exception as e:
        # Get request ID which serves as run ID
        run_id = getattr(g, "request_id", str(uuid.uuid4()))

        # Extract calling function name
        calling_function = "send_email_endpoint"

        # Capture the traceback
        tb = traceback.format_exc()

        # Format error message with detailed information
        error_message = f"""
        <h2>Gmail Send Email Error</h2>
        <p><strong>Error:</strong> {str(e)}</p>
        <p><strong>Route:</strong> {request.path}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Origin:</strong> {calling_function}</p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        
        <h3>Request Data:</h3>
        <pre>{json.dumps({k: v for k, v in request.get_json().items() if k not in ["auth_token", "password"]}, indent=2, default=str)}</pre>
        
        <h3>Traceback:</h3>
        <pre>{tb}</pre>
        """

        logger.error(
            "send_email_error",
            error=str(e),
            traceback=tb,
            run_id=run_id,
            route=request.path,
            origin=calling_function,
        )

        error_message = f"Error in send_email endpoint: {str(e)}"
        logger.error(error_message, traceback=traceback.format_exc())
        return jsonify({"status": "error", "message": error_message}), 500


@gmail_bp.route("/check_emails", methods=["GET"])
def check_emails_endpoint():
    """
    Endpoint for checking emails via the Gmail API.

    Query parameters:
    - user_email: Email address of the user to check (defaults to DEFAULT_SENDER)
    - query: Gmail search query (optional)
    - max_results: Maximum number of results to return (default: 10)
    - include_content: Whether to include full message content (default: false)
    """
    try:
        # Validate the request
        is_valid, error_response = validate_api_request()
        if not is_valid:
            return error_response

        # Get query parameters
        user_email = request.args.get("user_email", DEFAULT_SENDER)
        query = request.args.get("query")

        try:
            max_results = int(request.args.get("max_results", "10"))
        except ValueError:
            max_results = 10

        include_content = request.args.get("include_content", "").lower() == "true"

        # Check for emails
        result = check_for_emails(
            user_email=user_email,
            query=query,
            max_results=max_results,
            include_content=include_content,
        )

        # Return appropriate response
        if result.get("status") == "success":
            return jsonify(result), 200
        else:
            return jsonify(result), 500

    except Exception as e:
        error_message = f"Error in check_emails endpoint: {str(e)}"
        logger.error(error_message, traceback=traceback.format_exc())
        return jsonify({"status": "error", "message": error_message}), 500
