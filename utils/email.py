from config import env_type
import pytz
from datetime import datetime


def send_email(subject, body, **kwargs):
    """
    Send an email using the Gmail API.

    In production, error emails are sent to:
    - Lance Johnson only
    
    Note: Other team members (Barbara, Lauren, Noura) are excluded from default 
    error recipients. Consultants receive reply notifications for their assigned leads.

    In development/staging, no emails are sent.

    Args:
        subject (str): The email subject
        body (str): The HTML content for the email body
        **kwargs: Additional parameters
            - recipients: Override default recipients for this specific email
            - text_content: Plain text version of the email (optional)

    Returns:
        dict: Response from Gmail API
    """
    if env_type.lower() != "production":
        return {"status": "success", "message": "Email not sent in non-production env"}

    central_time_zone = pytz.timezone("America/Chicago")
    central_time_now = datetime.now(central_time_zone)
    time_now_formatted = central_time_now.strftime("%Y-%m-%d %H:%M:%S%z")

    recipients_list = [
        "Lance Johnson <lance@whiteboardgeeks.com>",
    ]
    recipients = ", ".join(recipients_list)

    # Override with any explicitly provided recipients
    recipients = kwargs.get("recipients", recipients)

    # Add environment information to the body
    environment_info = f"<p><strong>Environment:</strong> {env_type}</p>"
    html_body = environment_info + body

    # For text content, if it's provided separately
    text_content = kwargs.get("text_content", body)
    text_environment_info = f"Environment: {env_type}\n\n"
    text_content = text_environment_info + text_content

    # Import here to avoid circular import
    from blueprints.gmail import send_gmail as bp_send_gmail
    
    # Send email using Gmail API
    gmail_response = bp_send_gmail(
        sender="lance@whiteboardgeeks.com",
        to=recipients,
        subject=f"[MailerAutomation] [{env_type}] {subject} {time_now_formatted}",
        html_content=html_body,
        text_content=text_content,
    )

    return gmail_response