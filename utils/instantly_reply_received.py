"""Helpers shared by Instantly reply-received webhook handlers."""

import structlog

logger = structlog.get_logger("instantly.reply_received")

CONSULTANT_FIELD_KEY = "custom.lcf_TRIulkQaxJArdGl2k89qY6NKR0ZTYkzjRdeILo1h5fi"


def determine_notification_recipients(lead_details, env_type):
    """Return consultant-specific recipients (if any) for reply notifications."""
    consultant = lead_details.get(CONSULTANT_FIELD_KEY)
    lead_id = lead_details.get("id", "unknown")

    if consultant is None:
        logger.warning(
            "consultant_field_missing",
            lead_id=lead_id,
            message=f"Consultant field missing for lead {lead_id}. Using default recipients.",
        )
        return None, None

    if consultant == "":
        logger.warning(
            "consultant_field_empty",
            lead_id=lead_id,
            message=f"Consultant field empty for lead {lead_id}. Using default recipients.",
        )
        return None, None

    if consultant == "Barbara Pigg":
        if env_type == "development":
            recipients = "lance@whiteboardgeeks.com"
            logger.info(
                "consultant_determined",
                lead_id=lead_id,
                consultant="Barbara Pigg",
                environment="development",
                recipients=recipients,
            )
            return recipients, None

        recipients = "barbara.pigg@whiteboardgeeks.com"
        logger.info(
            "consultant_determined",
            lead_id=lead_id,
            consultant="Barbara Pigg",
            environment="production",
            recipients=recipients,
        )
        return recipients, None

    if consultant == "April Lowrie":
        if env_type == "development":
            recipients = "lance@whiteboardgeeks.com"
            logger.info(
                "consultant_determined",
                lead_id=lead_id,
                consultant="April Lowrie",
                environment="development",
                recipients=recipients,
            )
            return recipients, None

        recipients = "april.lowrie@whiteboardgeeks.com"
        logger.info(
            "consultant_determined",
            lead_id=lead_id,
            consultant="April Lowrie",
            environment="production",
            recipients=recipients,
        )
        return recipients, None

    logger.warning(
        "consultant_unknown",
        lead_id=lead_id,
        consultant=consultant,
        message=f"Unknown consultant '{consultant}' for lead {lead_id}. Using default recipients.",
    )
    return None, None


__all__ = [
    "CONSULTANT_FIELD_KEY",
    "determine_notification_recipients",
]
