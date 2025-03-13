"""
Constants and utility functions for Instantly integrations.
"""


def format_instantly_reply_task_text(reply_subject, campaign_name):
    """
    Format the task text for an Instantly reply according to the standard format.

    Args:
        reply_subject (str): The subject of the reply email
        campaign_name (str): The name of the Instantly campaign

    Returns:
        str: Formatted task text
    """
    return f"Instantly Email reply {reply_subject} from campaign {campaign_name}"
