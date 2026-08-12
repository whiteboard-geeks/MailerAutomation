import os

from utils.parse_config import parse_error_email_recipients_csv

env_type = os.getenv("ENV_TYPE", "development")
TEMPORAL_WORKFLOW_UI_BASE_URL = os.environ["TEMPORAL_WORKFLOW_UI_BASE_URL"]
CLOSE_CRM_UI_LEAD_BASE_URL = "https://app.close.com/lead"
MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL = "https://docs.google.com/document/d/1LaWLPXPkQqbUwLvGquvez_TJEZaTtfdEP4ZIfSDWOPc/edit?tab=t.0"
TEMPORAL_WORKFLOW_ACTIVITY_MAX_ATTEMPTS = 3
# Instantly webhook activities make several sequential Close API calls (lead search,
# task list, task update). Close's search endpoint is slow and Instantly delivers
# webhooks in bursts, so a tight timeout silently kills work mid-flight -- the activity
# is torn down before its in-activity error email can fire. 60s matches the value
# already used by WebhookAddLeadWorkflow.
TEMPORAL_ACTIVITY_START_TO_CLOSE_TIMEOUT_SECONDS = 60
ERROR_EMAIL_RECIPIENTS_CSV = os.getenv(
    "ERROR_EMAIL_RECIPIENTS_CSV"
)  # Comma separated list of email addresses
ERROR_EMAIL_RECIPIENTS = parse_error_email_recipients_csv(ERROR_EMAIL_RECIPIENTS_CSV)

# Campaign name used in integration tests that run against prod. No error email will be sent for this campaign.
TEST_CAMPAIGN_NAME = "Test20250305"

print("=== ENVIRONMENT INFO ===")
print(f"ENV_TYPE: {env_type}")
print(f"TEMPORAL_WORKFLOW_UI_BASE_URL: {TEMPORAL_WORKFLOW_UI_BASE_URL}")
print(f"CLOSE_CRM_UI_LEAD_BASE_URL: {CLOSE_CRM_UI_LEAD_BASE_URL}")
print(
    f"MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL: {MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}"
)
print(
    f"TEMPORAL_WORKFLOW_ACTIVITY_MAX_ATTEMPTS: {TEMPORAL_WORKFLOW_ACTIVITY_MAX_ATTEMPTS}"
)
print(
    "TEMPORAL_ACTIVITY_START_TO_CLOSE_TIMEOUT_SECONDS: "
    f"{TEMPORAL_ACTIVITY_START_TO_CLOSE_TIMEOUT_SECONDS}"
)
print(f"ERROR_EMAIL_RECIPIENTS_CSV: {ERROR_EMAIL_RECIPIENTS_CSV}")
print(f"ERROR_EMAIL_RECIPIENTS: {ERROR_EMAIL_RECIPIENTS}")
print(f"TEST_CAMPAIGN_NAME: {TEST_CAMPAIGN_NAME}")
print("=== END ENVIRONMENT INFO ===")
