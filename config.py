import os

env_type = os.getenv("ENV_TYPE", "development")
TEMPORAL_WORKFLOW_UI_BASE_URL = os.environ["TEMPORAL_WORKFLOW_UI_BASE_URL"]
CLOSE_CRM_UI_LEAD_BASE_URL = "https://app.close.com/lead"
MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL = "https://docs.google.com/document/d/1LaWLPXPkQqbUwLvGquvez_TJEZaTtfdEP4ZIfSDWOPc/edit?tab=t.0"

# Campaign name used in integration tests that run against prod. No error email will be sent for this campaign.
TEST_CAMPAIGN_NAME = "Test20250305"

print("=== ENVIRONMENT INFO ===")
print(f"ENV_TYPE: {env_type}")
print(f"TEMPORAL_WORKFLOW_UI_BASE_URL: {TEMPORAL_WORKFLOW_UI_BASE_URL}")
print(f"CLOSE_CRM_UI_LEAD_BASE_URL: {CLOSE_CRM_UI_LEAD_BASE_URL}")
print(f"MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL: {MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}")
print(f"TEST_CAMPAIGN_NAME: {TEST_CAMPAIGN_NAME}")
print("=== END ENVIRONMENT INFO ===")
