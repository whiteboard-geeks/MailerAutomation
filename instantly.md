# Plan

- See if you can create a blueprint AND have app.py contain some routes and logic.
- Create blueprint for Instantly and add access to the app.py file.
- End-to-end test setup: create a new lead and task in Close. We'll do this because you can only subscribe a lead to Instantly once.
  - Assign email lance+instantly{timeanddate}@whiteboardgeeks.com.
  - Name Lance{timeanddate}
  - Date & Time Delivered field completed (The same old string is fine): 2/27 to Richmond, VA
  - Company Name: InstantlyTest
- Trigger: receive webhook from Close when new task is created that contains Instantly: %Campaign Name%. Stress to the team that the webhook won't run without this. - Need a VCR payload from Close
- Instantly: Get campaigns - will have to paginate need the responses from Instantly that return the campaigns.
- Create lead in Instantly <https://developer.instantly.ai/api/v2/lead/createlead>. Include custom vars, Date & Time Delivered, Company Name, taskId, leadId, and contactId. Custom variables must be standardized so that they work with the email to be sent. - Unit test needs to check the format of the POST request.

==Pause until email sent in Instantly==

- Trigger: Webhook from Instantly is received that says the email has been sent - mock with a response.
- Find the task in Close and mark as complete. - Unit test needs to check the format of the request.

## Manual tasks the user will have to take

- Setup a campaign in Instantly
- Put the task in a Close Workflow. **Task name should be Instantly: %Campaign Name%**

## Detailed Steps

Create test lead in Close.

1. Check code in close_api.py (especially email_suffix timestamp; I want it to include the date).
2. Setup the debugger so when I launch test_instantly_e2e.py I can set breakpoints in that code.
3. Launch debugger and see if it works. This will mean setting up a compound launcher specifically for E2E tests. It should launch Flask, Redis, Celery, and Pytest.
4. Get the custom fields right for Date & Time Delivered and Company. Get the fields right for contacts.
5. Create a Close webhook.
6. Have it create a test lead and catch the webhook on webhook.site.
7. Add the task_created_data to the integration test. See if there are any potential variables to add to test response.
8. Add next steps and continue development!

Find campaign in Instantly

1. Create fn to parse out campaign name from task payload received and find campaign in Instantly. Will need to run the e2e test to have the webhook payload to work with when the task is created.
2. define fn to parse out campaign from task text (take out what comes after Instantly:)
3. env var for instantly api key
4. compare campaign name to list of instantly campaigns (trim and lower all names and name from Close task); may need to paginate.
5. Get campaignId if there's a campaign name match. If not send an error.
