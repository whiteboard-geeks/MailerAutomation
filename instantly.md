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
- Create lead in Instantly (https://developer.instantly.ai/api/v2/lead/createlead). Include custom vars, Date & Time Delivered, Company Name, taskId, leadId, and contactId. Custom variables must be standardized so that they work with the email to be sent. - Unit test needs to check the format of the POST request.

==Pause until email sent in Instantly==

- Trigger: Webhook from Instantly is received that says the email has been sent - mock with a response.
- Find the task in Close and mark as complete. - Unit test needs to check the format of the request.

## Manual tasks the user will have to take

- Setup a campaign in Instantly
- Put the task in a Close Workflow. **Task name should be Instantly: %Campaign Name%**
