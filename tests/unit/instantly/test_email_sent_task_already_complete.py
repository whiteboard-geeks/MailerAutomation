"""complete_lead_task_by_email when the campaign task was already completed by hand.

On 2026-11-03/04 Barbara bulk-completed 67 'Instantly: MTP_Letters/Big_Email 2' tasks in
the Close inbox before Instantly sent them; every later email_sent webhook then parked
with 'Task not found'. An already-completed task for the campaign is not an error.
"""

from unittest.mock import MagicMock, patch

import pytest

from temporal.activities.instantly import webhook_email_sent as mod

ARGS = mod.CompleteLeadTaskByEmailArgs(
    lead_email="a@example.com", campaign_name="MTP_Letters/Big_Email 2"
)


def _run(tasks):
    resp = MagicMock()
    resp.json.return_value = {"data": tasks}
    with patch.object(mod, "search_close_leads", return_value=[{"id": "lead_1"}]), \
         patch.object(mod, "make_close_request", return_value=resp) as req, \
         patch.object(mod, "is_last_attempt", return_value=True), \
         patch.object(mod, "send_email") as mail, \
         patch.object(mod.activity, "info", return_value=MagicMock(workflow_id="wf")), \
         patch.object(mod.activity, "logger"):
        try:
            return mod.complete_lead_task_by_email(ARGS), req, mail
        except ValueError as e:
            return e, req, mail


def test_already_completed_task_returns_lead_without_error():
    result, req, mail = _run(
        [{"id": "task_1", "text": "Instantly: MTP_Letters/Big_Email 2", "is_complete": True}]
    )
    assert result.lead_id == "lead_1"
    mail.assert_not_called()
    assert [c.args[0] for c in req.call_args_list] == ["get"]  # no PUT


def test_open_task_is_completed():
    result, req, _ = _run(
        [
            {"id": "task_old", "text": "Instantly: MTP_Letters/Big_Email 2", "is_complete": True},
            {"id": "task_1", "text": "Instantly: MTP_Letters/Big_Email 2", "is_complete": False},
        ]
    )
    assert result.lead_id == "lead_1"
    put = req.call_args_list[-1]
    assert put.args[0] == "put" and put.args[1].endswith("/task/task_1/")


def test_no_campaign_task_still_alerts():
    result, _, mail = _run([{"id": "t", "text": "Letter 3 Sent", "is_complete": True}])
    assert isinstance(result, ValueError)
    mail.assert_called_once()
