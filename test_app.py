import unittest
from unittest.mock import patch, Mock
from datetime import datetime
import pytz
from app import schedule_skylead_check, check_skylead_for_viewed_profile


class TestScheduleSkyleadCheck(unittest.TestCase):
    def setUp(self):
        # Mock the Celery task to prevent actual task queueing
        self.patcher = patch('app.check_skylead_for_viewed_profile.apply_async')
        self.mock_apply_async = self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    @patch('app.datetime')
    def test_during_daytime_hours(self, mock_datetime):
        # Setup the mock datetime to a weekday and daytime hour
        central = pytz.timezone('America/Chicago')
        mock_now = central.localize(datetime(2024, 4, 30, 10, 0))  # A Tuesday at 10 AM
        mock_datetime.now.return_value = mock_now

        contact = {'id': '123'}
        schedule_skylead_check(contact)

        # Check if the delay is roughly 60 minutes (3600 seconds)
        self.mock_apply_async.assert_called_once()
        args, kwargs = self.mock_apply_async.call_args
        self.assertEqual(kwargs['countdown'], 3600)

    @patch('app.datetime')
    def test_weekday_after_hours(self, mock_datetime):
        # Setup the mock datetime to a weekday but after hours
        central = pytz.timezone('America/Chicago')
        mock_now = central.localize(datetime(2024, 4, 30, 18, 0))  # A Tuesday at 6 PM
        mock_datetime.now.return_value = mock_now

        contact = {'id': '123'}
        schedule_skylead_check(contact)

        # Check if the next check time is set to the next morning at 8 AM
        self.mock_apply_async.assert_called_once()
        args, kwargs = self.mock_apply_async.call_args
        # Calculate expected delay until the next morning at 8 AM
        expected_delay = (central.localize(datetime(2024, 5, 1, 8, 0)) - mock_now).total_seconds()  # 50,400 secs
        self.assertEqual(kwargs['countdown'], expected_delay)

    @patch('app.datetime')
    def test_friday_after_hours_to_monday(self, mock_datetime):
        # Setup the mock datetime to Friday after hours
        central = pytz.timezone('America/Chicago')
        mock_now = central.localize(datetime(2024, 5, 3, 18, 0))  # A Friday at 6 PM
        mock_datetime.now.return_value = mock_now

        contact = {'id': '123'}
        schedule_skylead_check(contact)

        # Check if the next check time is set to Monday morning at 8 AM
        self.mock_apply_async.assert_called_once()
        args, kwargs = self.mock_apply_async.call_args
        # Calculate expected delay until Monday morning at 8 AM
        expected_delay = (central.localize(datetime(2024, 5, 6, 8, 0)) - mock_now).total_seconds()  # 223,200 secs
        print(expected_delay)
        self.assertEqual(kwargs['countdown'], expected_delay)

    @patch('app.datetime')
    def test_saturday_to_monday(self, mock_datetime):
        # Setup the mock datetime to Friday after hours
        central = pytz.timezone('America/Chicago')
        mock_now = central.localize(datetime(2024, 5, 4, 9, 0))  # A Saturday at 9 AM
        mock_datetime.now.return_value = mock_now

        contact = {'id': '123'}
        schedule_skylead_check(contact)

        # Check if the next check time is set to Monday morning at 8 AM
        self.mock_apply_async.assert_called_once()
        args, kwargs = self.mock_apply_async.call_args
        # Calculate expected delay until Monday morning at 8 AM
        expected_delay = (central.localize(datetime(2024, 5, 6, 8, 0)) - mock_now).total_seconds()  # 223,200 secs
        print(expected_delay)
        self.assertEqual(kwargs['countdown'], expected_delay)

    @patch('app.datetime')
    def test_sunday_to_monday(self, mock_datetime):
        # Setup the mock datetime to Friday after hours
        central = pytz.timezone('America/Chicago')
        mock_now = central.localize(datetime(2024, 5, 5, 19, 0))  # A Sunday at 7 PM
        mock_datetime.now.return_value = mock_now

        contact = {'id': '123'}
        schedule_skylead_check(contact)

        # Check if the next check time is set to Monday morning at 8 AM
        self.mock_apply_async.assert_called_once()
        args, kwargs = self.mock_apply_async.call_args
        # Calculate expected delay until Monday morning at 8 AM
        expected_delay = (central.localize(datetime(2024, 5, 6, 8, 0)) - mock_now).total_seconds()  # 223,200 secs
        print(expected_delay)
        self.assertEqual(kwargs['countdown'], expected_delay)
