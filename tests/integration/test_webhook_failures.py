"""
Run both webhook failure integration tests in sequence.

This script helps you test both Instantly and EasyPost webhook failure modes
and verify the email notifications.

To run:
python -m tests.integration.test_webhook_failures

All emails will be sent to the hardcoded recipient in the send_email function.
"""

import subprocess
import time
import sys

# ASCII colors for prettier output
GREEN = "\033[92m"
BLUE = "\033[94m"
YELLOW = "\033[93m"
RED = "\033[91m"
RESET = "\033[0m"
BOLD = "\033[1m"


def print_header(text):
    """Print a formatted header."""
    print(f"\n{BOLD}{BLUE}{'='*80}{RESET}")
    print(f"{BOLD}{BLUE}=== {text} {RESET}")
    print(f"{BOLD}{BLUE}{'='*80}{RESET}\n")


def main():
    """Run both webhook failure integration tests."""
    # Print email information
    print(
        f"{GREEN}Test emails will be sent to the recipient configured in the send_email function{RESET}\n"
    )

    # Run Instantly webhook failure tests
    print_header("RUNNING INSTANTLY WEBHOOK FAILURE TESTS")
    instantly_result = subprocess.run(
        [
            "python",
            "-m",
            "pytest",
            "tests/integration/instantly/test_webhook_failure_integration.py",
            "-v",
        ],
        capture_output=False,
    )

    # Pause between test runs
    time.sleep(1)
    print("\n")

    # Run EasyPost webhook failure tests
    print_header("RUNNING EASYPOST WEBHOOK FAILURE TESTS")
    easypost_result = subprocess.run(
        [
            "python",
            "-m",
            "pytest",
            "tests/integration/easypost/test_webhook_failure_integration.py",
            "-v",
        ],
        capture_output=False,
    )

    # Print summary
    print_header("TEST RUN SUMMARY")

    instantly_status = "PASSED" if instantly_result.returncode == 0 else "FAILED"
    easypost_status = "PASSED" if easypost_result.returncode == 0 else "FAILED"

    instantly_color = GREEN if instantly_result.returncode == 0 else RED
    easypost_color = GREEN if easypost_result.returncode == 0 else RED

    print(f"Instantly tests: {instantly_color}{instantly_status}{RESET}")
    print(f"EasyPost tests: {easypost_color}{easypost_status}{RESET}")

    print("\nCheck your email inbox for the test notification emails.")
    print("There should be multiple emails for each test suite.\n")

    # Return appropriate exit code
    if instantly_result.returncode != 0 or easypost_result.returncode != 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
