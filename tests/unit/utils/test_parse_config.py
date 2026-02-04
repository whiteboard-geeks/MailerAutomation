import pytest

from utils.parse_config import parse_error_email_recipients_csv


@pytest.mark.parametrize(
    "input, expected_output",
    [
        (None, []),
        ("", []),
        (" ", []),
        ("hello@foo.de", ["hello@foo.de"]),
        (
            "hello@gmail.com, <Hello World> world@yahoo.com",
            ["hello@gmail.com", "<Hello World> world@yahoo.com"],
        ),
    ],
)
def test_parse_error_email_recipients_csv(
    input: str | None, expected_output: list[str]
) -> None:
    assert parse_error_email_recipients_csv(input) == expected_output
