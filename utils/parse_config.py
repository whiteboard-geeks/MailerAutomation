def parse_error_email_recipients_csv(email_addresses_csv: str | None) -> list[str]:
    if not email_addresses_csv or not email_addresses_csv.strip():
        return []
    return [email.strip() for email in email_addresses_csv.split(",")]
