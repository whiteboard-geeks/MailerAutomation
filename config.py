import os


def _str_to_bool(value: str | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


env_type = os.getenv("ENV_TYPE", "development")

# Feature flag: toggles EasyPost tracker creation between Celery and Temporal.
USE_TEMPORAL_FOR_EASYPOST_CREATE_TRACKER = _str_to_bool(
    os.getenv("USE_TEMPORAL_FOR_EASYPOST_CREATE_TRACKER"),
    default=False,
)

print("=== ENVIRONMENT INFO ===")
print(f"ENV_TYPE: {env_type}")
print(
    "USE_TEMPORAL_FOR_EASYPOST_CREATE_TRACKER:",
    USE_TEMPORAL_FOR_EASYPOST_CREATE_TRACKER,
)
print("=== END ENVIRONMENT INFO ===")
