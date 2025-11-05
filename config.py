import os


def _str_to_bool(value: str | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


env_type = os.getenv("ENV_TYPE", "development")

USE_TEMPORAL_FOR_EASYPOST_DELIVERY_STATUS = _str_to_bool(
    os.getenv("USE_TEMPORAL_FOR_EASYPOST_DELIVERY_STATUS"), default=False
)

print("=== ENVIRONMENT INFO ===")
print(f"ENV_TYPE: {env_type}")
print(f"USE_TEMPORAL_FOR_EASYPOST_DELIVERY_STATUS: {USE_TEMPORAL_FOR_EASYPOST_DELIVERY_STATUS}")
print("=== END ENVIRONMENT INFO ===")
