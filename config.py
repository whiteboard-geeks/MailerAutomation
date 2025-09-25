import os
from typing import Optional


def _str_to_bool(value: Optional[str]) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "t", "yes", "y"}


env_type = os.getenv("ENV_TYPE", "development")
use_temporal_for_reply_received = _str_to_bool(
    os.getenv("USE_TEMPORAL_FOR_REPLY_RECEIVED", "false")
)

print("=== ENVIRONMENT INFO ===")
print(f"ENV_TYPE: {env_type}")
print(f"USE_TEMPORAL_FOR_REPLY_RECEIVED: {use_temporal_for_reply_received}")
print("=== END ENVIRONMENT INFO ===")
