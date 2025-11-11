import os


def _str_to_bool(value: str | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


env_type = os.getenv("ENV_TYPE", "development")

print("=== ENVIRONMENT INFO ===")
print(f"ENV_TYPE: {env_type}")
print("=== END ENVIRONMENT INFO ===")
