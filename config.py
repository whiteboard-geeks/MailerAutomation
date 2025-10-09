import os


env_type = os.getenv("ENV_TYPE", "development")

print("=== ENVIRONMENT INFO ===")
print(f"ENV_TYPE: {env_type}")
print("=== END ENVIRONMENT INFO ===")
