import logging
import os
import structlog
from enum import Enum
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter


class Environment(Enum):
    PROD = "PROD"
    STAGING = "STAGING"


async def get_temporal_client(environment: Environment) -> Client:
    logging.basicConfig(level=logging.INFO)
    logger = structlog.get_logger(__name__)

    api_key = os.getenv(f"TEMPORAL_API_KEY_{environment.value}")

    # Check for mTLS authentication
    if api_key:
        target_host = os.getenv(f"TEMPORAL_ADDRESS_{environment.value}")
        namespace = os.getenv(f"TEMPORAL_NAMESPACE_{environment.value}")
        if not target_host or not namespace:
            raise ValueError(f"TEMPORAL_ADDRESS_{environment.value} and TEMPORAL_NAMESPACE_{environment.value} must be set when using API key authentication")
        tls = True
    else:
        target_host = os.getenv(f"TEMPORAL_ADDRESS_{environment.value}", "localhost:7233")
        namespace = os.getenv(f"TEMPORAL_NAMESPACE_{environment.value}", "default")
        tls = False
    
    logger.info("connecting_to_temporal_server",
                 target_host=target_host, 
                 namespace=namespace)

    return await Client.connect(
        target_host,
        namespace=namespace,
        data_converter=pydantic_data_converter,
        api_key=api_key,
        tls=tls,
    )
