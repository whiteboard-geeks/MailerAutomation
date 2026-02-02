import logging
import os
import structlog
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.service import TLSConfig


async def get_temporal_client() -> Client:
    logging.basicConfig(level=logging.INFO)
    logger = structlog.get_logger(__name__)

    cert_path = os.getenv("TEMPORAL_TLS_CERT")
    key_path = os.getenv("TEMPORAL_TLS_KEY")
    api_key = os.getenv("TEMPORAL_API_KEY")

    # Check for mTLS authentication
    if cert_path and key_path:
        with open(cert_path, "rb") as f:
            client_cert = f.read()
        with open(key_path, "rb") as f:
            client_key = f.read()

        target_host=os.getenv("TEMPORAL_ADDRESS")
        namespace=os.getenv("TEMPORAL_NAMESPACE")
        tls=TLSConfig(
                client_cert=client_cert,
                client_private_key=client_key,
            )
    elif api_key:
        target_host=os.getenv("TEMPORAL_ADDRESS")
        namespace=os.getenv("TEMPORAL_NAMESPACE")
        tls=True
    else:
        target_host=os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
        namespace=os.getenv("TEMPORAL_NAMESPACE", "default")
        tls = False
      
    if not target_host:
        raise ValueError("TEMPORAL_ADDRESS environment variable is not set")
    
    if not namespace:
        raise ValueError("TEMPORAL_NAMESPACE environment variable is not set")
    
    logger.info("temporal.client_provider.get_temporal_client.connecting_to_temporal_server",
                 target_host=target_host, 
                 namespace=namespace)

    return await Client.connect(
        target_host,
        namespace=namespace,
        data_converter=pydantic_data_converter,
        api_key=api_key,
        tls=tls,
    )
