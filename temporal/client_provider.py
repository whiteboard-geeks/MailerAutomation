import base64
import logging
import os
from pathlib import Path

import structlog
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.service import TLSConfig


def _env_name(config_prefix: str, suffix: str) -> str:
    return f"{config_prefix}_{suffix}"


def _read_optional_bytes(config_prefix: str, suffix: str) -> bytes | None:
    base64_value = os.getenv(_env_name(config_prefix, f"{suffix}_BASE64"))
    if base64_value:
        try:
            normalized_value = "".join(base64_value.split())
            return base64.b64decode(normalized_value, validate=True)
        except ValueError as exc:
            raise ValueError(
                f"{_env_name(config_prefix, f'{suffix}_BASE64')} is not valid base64"
            ) from exc

    path_value = os.getenv(_env_name(config_prefix, suffix))
    if path_value:
        return Path(path_value).read_bytes()

    return None


def temporal_config_is_set(config_prefix: str = "TEMPORAL") -> bool:
    """Return whether a non-default Temporal connection is configured."""
    return bool(os.getenv(_env_name(config_prefix, "ADDRESS")))


async def get_temporal_client(config_prefix: str = "TEMPORAL") -> Client:
    """Connect to Temporal using API-key auth, mTLS, or local plaintext.

    ``config_prefix`` is ``TEMPORAL`` for the primary cluster and
    ``TEMPORAL_LEGACY`` for the temporary Cloud drain connection.
    """
    logging.basicConfig(level=logging.INFO)
    logger = structlog.get_logger(__name__)

    address_name = _env_name(config_prefix, "ADDRESS")
    namespace_name = _env_name(config_prefix, "NAMESPACE")
    api_key = os.getenv(_env_name(config_prefix, "API_KEY"))
    client_cert = _read_optional_bytes(config_prefix, "TLS_CERT")
    client_key = _read_optional_bytes(config_prefix, "TLS_KEY")
    server_ca = _read_optional_bytes(config_prefix, "TLS_CA")
    server_name = os.getenv(_env_name(config_prefix, "TLS_SERVER_NAME"))

    if bool(client_cert) != bool(client_key):
        raise ValueError(
            f"{_env_name(config_prefix, 'TLS_CERT')} and "
            f"{_env_name(config_prefix, 'TLS_KEY')} must be configured together"
        )

    if client_cert and client_key:
        tls: bool | TLSConfig = TLSConfig(
            server_root_ca_cert=server_ca,
            domain=server_name,
            client_cert=client_cert,
            client_private_key=client_key,
        )
    elif api_key:
        tls = True
    else:
        tls = os.getenv(_env_name(config_prefix, "TLS"), "false").lower() in {
            "1",
            "true",
            "yes",
        }

    target_host: str | None
    namespace: str | None
    if config_prefix == "TEMPORAL":
        target_host = os.getenv(address_name, "localhost:7233")
        namespace = os.getenv(namespace_name, "default")
    else:
        target_host = os.getenv(address_name)
        namespace = os.getenv(namespace_name)

    if not target_host:
        raise ValueError(f"{address_name} environment variable is not set")
    if not namespace:
        raise ValueError(f"{namespace_name} environment variable is not set")

    logger.info(
        "temporal.client_provider.connecting_to_temporal_server",
        config_prefix=config_prefix,
        target_host=target_host,
        namespace=namespace,
        authentication="mtls" if client_cert else "api_key" if api_key else "none",
    )

    return await Client.connect(
        target_host,
        namespace=namespace,
        data_converter=pydantic_data_converter,
        api_key=api_key,
        tls=tls,
    )
