import base64
from unittest.mock import AsyncMock

import pytest

from temporal import client_provider


@pytest.fixture(autouse=True)
def clear_temporal_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    suffixes = [
        "ADDRESS",
        "NAMESPACE",
        "API_KEY",
        "TLS",
        "TLS_CERT",
        "TLS_CERT_BASE64",
        "TLS_KEY",
        "TLS_KEY_BASE64",
        "TLS_CA",
        "TLS_CA_BASE64",
        "TLS_SERVER_NAME",
    ]
    for prefix in ("TEMPORAL", "TEMPORAL_LEGACY"):
        for suffix in suffixes:
            monkeypatch.delenv(f"{prefix}_{suffix}", raising=False)


@pytest.mark.asyncio
async def test_local_defaults_use_plaintext(monkeypatch: pytest.MonkeyPatch) -> None:
    connect = AsyncMock(return_value=object())
    monkeypatch.setattr(client_provider.Client, "connect", connect)

    await client_provider.get_temporal_client()

    args, kwargs = connect.await_args
    assert args == ("localhost:7233",)
    assert kwargs["namespace"] == "default"
    assert kwargs["api_key"] is None
    assert kwargs["tls"] is False


@pytest.mark.asyncio
async def test_api_key_enables_tls(monkeypatch: pytest.MonkeyPatch) -> None:
    connect = AsyncMock(return_value=object())
    monkeypatch.setattr(client_provider.Client, "connect", connect)
    monkeypatch.setenv("TEMPORAL_ADDRESS", "cloud.example:7233")
    monkeypatch.setenv("TEMPORAL_NAMESPACE", "cloud-namespace")
    monkeypatch.setenv("TEMPORAL_API_KEY", "secret")

    await client_provider.get_temporal_client()

    _, kwargs = connect.await_args
    assert kwargs["api_key"] == "secret"
    assert kwargs["tls"] is True


@pytest.mark.asyncio
async def test_base64_mtls_material_is_loaded(monkeypatch: pytest.MonkeyPatch) -> None:
    connect = AsyncMock(return_value=object())
    monkeypatch.setattr(client_provider.Client, "connect", connect)
    monkeypatch.setenv("TEMPORAL_ADDRESS", "app.whiteboardgeeks.com:7233")
    monkeypatch.setenv("TEMPORAL_NAMESPACE", "mailerautomation-prod")
    encoded_cert = base64.b64encode(b"cert").decode()
    monkeypatch.setenv("TEMPORAL_TLS_CERT_BASE64", f"{encoded_cert[:3]}\n{encoded_cert[3:]}")
    monkeypatch.setenv("TEMPORAL_TLS_KEY_BASE64", base64.b64encode(b"key").decode())
    monkeypatch.setenv("TEMPORAL_TLS_CA_BASE64", base64.b64encode(b"ca").decode())
    monkeypatch.setenv("TEMPORAL_TLS_SERVER_NAME", "app.whiteboardgeeks.com")

    await client_provider.get_temporal_client()

    _, kwargs = connect.await_args
    tls = kwargs["tls"]
    assert tls.client_cert == b"cert"
    assert tls.client_private_key == b"key"
    assert tls.server_root_ca_cert == b"ca"
    assert tls.domain == "app.whiteboardgeeks.com"
    assert kwargs["api_key"] is None


@pytest.mark.asyncio
async def test_mtls_cert_and_key_are_required_together(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TEMPORAL_TLS_CERT_BASE64", base64.b64encode(b"cert").decode())

    with pytest.raises(ValueError, match="must be configured together"):
        await client_provider.get_temporal_client()


@pytest.mark.asyncio
async def test_legacy_connection_has_no_local_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TEMPORAL_LEGACY_ADDRESS", "cloud.example:7233")

    with pytest.raises(ValueError, match="TEMPORAL_LEGACY_NAMESPACE"):
        await client_provider.get_temporal_client("TEMPORAL_LEGACY")
