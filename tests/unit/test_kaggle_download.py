"""Offline tests for the Kaggle dataset download client."""

from __future__ import annotations

import base64
import io
import threading
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from open_data_platform.ingestion.kaggle_download import (
    DownloadRequest,
    DownloadResponse,
    KaggleCredentials,
    KaggleDownloadError,
    _urllib_transport,
    credentials_from_env,
    download_dataset_zip,
    extract_expected_members,
)

_SECRET = "super-secret-token"


def _zip(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)
    return buffer.getvalue()


def _response(
    status: int,
    body: bytes = b"",
    headers: dict[str, str] | None = None,
) -> DownloadResponse:
    return DownloadResponse(status=status, headers=headers or {}, body=body)


def test_credentials_from_env_prefers_token() -> None:
    credentials = credentials_from_env(
        {
            "KAGGLE_API_TOKEN": "token-value",
            "KAGGLE_USERNAME": "user",
            "KAGGLE_KEY": "key",
        }
    )
    assert credentials.authorization_header() == "Bearer token-value"
    assert "token-value" not in repr(credentials)


def test_credentials_from_env_uses_basic_when_token_is_absent() -> None:
    credentials = credentials_from_env(
        {"KAGGLE_USERNAME": "user", "KAGGLE_KEY": "secret-key"}
    )
    encoded = base64.b64encode(b"user:secret-key").decode("ascii")
    assert credentials.authorization_header() == f"Basic {encoded}"


def test_credentials_from_env_rejects_empty_values() -> None:
    with pytest.raises(KaggleDownloadError, match="missing"):
        credentials_from_env(
            {"KAGGLE_API_TOKEN": "  ", "KAGGLE_USERNAME": "", "KAGGLE_KEY": ""}
        )


def test_download_sends_bearer_only_to_kaggle_host() -> None:
    seen: list[DownloadRequest] = []
    payload = _zip({"orders.csv": b"order_id\n"})

    def transport(request: DownloadRequest, timeout: float) -> DownloadResponse:
        del timeout
        seen.append(request)
        if len(seen) == 1:
            return _response(
                302,
                headers={
                    "Location": "https://storage.googleapis.com/bucket/a.zip?sig=secret",
                },
            )
        return _response(200, payload)

    body = download_dataset_zip(
        slug="olistbr/brazilian-ecommerce",
        credentials=KaggleCredentials(token=_SECRET),
        transport=transport,
        sleep=lambda _seconds: None,
    )

    assert body == payload
    assert seen[0].url == (
        "https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce"
    )
    assert seen[0].headers["Authorization"] == f"Bearer {_SECRET}"
    assert "Authorization" not in seen[1].headers
    assert seen[1].url.startswith("https://storage.googleapis.com/")


def test_download_keeps_authorization_on_kaggle_redirect() -> None:
    seen: list[DownloadRequest] = []

    def transport(request: DownloadRequest, timeout: float) -> DownloadResponse:
        del timeout
        seen.append(request)
        if len(seen) == 1:
            return _response(
                302,
                headers={"Location": "/api/v1/datasets/download/olistbr/x?download=1"},
            )
        return _response(200, b"PK")

    download_dataset_zip(
        slug="olistbr/x",
        credentials=KaggleCredentials(token=_SECRET),
        transport=transport,
        sleep=lambda _seconds: None,
    )

    assert seen[1].headers["Authorization"] == f"Bearer {_SECRET}"
    assert seen[1].url == (
        "https://www.kaggle.com/api/v1/datasets/download/olistbr/x?download=1"
    )


def test_unauthorized_does_not_retry_or_echo_the_secret() -> None:
    calls = 0

    def transport(request: DownloadRequest, timeout: float) -> DownloadResponse:
        del request, timeout
        nonlocal calls
        calls += 1
        return _response(401, body=_SECRET.encode())

    with pytest.raises(KaggleDownloadError, match="status 401") as raised:
        download_dataset_zip(
            slug="olistbr/brazilian-ecommerce",
            credentials=KaggleCredentials(token=_SECRET),
            transport=transport,
            sleep=lambda _seconds: None,
        )

    assert calls == 1
    assert _SECRET not in str(raised.value)


def test_server_error_retries_then_returns_body() -> None:
    calls = 0
    pauses: list[float] = []

    def transport(request: DownloadRequest, timeout: float) -> DownloadResponse:
        del request, timeout
        nonlocal calls
        calls += 1
        if calls == 1:
            return _response(503)
        return _response(200, b"zip")

    body = download_dataset_zip(
        slug="olistbr/brazilian-ecommerce",
        credentials=KaggleCredentials(token=_SECRET),
        transport=transport,
        sleep=pauses.append,
    )

    assert body == b"zip"
    assert calls == 2
    assert pauses == [1.0]


def test_server_error_omits_secret_after_final_attempt() -> None:
    def transport(request: DownloadRequest, timeout: float) -> DownloadResponse:
        del request, timeout
        return _response(500, body=_SECRET.encode())

    with pytest.raises(KaggleDownloadError, match="status 500") as raised:
        download_dataset_zip(
            slug="olistbr/brazilian-ecommerce",
            credentials=KaggleCredentials(username="user", key=_SECRET),
            transport=transport,
            max_attempts=2,
            sleep=lambda _seconds: None,
        )

    assert _SECRET not in str(raised.value)


def test_redirect_limit() -> None:
    def transport(request: DownloadRequest, timeout: float) -> DownloadResponse:
        del request, timeout
        return _response(302, headers={"Location": "https://www.kaggle.com/again"})

    with pytest.raises(KaggleDownloadError, match="redirect limit"):
        download_dataset_zip(
            slug="olistbr/brazilian-ecommerce",
            credentials=KaggleCredentials(token=_SECRET),
            transport=transport,
            max_redirects=2,
            sleep=lambda _seconds: None,
        )


def test_download_rejects_an_oversized_body() -> None:
    def transport(request: DownloadRequest, timeout: float) -> DownloadResponse:
        del request, timeout
        return _response(200, b"0123456789")

    with pytest.raises(KaggleDownloadError, match="size limit"):
        download_dataset_zip(
            slug="olistbr/brazilian-ecommerce",
            credentials=KaggleCredentials(token=_SECRET),
            transport=transport,
            max_download_bytes=4,
            sleep=lambda _seconds: None,
        )


def test_extract_publishes_only_the_expected_members(tmp_path) -> None:  # noqa: ANN001
    dest = tmp_path / "stage"
    extract_expected_members(
        _zip({"orders.csv": b"order_id\n", "sellers.csv": b"seller_id\n"}),
        dest,
        frozenset({"orders.csv", "sellers.csv"}),
    )
    assert (dest / "orders.csv").read_bytes() == b"order_id\n"
    assert (dest / "sellers.csv").read_bytes() == b"seller_id\n"


def test_extract_rejects_unexpected_member_without_publishing(tmp_path) -> None:  # noqa: ANN001
    dest = tmp_path / "stage"
    with pytest.raises(KaggleDownloadError, match="unexpected member"):
        extract_expected_members(
            _zip({"orders.csv": b"a", "readme.txt": b"nope"}),
            dest,
            frozenset({"orders.csv"}),
        )
    assert not (dest / "orders.csv").exists()
    assert not (dest / "readme.txt").exists()


def test_extract_rejects_path_traversal(tmp_path) -> None:  # noqa: ANN001
    outside = tmp_path / "evil.csv"
    dest = tmp_path / "stage"
    with pytest.raises(KaggleDownloadError, match="unexpected member"):
        extract_expected_members(
            _zip({"../evil.csv": b"nope", "orders.csv": b"a"}),
            dest,
            frozenset({"orders.csv"}),
        )
    assert not outside.exists()
    assert not (dest / "orders.csv").exists()


def test_extract_rejects_missing_expected_file(tmp_path) -> None:  # noqa: ANN001
    with pytest.raises(KaggleDownloadError, match="expected files"):
        extract_expected_members(
            _zip({"orders.csv": b"a"}),
            tmp_path / "stage",
            frozenset({"orders.csv", "sellers.csv"}),
        )


def test_extract_rejects_uncompressed_size_above_the_limit(tmp_path) -> None:  # noqa: ANN001
    with pytest.raises(KaggleDownloadError, match="size limit"):
        extract_expected_members(
            _zip({"orders.csv": b"12345"}),
            tmp_path / "stage",
            frozenset({"orders.csv"}),
            max_uncompressed_bytes=4,
        )
    assert not (tmp_path / "stage" / "orders.csv").exists()


def test_urllib_transport_returns_redirect_instead_of_following_it() -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/start":
                self.send_response(302)
                self.send_header("Location", "/next")
                self.end_headers()
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"followed")

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        response = _urllib_transport(
            DownloadRequest(url=f"http://127.0.0.1:{port}/start", headers={}),
            timeout=5,
        )
    finally:
        server.shutdown()
        server.server_close()

    assert response.status == 302
    assert response.headers["location"].endswith("/next")
    assert response.body != b"followed"
