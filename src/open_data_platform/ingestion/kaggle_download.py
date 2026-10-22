"""Download a Kaggle dataset zip and extract an expected set of files.

The client calls ``GET /api/v1/datasets/download/{owner}/{slug}``.
``Authorization`` is sent only to ``www.kaggle.com``. Redirects to any
other host drop that header. Response bytes are treated as untrusted.
"""

from __future__ import annotations

import base64
import io
import re
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from urllib.parse import urljoin, urlparse

DEFAULT_API_BASE = "https://www.kaggle.com"
DEFAULT_TIMEOUT_SECONDS = 120.0
MAX_DOWNLOAD_BYTES = 512 * 1024 * 1024
MAX_UNCOMPRESSED_BYTES = 512 * 1024 * 1024
MAX_REDIRECTS = 5
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_AUTH_HOST = "www.kaggle.com"
_CHUNK_BYTES = 64 * 1024
_SLUG_PART = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


class KaggleDownloadError(ValueError):
    """A Kaggle download or archive extract failed."""


@dataclass(frozen=True, slots=True)
class KaggleCredentials:
    """Kaggle API credentials. The token wins when it is present."""

    token: str | None = None
    username: str | None = None
    key: str | None = None

    def __repr__(self) -> str:
        return "KaggleCredentials(redacted)"

    def authorization_header(self) -> str:
        """Return the Authorization header value."""
        if self.token:
            return f"Bearer {self.token}"
        if self.username and self.key:
            raw = f"{self.username}:{self.key}".encode()
            encoded = base64.b64encode(raw).decode("ascii")
            return f"Basic {encoded}"
        msg = "Kaggle credentials are missing"
        raise KaggleDownloadError(msg)


@dataclass(frozen=True, slots=True)
class DownloadRequest:
    """One GET issued by the download loop."""

    url: str
    headers: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class DownloadResponse:
    """HTTP response visible to the download loop. Redirects are not followed."""

    status: int
    headers: Mapping[str, str]
    body: bytes


Transport = Callable[[DownloadRequest, float], DownloadResponse]


def credentials_from_env(env: Mapping[str, str]) -> KaggleCredentials:
    """Build credentials from ``KAGGLE_API_TOKEN`` or the legacy username/key pair.

    Empty values count as absent. A missing pair raises before any request.
    """
    token = env.get("KAGGLE_API_TOKEN", "").strip()
    username = env.get("KAGGLE_USERNAME", "").strip()
    key = env.get("KAGGLE_KEY", "").strip()
    if token:
        return KaggleCredentials(token=token)
    if username and key:
        return KaggleCredentials(username=username, key=key)
    msg = "Kaggle credentials are missing"
    raise KaggleDownloadError(msg)


def download_dataset_zip(
    *,
    slug: str,
    credentials: KaggleCredentials,
    transport: Transport | None = None,
    api_base: str = DEFAULT_API_BASE,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_attempts: int = 3,
    max_redirects: int = MAX_REDIRECTS,
    max_download_bytes: int = MAX_DOWNLOAD_BYTES,
    sleep: Callable[[float], None] | None = None,
) -> bytes:
    """Download one dataset zip.

    Retries apply to HTTP 429 and 5xx. HTTP 401 and 403 fail immediately.
    The error text does not include the token, key, or response body.
    """
    _validate_slug(slug)
    if max_attempts < 1:
        msg = "max_attempts must be >= 1"
        raise KaggleDownloadError(msg)
    authorization = credentials.authorization_header()
    active = _urllib_transport if transport is None else transport
    pause = time.sleep if sleep is None else sleep
    url = f"{api_base.rstrip('/')}/api/v1/datasets/download/{slug}"
    redirects = 0
    attempt = 0
    while True:
        request = DownloadRequest(
            url=url,
            headers=_headers_for(url, authorization),
        )
        try:
            response = active(request, timeout)
        except KaggleDownloadError:
            raise
        except OSError as exc:
            msg = "Kaggle download failed"
            raise KaggleDownloadError(msg) from exc

        if response.status in _REDIRECT_STATUSES:
            redirects += 1
            if redirects > max_redirects:
                msg = "Kaggle download exceeded the redirect limit"
                raise KaggleDownloadError(msg)
            location = _header(response.headers, "location")
            if not location:
                msg = "Kaggle download redirect is missing a location"
                raise KaggleDownloadError(msg)
            url = urljoin(url, location)
            continue

        if response.status == 200:
            if len(response.body) > max_download_bytes:
                msg = "Kaggle download exceeded the size limit"
                raise KaggleDownloadError(msg)
            return response.body

        if response.status in (401, 403):
            msg = f"Kaggle download was rejected with status {response.status}"
            raise KaggleDownloadError(msg)

        if response.status == 429 or response.status >= 500:
            attempt += 1
            if attempt >= max_attempts:
                msg = f"Kaggle download failed with status {response.status}"
                raise KaggleDownloadError(msg)
            pause(float(2 ** (attempt - 1)))
            continue

        msg = f"Kaggle download failed with status {response.status}"
        raise KaggleDownloadError(msg)


def extract_expected_members(
    payload: bytes,
    dest_dir: Path,
    expected_names: frozenset[str],
    *,
    max_uncompressed_bytes: int = MAX_UNCOMPRESSED_BYTES,
) -> None:
    """Extract exactly ``expected_names`` into ``dest_dir``.

    Members are published only after the whole archive passes the checks.
    Absolute paths, ``..``, nested names, and unexpected members fail the extract.
    """
    if not expected_names:
        msg = "expected file set is empty"
        raise KaggleDownloadError(msg)
    try:
        archive = zipfile.ZipFile(io.BytesIO(payload))
    except zipfile.BadZipFile as exc:
        msg = "Kaggle download is not a zip archive"
        raise KaggleDownloadError(msg) from exc

    with archive:
        names: list[str] = []
        declared = 0
        for info in archive.infolist():
            name = info.filename
            if info.is_dir() or _unsafe_member(name) or name not in expected_names:
                msg = "Kaggle archive contains an unexpected member"
                raise KaggleDownloadError(msg)
            declared += info.file_size
            if declared > max_uncompressed_bytes:
                msg = "Kaggle archive exceeds the uncompressed size limit"
                raise KaggleDownloadError(msg)
            names.append(name)
        if len(names) != len(set(names)) or set(names) != expected_names:
            msg = "Kaggle archive does not contain the expected files"
            raise KaggleDownloadError(msg)

        dest_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=dest_dir) as tmp_name:
            temporary = Path(tmp_name)
            written = 0
            for name in names:
                target = temporary / name
                with archive.open(name) as src, target.open("wb") as dst:
                    while True:
                        chunk = src.read(_CHUNK_BYTES)
                        if not chunk:
                            break
                        written += len(chunk)
                        if written > max_uncompressed_bytes:
                            msg = "Kaggle archive exceeds the uncompressed size limit"
                            raise KaggleDownloadError(msg)
                        dst.write(chunk)
            for name in names:
                (temporary / name).replace(dest_dir / name)


def _headers_for(url: str, authorization: str) -> dict[str, str]:
    headers = {"Accept": "application/zip"}
    host = urlparse(url).hostname
    if host is not None and host.lower() == _AUTH_HOST:
        headers["Authorization"] = authorization
    return headers


def _validate_slug(slug: str) -> None:
    owner, separator, name = slug.partition("/")
    if (
        not separator
        or "/" in name
        or _SLUG_PART.fullmatch(owner) is None
        or _SLUG_PART.fullmatch(name) is None
    ):
        msg = "Kaggle dataset slug must be owner/dataset"
        raise KaggleDownloadError(msg)


def _header(headers: Mapping[str, str], name: str) -> str | None:
    wanted = name.lower()
    for key, value in headers.items():
        if key.lower() == wanted:
            return value
    return None


def _unsafe_member(name: str) -> bool:
    if not name or name.strip() != name or "\\" in name:
        return True
    path = PurePosixPath(name)
    if path.is_absolute() or len(path.parts) != 1:
        return True
    return path.parts[0] in {"", ".", ".."}


def _read_limited(stream: object, limit: int) -> bytes:
    read = getattr(stream, "read", None)
    if read is None:
        msg = "Kaggle download failed"
        raise KaggleDownloadError(msg)
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = read(_CHUNK_BYTES)
        if not chunk:
            break
        total += len(chunk)
        if total > limit:
            msg = "Kaggle download exceeded the size limit"
            raise KaggleDownloadError(msg)
        chunks.append(chunk)
    return b"".join(chunks)


def _header_map(headers: object) -> dict[str, str]:
    items = getattr(headers, "items", None)
    if not callable(items):
        return {}
    return {str(key).lower(): str(value) for key, value in items()}


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Surface 3xx to the caller instead of following it."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        raise urllib.error.HTTPError(req.full_url, code, msg, headers, fp)


def _urllib_transport(request: DownloadRequest, timeout: float) -> DownloadResponse:
    opener = urllib.request.build_opener(_NoRedirectHandler)
    outgoing = urllib.request.Request(
        request.url,
        headers=dict(request.headers),
        method="GET",
    )
    try:
        with opener.open(outgoing, timeout=timeout) as response:
            body = _read_limited(response, MAX_DOWNLOAD_BYTES)
            status = int(getattr(response, "status", 200))
            return DownloadResponse(
                status=status,
                headers=_header_map(response.headers),
                body=body,
            )
    except urllib.error.HTTPError as exc:
        try:
            body = _read_limited(exc, MAX_DOWNLOAD_BYTES)
        except KaggleDownloadError:
            exc.close()
            raise
        status = int(exc.code)
        headers = _header_map(exc.headers)
        exc.close()
        return DownloadResponse(status=status, headers=headers, body=body)
    except urllib.error.URLError as exc:
        msg = "Kaggle download failed"
        raise KaggleDownloadError(msg) from exc
