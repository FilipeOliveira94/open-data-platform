"""GET helper with a caller-supplied retry and redirect policy.

The transport returns 3xx responses. It does not follow redirects.
``get_with_retries`` applies ``RedirectPolicy`` and, when following, sends
the original headers to the next URL. A caller that must not resend a
header sets ``follow`` to false.

Error text does not include the response body. The Kaggle download client
keeps its own redirect and credential handling.
"""

from __future__ import annotations

import re
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from urllib.parse import urljoin

_CHUNK_BYTES = 64 * 1024
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_RETRY_AFTER_RE = re.compile(r"\d+(?:\.\d+)?")


@dataclass(frozen=True, slots=True)
class HttpRequest:
    """One GET. Redirects are a later request, not a transport feature."""

    url: str
    headers: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """HTTP response visible to the retry and redirect policy."""

    status: int
    headers: Mapping[str, str]
    body: bytes


Transport = Callable[[HttpRequest, float], HttpResponse]


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Which failures produce another attempt, and how long to wait.

    ``retry_status_codes`` matches those statuses. ``retry_status_at_least``
    also matches ``status >= value`` when it is not ``None``. A numeric
    ``Retry-After`` is used only for ``retry_after_status_codes`` and is
    capped at ``retry_after_cap_seconds``. An HTTP-date ``Retry-After``
    keeps the 1s, 2s, 4s backoff.
    """

    max_attempts: int
    retry_timeouts: bool
    retry_status_codes: frozenset[int]
    retry_status_at_least: int | None
    retry_after_status_codes: frozenset[int]
    retry_after_cap_seconds: float


@dataclass(frozen=True, slots=True)
class RedirectPolicy:
    """Whether a 3xx response becomes another GET.

    ``max_redirects`` counts hops. Those hops do not consume retry attempts.
    """

    follow: bool
    max_redirects: int


def get_with_retries(
    request: HttpRequest,
    *,
    timeout: float,
    max_response_bytes: int,
    retry: RetryPolicy,
    redirect: RedirectPolicy,
    error_type: type[Exception],
    message_prefix: str,
    transport: Transport | None = None,
    sleep: Callable[[float], None] | None = None,
) -> bytes:
    """GET ``request`` and return the success body.

    The same ``message_prefix`` builds every failure message. A response
    larger than ``max_response_bytes`` fails on that attempt and is not
    retried. The default transport reads only up to that limit.
    """
    _require_exception_type(error_type)
    prefix = _require_prefix(message_prefix, error_type)
    _require_timeout(timeout, error_type)
    limit = _require_positive_int("max_response_bytes", max_response_bytes, error_type)
    _validate_retry(retry, error_type)
    _validate_redirect(redirect, error_type)
    pause = time.sleep if sleep is None else sleep
    active = (
        _urllib_transport(
            max_response_bytes=limit,
            error_type=error_type,
            message_prefix=prefix,
        )
        if transport is None
        else transport
    )
    url = request.url
    redirects = 0
    attempt = 0
    while True:
        current = HttpRequest(url=url, headers=request.headers)
        try:
            response = active(current, float(timeout))
        except TimeoutError:
            if not retry.retry_timeouts:
                raise error_type(f"{prefix} request timed out") from None
            attempt = _pause_or_stop(
                attempt=attempt,
                max_attempts=retry.max_attempts,
                pause=pause,
                delay=_backoff(attempt + 1),
                message=f"{prefix} request timed out",
                error_type=error_type,
            )
            continue
        except error_type:
            raise
        except OSError as exc:
            msg = f"{prefix} request failed"
            raise error_type(msg) from exc

        if response.status in _REDIRECT_STATUSES and redirect.follow:
            redirects += 1
            if redirects > redirect.max_redirects:
                msg = f"{prefix} request exceeded the redirect limit"
                raise error_type(msg)
            location = _header(response.headers, "location")
            if not location:
                msg = f"{prefix} request redirect is missing a location"
                raise error_type(msg)
            url = urljoin(url, location)
            continue

        if response.status == 200:
            if len(response.body) > limit:
                msg = f"{prefix} response exceeded the size limit"
                raise error_type(msg)
            return response.body

        if _retries_status(response.status, retry):
            retry_after = None
            if response.status in retry.retry_after_status_codes:
                retry_after = _retry_after_seconds(
                    response.headers,
                    retry.retry_after_cap_seconds,
                )
            delay = _backoff(attempt + 1) if retry_after is None else retry_after
            attempt = _pause_or_stop(
                attempt=attempt,
                max_attempts=retry.max_attempts,
                pause=pause,
                delay=delay,
                message=f"{prefix} request failed with status {response.status}",
                error_type=error_type,
            )
            continue

        msg = f"{prefix} request failed with status {response.status}"
        raise error_type(msg)


def _urllib_transport(
    *,
    max_response_bytes: int,
    error_type: type[Exception],
    message_prefix: str,
) -> Transport:
    def transport(request: HttpRequest, timeout: float) -> HttpResponse:
        opener = urllib.request.build_opener(_NoRedirectHandler)
        outgoing = urllib.request.Request(
            request.url,
            headers=dict(request.headers),
            method="GET",
        )
        try:
            with opener.open(outgoing, timeout=timeout) as response:
                body = _read_limited(
                    response,
                    max_response_bytes,
                    error_type,
                    message_prefix,
                )
                status = int(getattr(response, "status", 200))
                return HttpResponse(
                    status=status,
                    headers=_header_map(response.headers),
                    body=body,
                )
        except TimeoutError:
            raise
        except error_type:
            raise
        except urllib.error.HTTPError as exc:
            try:
                body = _read_limited(
                    exc,
                    max_response_bytes,
                    error_type,
                    message_prefix,
                )
            except error_type:
                exc.close()
                raise
            status = int(exc.code)
            headers = _header_map(exc.headers)
            exc.close()
            return HttpResponse(status=status, headers=headers, body=body)
        except urllib.error.URLError as exc:
            if isinstance(exc.reason, TimeoutError):
                raise TimeoutError(f"{message_prefix} request timed out") from exc
            msg = f"{message_prefix} request failed"
            raise error_type(msg) from exc

    return transport


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Surface 3xx to the caller instead of following it."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        raise urllib.error.HTTPError(req.full_url, code, msg, headers, fp)


def _read_limited(
    stream: object,
    limit: int,
    error_type: type[Exception],
    message_prefix: str,
) -> bytes:
    read = getattr(stream, "read", None)
    if not callable(read):
        msg = f"{message_prefix} request failed"
        raise error_type(msg)
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = read(_CHUNK_BYTES)
        if not chunk:
            break
        if not isinstance(chunk, bytes):
            msg = f"{message_prefix} request failed"
            raise error_type(msg)
        total += len(chunk)
        if total > limit:
            msg = f"{message_prefix} response exceeded the size limit"
            raise error_type(msg)
        chunks.append(chunk)
    return b"".join(chunks)


def _pause_or_stop(
    *,
    attempt: int,
    max_attempts: int,
    pause: Callable[[float], None],
    delay: float,
    message: str,
    error_type: type[Exception],
) -> int:
    attempt += 1
    if attempt >= max_attempts:
        raise error_type(message)
    pause(delay)
    return attempt


def _backoff(failed_attempt: int) -> float:
    return float(2 ** (failed_attempt - 1))


def _retries_status(status: int, retry: RetryPolicy) -> bool:
    if status in retry.retry_status_codes:
        return True
    if retry.retry_status_at_least is None:
        return False
    return status >= retry.retry_status_at_least


def _retry_after_seconds(headers: Mapping[str, str], cap: float) -> float | None:
    raw = _header(headers, "retry-after")
    if raw is None:
        return None
    text = raw.strip()
    if _RETRY_AFTER_RE.fullmatch(text) is None:
        return None
    return min(float(text), cap)


def _header(headers: Mapping[str, str], name: str) -> str | None:
    wanted = name.lower()
    for key, value in headers.items():
        if key.lower() == wanted:
            return value
    return None


def _header_map(headers: object) -> dict[str, str]:
    items = getattr(headers, "items", None)
    if not callable(items):
        return {}
    return {str(key).lower(): str(value) for key, value in items()}


def _require_exception_type(error_type: type[Exception]) -> None:
    if not isinstance(error_type, type) or not issubclass(error_type, Exception):
        msg = "error_type must be an exception class"
        raise TypeError(msg)


def _require_prefix(message_prefix: str, error_type: type[Exception]) -> str:
    if (
        not isinstance(message_prefix, str)
        or not message_prefix
        or message_prefix.strip() != message_prefix
    ):
        msg = "message_prefix must be non-empty"
        raise error_type(msg)
    return message_prefix


def _require_timeout(timeout: float, error_type: type[Exception]) -> float:
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        msg = "timeout must be a positive number"
        raise error_type(msg)
    if timeout <= 0:
        msg = "timeout must be a positive number"
        raise error_type(msg)
    return float(timeout)


def _require_positive_int(name: str, value: int, error_type: type[Exception]) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        msg = f"{name} must be an integer >= 1"
        raise error_type(msg)
    return value


def _validate_retry(retry: RetryPolicy, error_type: type[Exception]) -> None:
    _require_positive_int("max_attempts", retry.max_attempts, error_type)
    if not isinstance(retry.retry_timeouts, bool):
        msg = "retry_timeouts must be a boolean"
        raise error_type(msg)
    _require_status_codes("retry_status_codes", retry.retry_status_codes, error_type)
    _require_status_codes(
        "retry_after_status_codes",
        retry.retry_after_status_codes,
        error_type,
    )
    at_least = retry.retry_status_at_least
    if at_least is not None and (
        isinstance(at_least, bool) or not isinstance(at_least, int)
    ):
        msg = "retry_status_at_least must be an integer or None"
        raise error_type(msg)
    cap = retry.retry_after_cap_seconds
    if isinstance(cap, bool) or not isinstance(cap, (int, float)) or cap < 0:
        msg = "retry_after_cap_seconds must be a non-negative number"
        raise error_type(msg)


def _validate_redirect(redirect: RedirectPolicy, error_type: type[Exception]) -> None:
    if not isinstance(redirect.follow, bool):
        msg = "follow must be a boolean"
        raise error_type(msg)
    if (
        isinstance(redirect.max_redirects, bool)
        or not isinstance(redirect.max_redirects, int)
        or redirect.max_redirects < 0
    ):
        msg = "max_redirects must be an integer >= 0"
        raise error_type(msg)


def _require_status_codes(
    name: str,
    codes: frozenset[int],
    error_type: type[Exception],
) -> None:
    if not isinstance(codes, frozenset):
        msg = f"{name} must be a frozenset of status codes"
        raise error_type(msg)
    for code in codes:
        if isinstance(code, bool) or not isinstance(code, int):
            msg = f"{name} must be a frozenset of status codes"
            raise error_type(msg)
