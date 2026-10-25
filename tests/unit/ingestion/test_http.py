"""Unit tests for the shared GET helper. No public API calls."""

from __future__ import annotations

import gzip
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from open_data_platform.ingestion.http import (
    HttpRequest,
    HttpResponse,
    RedirectPolicy,
    RetryPolicy,
    get_with_retries,
)


class ExampleError(ValueError):
    """Failure type supplied by a caller in these tests."""


class _Steps:
    """Return or raise the next scripted step."""

    def __init__(self, steps: list[HttpResponse | Exception]) -> None:
        self.steps = steps
        self.calls = 0
        self.requests: list[HttpRequest] = []

    def __call__(self, request: HttpRequest, timeout: float) -> HttpResponse:
        del timeout
        self.requests.append(request)
        step = self.steps[self.calls]
        self.calls += 1
        if isinstance(step, Exception):
            raise step
        return step


def _retry(
    *,
    max_attempts: int = 3,
    retry_timeouts: bool = True,
    retry_status_codes: frozenset[int] | None = None,
    retry_status_at_least: int | None = 500,
    retry_after_status_codes: frozenset[int] | None = None,
    retry_after_cap_seconds: float = 30.0,
) -> RetryPolicy:
    return RetryPolicy(
        max_attempts=max_attempts,
        retry_timeouts=retry_timeouts,
        retry_status_codes=(
            frozenset({429}) if retry_status_codes is None else retry_status_codes
        ),
        retry_status_at_least=retry_status_at_least,
        retry_after_status_codes=(
            frozenset({429})
            if retry_after_status_codes is None
            else retry_after_status_codes
        ),
        retry_after_cap_seconds=retry_after_cap_seconds,
    )


def _call(
    transport: _Steps,
    *,
    retry: RetryPolicy | None = None,
    redirect: RedirectPolicy | None = None,
    max_response_bytes: int = 64,
    sleeps: list[float] | None = None,
    headers: dict[str, str] | None = None,
    url: str = "https://example.test/items",
) -> bytes:
    def pause(seconds: float) -> None:
        if sleeps is not None:
            sleeps.append(seconds)

    return get_with_retries(
        HttpRequest(
            url=url,
            headers={"Accept": "application/json", **(headers or {})},
        ),
        timeout=5,
        max_response_bytes=max_response_bytes,
        retry=_retry() if retry is None else retry,
        redirect=(
            RedirectPolicy(follow=False, max_redirects=0)
            if redirect is None
            else redirect
        ),
        error_type=ExampleError,
        message_prefix="Example",
        transport=transport,
        sleep=pause,
    )


def test_timeouts_use_the_backoff_and_then_succeed() -> None:
    transport = _Steps(
        [TimeoutError("slow"), TimeoutError("slow"), HttpResponse(200, {}, b"ok")]
    )
    seen: list[float] = []

    assert _call(transport, sleeps=seen) == b"ok"
    assert seen == [1.0, 2.0]
    assert transport.calls == 3


def test_disabled_timeout_retry_fails_on_the_first_attempt() -> None:
    transport = _Steps([TimeoutError("slow")])
    seen: list[float] = []

    with pytest.raises(ExampleError, match="timed out") as caught:
        _call(transport, retry=_retry(retry_timeouts=False), sleeps=seen)

    assert str(caught.value) == "Example request timed out"
    assert "slow" not in str(caught.value)
    assert transport.calls == 1
    assert seen == []


def test_server_error_backoff_skips_retry_after_for_other_statuses() -> None:
    transport = _Steps(
        [
            HttpResponse(500, {"Retry-After": "120"}, b"no"),
            HttpResponse(200, {}, b"ok"),
        ]
    )
    seen: list[float] = []

    assert _call(transport, sleeps=seen) == b"ok"
    assert seen == [1.0]


def test_retry_after_seconds_replace_the_backoff_and_are_capped() -> None:
    transport = _Steps(
        [
            HttpResponse(429, {"Retry-After": "120"}, b"later"),
            HttpResponse(200, {}, b"ok"),
        ]
    )
    seen: list[float] = []

    assert _call(transport, sleeps=seen) == b"ok"
    assert seen == [30.0]


def test_fractional_retry_after_is_kept() -> None:
    transport = _Steps(
        [
            HttpResponse(429, {"Retry-After": "1.5"}, b"later"),
            HttpResponse(200, {}, b"ok"),
        ]
    )
    seen: list[float] = []

    assert _call(transport, sleeps=seen) == b"ok"
    assert seen == [1.5]


def test_http_date_retry_after_keeps_the_backoff() -> None:
    transport = _Steps(
        [
            HttpResponse(
                429,
                {"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"},
                b"later",
            ),
            HttpResponse(200, {}, b"ok"),
        ]
    )
    seen: list[float] = []

    assert _call(transport, sleeps=seen) == b"ok"
    assert seen == [1.0]


def test_explicit_status_code_is_retried_without_the_server_error_band() -> None:
    transport = _Steps([HttpResponse(408, {}, b"wait"), HttpResponse(200, {}, b"ok")])
    seen: list[float] = []
    retry = _retry(
        retry_status_codes=frozenset({408}),
        retry_status_at_least=None,
        retry_after_status_codes=frozenset(),
    )

    assert _call(transport, retry=retry, sleeps=seen) == b"ok"
    assert seen == [1.0]


def test_non_retryable_status_fails_immediately_and_hides_the_body() -> None:
    marker = "super-secret-body"
    transport = _Steps([HttpResponse(404, {}, marker.encode())])
    seen: list[float] = []

    with pytest.raises(ExampleError) as caught:
        _call(transport, sleeps=seen)

    assert str(caught.value) == "Example request failed with status 404"
    assert marker not in str(caught.value)
    assert transport.calls == 1
    assert seen == []


def test_connection_error_is_not_retried() -> None:
    transport = _Steps([OSError("down")])
    seen: list[float] = []

    with pytest.raises(ExampleError) as caught:
        _call(transport, sleeps=seen)

    assert str(caught.value) == "Example request failed"
    assert "down" not in str(caught.value)
    assert transport.calls == 1
    assert seen == []


def test_oversized_success_body_is_not_retried() -> None:
    transport = _Steps([HttpResponse(200, {}, b"abcdef")])
    seen: list[float] = []

    with pytest.raises(ExampleError, match="size limit"):
        _call(transport, max_response_bytes=4, sleeps=seen)

    assert transport.calls == 1
    assert seen == []


def test_followed_redirect_resends_headers_and_resolves_a_relative_location() -> None:
    transport = _Steps(
        [
            HttpResponse(302, {"Location": "/next"}, b""),
            HttpResponse(200, {}, b"landed"),
        ]
    )

    body = _call(
        transport,
        redirect=RedirectPolicy(follow=True, max_redirects=1),
        headers={"Authorization": "secret"},
        url="https://example.test/start",
    )

    assert body == b"landed"
    assert [item.url for item in transport.requests] == [
        "https://example.test/start",
        "https://example.test/next",
    ]
    assert transport.requests[1].headers["Authorization"] == "secret"


def test_redirect_limit_stops_before_another_hop() -> None:
    transport = _Steps(
        [
            HttpResponse(302, {"Location": "/a"}, b""),
            HttpResponse(302, {"Location": "/b"}, b""),
            HttpResponse(200, {}, b"nope"),
        ]
    )

    with pytest.raises(ExampleError, match="redirect limit"):
        _call(transport, redirect=RedirectPolicy(follow=True, max_redirects=1))

    assert transport.calls == 2


def test_redirect_without_a_location_hides_the_body() -> None:
    marker = "secret-body"
    transport = _Steps([HttpResponse(301, {}, marker.encode())])

    with pytest.raises(ExampleError) as caught:
        _call(transport, redirect=RedirectPolicy(follow=True, max_redirects=2))

    assert str(caught.value) == "Example request redirect is missing a location"
    assert marker not in str(caught.value)


def test_disabled_redirect_fails_on_the_redirect_status() -> None:
    transport = _Steps(
        [HttpResponse(302, {"Location": "https://elsewhere.test/landed"}, b"secret")]
    )
    seen: list[float] = []

    with pytest.raises(ExampleError, match="status 302"):
        _call(transport, sleeps=seen)

    assert transport.calls == 1
    assert seen == []


def test_default_transport_leaves_a_redirect_unfollowed() -> None:
    hits: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            hits.append(self.path)
            if self.path == "/start":
                self.send_response(302)
                self.send_header("Location", "/done")
                self.end_headers()
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"landed")

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with pytest.raises(ExampleError, match="status 302"):
            get_with_retries(
                HttpRequest(url=f"http://127.0.0.1:{port}/start", headers={}),
                timeout=5,
                max_response_bytes=64,
                retry=_retry(max_attempts=1),
                redirect=RedirectPolicy(follow=False, max_redirects=0),
                error_type=ExampleError,
                message_prefix="Example",
                sleep=lambda _seconds: None,
            )
    finally:
        server.shutdown()
        server.server_close()

    assert hits == ["/start"]


def test_default_transport_stops_at_the_caller_byte_limit() -> None:
    hits = {"n": 0}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            hits["n"] += 1
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"0123456789abcdef")

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with pytest.raises(ExampleError, match="size limit"):
            get_with_retries(
                HttpRequest(url=f"http://127.0.0.1:{port}/blob", headers={}),
                timeout=5,
                max_response_bytes=8,
                retry=_retry(),
                redirect=RedirectPolicy(follow=False, max_redirects=0),
                error_type=ExampleError,
                message_prefix="Example",
                sleep=lambda _seconds: None,
            )
    finally:
        server.shutdown()
        server.server_close()

    assert hits["n"] == 1


def test_default_transport_follows_one_redirect_when_asked() -> None:
    hits: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            hits.append(self.path)
            if self.path == "/start":
                self.send_response(302)
                self.send_header("Location", "/done")
                self.end_headers()
                return
            body = b"landed"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        body = get_with_retries(
            HttpRequest(
                url=f"http://127.0.0.1:{port}/start",
                headers={"User-Agent": "example-test"},
            ),
            timeout=5,
            max_response_bytes=64,
            retry=_retry(max_attempts=1),
            redirect=RedirectPolicy(follow=True, max_redirects=1),
            error_type=ExampleError,
            message_prefix="Example",
            sleep=lambda _seconds: None,
        )
    finally:
        server.shutdown()
        server.server_close()

    assert body == b"landed"
    assert hits == ["/start", "/done"]


def _serve(handler: type[BaseHTTPRequestHandler]) -> tuple[ThreadingHTTPServer, int]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = int(server.server_address[1])
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, port


def test_default_transport_decodes_gzip_before_returning_the_body() -> None:
    payload = b'[{"municipio-id":1}]'
    encoded = gzip.compress(payload)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server, port = _serve(Handler)
    try:
        body = get_with_retries(
            HttpRequest(url=f"http://127.0.0.1:{port}/municipios", headers={}),
            timeout=5,
            max_response_bytes=64,
            retry=_retry(max_attempts=1),
            redirect=RedirectPolicy(follow=False, max_redirects=0),
            error_type=ExampleError,
            message_prefix="Example",
            sleep=lambda _seconds: None,
        )
    finally:
        server.shutdown()
        server.server_close()

    assert body == payload


def test_gzip_body_that_expands_past_the_limit_fails() -> None:
    encoded = gzip.compress(b"a" * 64)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Encoding", "gzip")
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server, port = _serve(Handler)
    try:
        with pytest.raises(ExampleError, match="size limit"):
            get_with_retries(
                HttpRequest(url=f"http://127.0.0.1:{port}/blob", headers={}),
                timeout=5,
                max_response_bytes=8,
                retry=_retry(max_attempts=1),
                redirect=RedirectPolicy(follow=False, max_redirects=0),
                error_type=ExampleError,
                message_prefix="Example",
                sleep=lambda _seconds: None,
            )
    finally:
        server.shutdown()
        server.server_close()


def test_unsupported_content_encoding_fails() -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Encoding", "br")
            self.end_headers()
            self.wfile.write(b"not-json")

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server, port = _serve(Handler)
    try:
        with pytest.raises(ExampleError, match="content encoding is not supported"):
            get_with_retries(
                HttpRequest(url=f"http://127.0.0.1:{port}/blob", headers={}),
                timeout=5,
                max_response_bytes=64,
                retry=_retry(max_attempts=1),
                redirect=RedirectPolicy(follow=False, max_redirects=0),
                error_type=ExampleError,
                message_prefix="Example",
                sleep=lambda _seconds: None,
            )
    finally:
        server.shutdown()
        server.server_close()
