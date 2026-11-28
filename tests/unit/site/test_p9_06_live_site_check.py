"""Tests for scripts/p9_06_live_site_check.py, the P9-06 live site check.

No network: a fake fetch serves fixture pages, so link classes, exit codes,
fragment checks, and claim extraction are checked without the live site.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
SITE = "https://example.test/odp/"
REPO = "https://github.com/FilipeOliveira94/open-data-platform/blob/main/"


def _load():
    spec = importlib.util.spec_from_file_location(
        "p9_06_live_site_check", ROOT / "scripts" / "p9_06_live_site_check.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    # dataclasses look the module up in sys.modules while the class is built.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


check = _load()

HOME = f"""<html><head><title>Home | ODP</title></head><body>
<nav><a href="/odp/architecture/">Architecture 2</a></nav>
<main><h1>Open Data Platform</h1>
<p>The map is the target. Phase 3 ran on 2026-11-27 with 152 reads. No digits here.</p>
<table><tr><th>Phase</th><th>Status</th></tr>
<tr><td>3</td><td>Verified</td></tr><tr><td>x</td><td>y</td></tr></table>
<pre>make build 42</pre>
<a href="/odp/pipeline/#gate">gate</a><a href="/odp/pipeline/#gone">gone</a>
<a href="{REPO}docs/a.md#real-heading">doc</a>
<a href="https://ext.test/rate">rate</a><a href="mailto:x@example.test">m</a>
<img src="/odp/_astro/a.png"><link rel="stylesheet" href="/odp/_astro/s.css">
</main></body></html>"""

PAGE = '<html><head><title>{0}</title></head><body><main id="main"><h1>{0}</h1>'
PAGE += '<h2 id="gate">Gate</h2></main></body></html>'


class FakeFetch:
    def __init__(self, overrides: dict[str, check.Response] | None = None) -> None:
        self.calls: list[tuple[str, str]] = []
        self.pages = {SITE: HOME}
        for route in ("architecture/", "pipeline/", "operations/", "reference/"):
            self.pages[SITE + route] = PAGE.format(route.strip("/").title())
        self.overrides = overrides or {}

    def __call__(self, url: str, method: str):
        self.calls.append((url, method))
        if url in self.overrides:
            return self.overrides[url]
        if url in self.pages:
            return check.Response(200, self.pages[url].encode(), "text/html")
        if url.endswith(".css"):
            return check.Response(200, b"@font-face{src:url(./f.woff2)}", "text/css")
        if url == "https://ext.test/rate":
            return check.Response(429, error="HTTP 429")
        return check.Response(200)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(check, "RETRY_DELAY_S", 0)


def test_classify_statuses() -> None:
    assert check.classify(check.Response(204)) == "ok"
    assert check.classify(check.Response(404)) == "broken"
    assert check.classify(check.Response(503)) == "broken"
    assert check.classify(check.Response(429)) == "unverified"
    assert check.classify(check.Response(None, error="TimeoutError")) == "unverified"


def test_pages_links_and_fragments() -> None:
    result = check.check_site(SITE, FakeFetch())
    assert [p["page"] for p in result.pages] == list(check.ROUTES)
    assert result.pages[0]["title"] == "Home | ODP"
    assert result.pages[0]["h1"] == "Open Data Platform"
    pipeline = result.links[SITE + "pipeline/"]
    assert pipeline["class"] == "broken"
    assert pipeline["missing_fragments"] == ["gone"]
    assert result.links["https://ext.test/rate"]["class"] == "unverified"
    assert result.links[SITE + "_astro/f.woff2"]["class"] == "ok"
    assert not any(url.startswith("mailto:") for url in result.links)
    assert result.links[REPO + "docs/a.md"]["fragments_unchecked"] == ["real-heading"]
    assert result.exit_code() == 1


def test_exit_codes() -> None:
    clean = FakeFetch()
    clean.pages[SITE] = HOME.replace('<a href="/odp/pipeline/#gone">gone</a>', "")
    assert check.check_site(SITE, clean).exit_code() == 0

    external = FakeFetch({"https://ext.test/rate": check.Response(404)})
    external.pages[SITE] = clean.pages[SITE]
    assert check.check_site(SITE, external).exit_code() == 2

    missing = FakeFetch({SITE + "reference/": check.Response(404)})
    missing.pages[SITE] = clean.pages[SITE]
    result = check.check_site(SITE, missing)
    assert result.pages[-1]["class"] == "broken"
    assert result.exit_code() == 1


def test_retry_after_unverified_then_ok() -> None:
    answers = iter([check.Response(None, error="TimeoutError"), check.Response(200)])
    fetch = FakeFetch()
    base = fetch.__call__

    def flaky(url: str, method: str):
        if url == "https://ext.test/rate":
            return next(answers)
        return base(url, method)

    result = check.check_site(SITE, flaky)
    assert result.links["https://ext.test/rate"]["class"] == "ok"


def test_claims_cover_rows_and_sentences_with_digits() -> None:
    result = check.check_site(SITE, FakeFetch())
    texts = [(c["kind"], c["text"]) for c in result.claims if c["page"] == "Home"]
    assert ("table row", "3 | Verified") in texts
    assert ("sentence", "Phase 3 ran on 2026-11-27 with 152 reads.") in texts
    assert all("No digits" not in text for _, text in texts)
    assert all("make build" not in text for _, text in texts)  # <pre> skipped
    assert all("Architecture 2" not in text for _, text in texts)  # nav skipped


def test_repo_anchors_checked_against_checkout(tmp_path: Path) -> None:
    doc = tmp_path / "docs" / "a.md"
    doc.parent.mkdir()
    doc.write_text("# Title\n\n```\n# not a heading\n```\n## Real heading\n")
    result = check.check_site(SITE, FakeFetch())
    check.check_repo_anchors(result, tmp_path)
    link = result.links[REPO + "docs/a.md"]
    assert link["class"] == "ok" and link["fragments_checked"] == 1

    doc.write_text("# Title\n")
    result = check.check_site(SITE, FakeFetch())
    check.check_repo_anchors(result, tmp_path)
    link = result.links[REPO + "docs/a.md"]
    assert link["class"] == "broken"
    assert link["missing_fragments"] == ["real-heading"]


def test_heading_slugs_follow_github() -> None:
    slugs = check.heading_slugs("## Catchup, gaps, and backfill\n## A b\n## A b\n")
    assert slugs == {"catchup-gaps-and-backfill", "a-b", "a-b-1"}
