"""P9-06: check the published report site and extract its claims for comparison.

Usage:
    python3 scripts/p9_06_live_site_check.py \
        https://filipeoliveira94.github.io/open-data-platform/ out.json [repo root]

Fetches the five routes (Home, Architecture, Pipeline, Operations, Reference)
and records each one's HTTP status, <title> and <h1>. Requests every href, src
and srcset on them, and every url(...) in the site's CSS, once per URL; an
internal link with a #fragment also needs that id on the target page; an
external link's fragments are listed as unchecked. With a repo root, a
fragment on a link to this repository's blob/main Markdown must match a
heading slug of that file in the checkout. Each link is:

- ok: 2xx after redirects;
- broken: 4xx/5xx after one retry, or a missing #fragment;
- unverified: 429, timeout or network error after one retry.

Claims: every table row and every sentence inside <main> (code blocks
excluded) that contains a digit, with its page. They go to the JSON file for
the manual comparison against the repository docs.

Exit 1 when a route or an internal link is broken, 2 when only external links
are broken (still findings), 0 otherwise. Unverified links never fail the run.
Output names URLs, statuses and counts; the site is public and holds no row
values or secrets.
"""

from __future__ import annotations

import json
import re
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urldefrag, urljoin, urlsplit

ROUTES = {
    "Home": "",
    "Architecture": "architecture/",
    "Pipeline": "pipeline/",
    "Operations": "operations/",
    "Reference": "reference/",
}
USER_AGENT = "open-data-platform-site-check/1.0"
CSS_URL = re.compile(r"""url\(\s*['"]?([^'")]+)['"]?\s*\)""")
SENTENCE_END = re.compile(r"(?<=[.!?])\s+(?=[A-Z(`\[])")
SKIP_SCHEMES = ("mailto:", "data:", "javascript:", "tel:")
RETRY_DELAY_S = 2.0


@dataclass
class Response:
    status: int | None
    body: bytes = b""
    content_type: str = ""
    error: str = ""


Fetch = Callable[[str, str], Response]


def fetch_url(url: str, method: str) -> Response:
    request = urllib.request.Request(
        url, method=method, headers={"User-Agent": USER_AGENT}
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as reply:
            body = reply.read() if method == "GET" else b""
            return Response(reply.status, body, reply.headers.get("Content-Type", ""))
    except urllib.error.HTTPError as exc:
        return Response(exc.code, error=f"HTTP {exc.code}")
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return Response(None, error=type(exc).__name__)


def classify(response: Response) -> str:
    if response.status is not None and 200 <= response.status < 300:
        return "ok"
    if response.status is None or response.status == 429:
        return "unverified"
    return "broken"


class _Page(HTMLParser):
    """Links, ids, title, h1, and digit-bearing text of one HTML page."""

    BLOCKS = {"p", "li", "td", "th", "h1", "h2", "h3", "h4", "dd", "dt", "figcaption"}
    SKIP = {"script", "style", "pre", "template"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []
        self.ids: set[str] = set()
        self.title = ""
        self.h1 = ""
        self.texts: list[str] = []
        self.rows: list[list[str]] = []
        self._stack: list[str] = []
        self._main = 0
        self._skip = 0
        self._buf: list[str] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None
        self._in_title = False
        self._in_h1 = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        for name, value in attrs:
            if not value:
                continue
            if name == "id":
                self.ids.add(value)
            elif name == "srcset":
                self.links += [p.split()[0] for p in value.split(",") if p.strip()]
            elif name in ("href", "src"):
                self.links.append(value.strip())
        if tag in ("br", "img", "meta", "link", "input", "hr", "source"):
            return
        self._stack.append(tag)
        if tag == "main":
            self._main += 1
        if tag in self.SKIP:
            self._skip += 1
        if tag == "title":
            self._in_title = True
        if tag == "h1":
            self._in_h1 = True
        if self._main and not self._skip:
            if tag == "tr":
                self._row = []
            elif tag in ("td", "th") and self._row is not None:
                self._cell = []
            elif tag in self.BLOCKS:
                self._flush()

    def handle_endtag(self, tag: str) -> None:
        if tag not in self._stack:
            return
        while self._stack:
            open_tag = self._stack.pop()
            self._close(open_tag)
            if open_tag == tag:
                break

    def _close(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False
        if tag == "h1":
            self._in_h1 = False
        if tag in self.SKIP:
            self._skip -= 1
        if self._main and not self._skip:
            if tag in ("td", "th") and self._cell is not None and self._row is not None:
                self._row.append(_squash("".join(self._cell)))
                self._cell = None
            elif tag == "tr" and self._row is not None:
                self.rows.append(self._row)
                self._row = None
            elif tag in self.BLOCKS or tag == "main":
                self._flush()
        if tag == "main":
            self._main -= 1

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title += data
        if self._in_h1:
            self.h1 += data
        if not self._main or self._skip:
            return
        if self._cell is not None:
            self._cell.append(data)
        elif self._row is None:
            self._buf.append(data)

    def _flush(self) -> None:
        text = _squash("".join(self._buf))
        self._buf = []
        if text:
            self.texts.append(text)


def _squash(text: str) -> str:
    return " ".join(text.split())


def parse_page(html: str) -> _Page:
    page = _Page()
    page.feed(html)
    page.close()
    page._flush()
    return page


def claims_of(page: _Page) -> list[dict[str, str]]:
    claims: list[dict[str, str]] = []
    for row in page.rows:
        if any(re.search(r"\d", cell) for cell in row):
            claims.append({"kind": "table row", "text": " | ".join(row)})
    for text in page.texts:
        for sentence in SENTENCE_END.split(text):
            if re.search(r"\d", sentence):
                claims.append({"kind": "sentence", "text": sentence.strip()})
    return claims


def _retrying(fetch: Fetch, url: str, method: str) -> Response:
    response = fetch(url, method)
    if classify(response) == "ok":
        return response
    if method == "HEAD" and response.status in (403, 405, 501):
        response = fetch(url, "GET")
        if classify(response) == "ok":
            return response
    time.sleep(RETRY_DELAY_S)
    return fetch(url, "GET")


@dataclass
class Result:
    site: str
    pages: list[dict[str, object]] = field(default_factory=list)
    links: dict[str, dict[str, object]] = field(default_factory=dict)
    claims: list[dict[str, str]] = field(default_factory=list)

    def failing(self, scope: str) -> list[str]:
        return [
            url
            for url, link in self.links.items()
            if link["scope"] == scope and link["class"] == "broken"
        ]

    def exit_code(self) -> int:
        routes_broken = any(p["class"] != "ok" for p in self.pages)
        if routes_broken or self.failing("internal"):
            return 1
        return 2 if self.failing("external") else 0


def check_site(site: str, fetch: Fetch = fetch_url) -> Result:
    if not site.endswith("/"):
        raise ValueError("site URL must end with '/'")
    origin = urlsplit(site)[:2]
    result = Result(site)
    referrers: dict[str, set[str]] = {}
    fragments: dict[str, set[str]] = {}
    ids: dict[str, set[str]] = {}

    def note(target: str, page_name: str) -> None:
        if target.startswith("#") or target.lower().startswith(SKIP_SCHEMES):
            return
        url, fragment = urldefrag(target)
        referrers.setdefault(url, set()).add(page_name)
        if fragment:
            fragments.setdefault(url, set()).add(fragment)

    for name, route in ROUTES.items():
        url = site + route
        response = _retrying(fetch, url, "GET")
        entry: dict[str, object] = {
            "page": name,
            "url": url,
            "status": response.status,
            "class": classify(response),
        }
        if classify(response) == "ok":
            page = parse_page(response.body.decode("utf-8", "replace"))
            entry |= {"title": _squash(page.title), "h1": _squash(page.h1)}
            ids[url] = page.ids
            for target in page.links:
                note(urljoin(url, target), name)
            for claim in claims_of(page):
                result.claims.append({"page": name, **claim})
        result.pages.append(entry)

    for url in sorted(referrers):
        internal = urlsplit(url)[:2] == origin
        is_css = internal and urlsplit(url).path.endswith(".css")
        wants_body = internal and (url in fragments or is_css)
        response = _retrying(fetch, url, "GET" if wants_body else "HEAD")
        link: dict[str, object] = {
            "scope": "internal" if internal else "external",
            "status": response.status,
            "class": classify(response),
            "pages": sorted(referrers[url]),
        }
        if is_css and link["class"] == "ok":
            css = response.body.decode("utf-8", "replace")
            for target in CSS_URL.findall(css):
                note(urljoin(url, target.strip()), f"css:{url}")
        if internal and url in fragments and link["class"] == "ok":
            found = ids.get(url)
            if found is None:
                found = parse_page(response.body.decode("utf-8", "replace")).ids
            missing = sorted(fragments[url] - found)
            if missing:
                link |= {"class": "broken", "missing_fragments": missing}
        elif not internal and url in fragments:
            # GitHub renders heading ids client-side; check these against the repo.
            link["fragments_unchecked"] = sorted(fragments[url])
        result.links[url] = link

    # Fonts and images named only in CSS were noted after the main pass.
    for url in sorted(set(referrers) - set(result.links)):
        response = _retrying(fetch, url, "HEAD")
        result.links[url] = {
            "scope": "internal" if urlsplit(url)[:2] == origin else "external",
            "status": response.status,
            "class": classify(response),
            "pages": sorted(referrers[url]),
        }
    return result


REPO_BLOB = "https://github.com/FilipeOliveira94/open-data-platform/blob/main/"
HEADING = re.compile(r"#{1,6}\s+(.*)")


def heading_slugs(markdown: str) -> set[str]:
    """GitHub's heading ids: lowercase, punctuation dropped, spaces to hyphens."""
    slugs: set[str] = set()
    seen: dict[str, int] = {}
    fenced = False
    for line in markdown.splitlines():
        if line.lstrip().startswith("```"):
            fenced = not fenced
        match = None if fenced else HEADING.match(line)
        if not match:
            continue
        slug = re.sub(r"[^\w\- ]", "", match.group(1).strip().lower()).replace(" ", "-")
        count = seen.get(slug, 0)
        seen[slug] = count + 1
        slugs.add(slug if count == 0 else f"{slug}-{count}")
    return slugs


def check_repo_anchors(result: Result, root: Path) -> None:
    for url, link in result.links.items():
        fragments = link.get("fragments_unchecked")
        if not fragments or not url.startswith(REPO_BLOB) or not url.endswith(".md"):
            continue
        path = root / url[len(REPO_BLOB) :]
        have = (
            heading_slugs(path.read_text(encoding="utf-8")) if path.is_file() else set()
        )
        missing = sorted(set(fragments) - have)  # type: ignore[arg-type]
        del link["fragments_unchecked"]
        link["fragments_checked"] = len(fragments)  # type: ignore[arg-type]
        if missing:
            link |= {"class": "broken", "missing_fragments": missing}


def main(argv: list[str]) -> int:
    if len(argv) not in (2, 3):
        print(
            "usage: p9_06_live_site_check.py <site url ending in /> <out.json> [repo]"
        )
        return 2
    site, out = argv[0], argv[1]
    result = check_site(site)
    if len(argv) == 3:
        check_repo_anchors(result, Path(argv[2]))
    Path(out).write_text(
        json.dumps(
            {
                "site": result.site,
                "pages": result.pages,
                "links": result.links,
                "claims": result.claims,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    for page in result.pages:
        print(f"{page['page']}: {page['status']} {page['class']} h1={page.get('h1')!r}")
    counts: dict[tuple[object, object], int] = {}
    for link in result.links.values():
        key = (link["scope"], link["class"])
        counts[key] = counts.get(key, 0) + 1
    for (scope, cls), n in sorted(counts.items(), key=str):
        print(f"links {scope} {cls}: {n}")
    for url, link in result.links.items():
        if link["class"] != "ok":
            extra = link.get("missing_fragments", "")
            print(f"{link['class']} {link['scope']} {link['status']} {url} {extra}")
    unchecked = sum(
        len(link.get("fragments_unchecked", [])) for link in result.links.values()
    )
    checked = sum(
        int(link.get("fragments_checked", 0)) for link in result.links.values()
    )
    print(f"repo anchors checked: {checked}, external fragments unchecked: {unchecked}")
    print(f"claims: {len(result.claims)} written to {out}")
    return result.exit_code()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
