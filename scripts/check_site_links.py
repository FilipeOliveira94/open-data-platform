"""Fail when the built site has a link that would 404 under its GitHub Pages base.

Usage:
    python3 scripts/check_site_links.py site/dist /open-data-platform/ \
        https://filipeoliveira94.github.io

Every href, src and srcset in dist/**/*.html, every <meta content> that is a
URL on the site origin, and every url(...) in dist/**/*.css is resolved against
the URL of the file it appears in. A target on the site origin must sit under
the base and exist in dist/: a path ending in "/" needs index.html, any other
path a file or a directory with index.html (the 404 page's own /404/ URL is
served from 404.html). Links to other hosts and scheme
links (mailto:, data:) are skipped. Errors name the file, the attribute and the
link path, never page text.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Iterator
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit

PLACEHOLDER_ORIGIN = "https://site.invalid"
CSS_URL = re.compile(r"""url\(\s*['"]?([^'")]+)['"]?\s*\)""")


class _Links(HTMLParser):
    def __init__(self, origin: str) -> None:
        super().__init__()
        self.origin = origin
        self.links: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        for name, value in attrs:
            if not value:
                continue
            if name == "srcset":
                self.links += [
                    (name, part.split()[0]) for part in value.split(",") if part.strip()
                ]
            elif name in ("href", "src"):
                self.links.append((name, value.strip()))
            elif tag == "meta" and name == "content" and value.startswith(self.origin):
                self.links.append(("meta content", value.strip()))


def _file_url(origin: str, dist: Path, path: Path, base: str) -> str:
    rel = path.relative_to(dist).as_posix()
    if rel == "index.html" or rel.endswith("/index.html"):
        rel = rel[: -len("index.html")]
    return f"{origin}{base}{rel}"


def _links(dist: Path, origin: str) -> Iterator[tuple[Path, str, str]]:
    for html in sorted(dist.rglob("*.html")):
        parser = _Links(origin)
        parser.feed(html.read_text(encoding="utf-8"))
        for attr, value in parser.links:
            yield html, attr, value
    for css in sorted(dist.rglob("*.css")):
        for value in CSS_URL.findall(css.read_text(encoding="utf-8")):
            yield css, "url()", value.strip()


def _problem(dist: Path, base: str, path: str) -> str | None:
    if path == base.rstrip("/"):
        path = base
    if path == f"{base}404/" and (dist / "404.html").is_file():
        # Starlight's 404 page names itself as /404/; Pages serves 404.html there.
        return None
    if not path.startswith(base):
        return f"outside base {base}"
    root = dist.resolve()
    target = (root / path[len(base) :]).resolve()
    if not target.is_relative_to(root):
        return "resolves outside dist"
    if path.endswith("/"):
        found = (target / "index.html").is_file()
    else:
        found = target.is_file() or (target / "index.html").is_file()
    return None if found else "no such file in dist"


def check_dist(dist: Path, base: str, site: str | None = None) -> list[str]:
    if not (base.startswith("/") and base.endswith("/")):
        raise ValueError("base must start and end with '/'")
    origin = site.rstrip("/") if site else PLACEHOLDER_ORIGIN
    scheme_host = urlsplit(origin)[:2]
    errors: list[str] = []
    for source, attr, value in _links(dist, origin):
        if value.startswith("#"):
            continue
        url = urlsplit(urljoin(_file_url(origin, dist, source, base), value))
        if (url.scheme, url.netloc) != scheme_host:
            continue
        path = unquote(url.path)
        reason = _problem(dist, base, path)
        if reason:
            where = source.relative_to(dist).as_posix()
            errors.append(f"{where}: {attr} -> {path} ({reason})")
    return errors


def main(argv: list[str]) -> int:
    if len(argv) not in (2, 3):
        print("usage: check_site_links.py <dist> <base> [site origin]")
        return 2
    dist, base = Path(argv[0]), argv[1]
    site = argv[2] if len(argv) == 3 else None
    pages = sum(1 for _ in dist.rglob("*.html"))
    if not pages:
        print(f"no HTML files under {dist}")
        return 1
    errors = check_dist(dist, base, site)
    for error in errors:
        print(error)
    print(f"{len(errors)} broken link(s) in {pages} HTML file(s) under base {base}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
