"""Fail when the built site has a link that would 404 under its GitHub Pages base.

Usage: python3 scripts/check_site_links.py site/dist /open-data-platform/

Every href, src and srcset in dist/**/*.html is resolved against the page's
own URL. An internal target must sit under the base and exist in dist/, as a
file or as a directory with index.html. External and scheme links are skipped.
Errors name the HTML file, the attribute and the link path, never page text.
"""

from __future__ import annotations

import sys
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit

HOST = "site.invalid"
ATTRS = ("href", "src", "srcset")


class _Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        for name, value in attrs:
            if name not in ATTRS or not value:
                continue
            if name == "srcset":
                self.links += [
                    (name, part.split()[0]) for part in value.split(",") if part.strip()
                ]
            else:
                self.links.append((name, value.strip()))


def _page_url(dist: Path, html: Path, base: str) -> str:
    rel = html.relative_to(dist).as_posix()
    if rel == "index.html" or rel.endswith("/index.html"):
        rel = rel[: -len("index.html")]
    return f"https://{HOST}{base}{rel}"


def _exists(dist: Path, rel: str) -> bool:
    target = dist / rel
    return target.is_file() or (target / "index.html").is_file()


def check_dist(dist: Path, base: str) -> list[str]:
    if not (base.startswith("/") and base.endswith("/")):
        raise ValueError("base must start and end with '/'")
    errors: list[str] = []
    for html in sorted(dist.rglob("*.html")):
        parser = _Links()
        parser.feed(html.read_text(encoding="utf-8"))
        page = _page_url(dist, html, base)
        for attr, value in parser.links:
            if value.startswith("#"):
                continue
            url = urlsplit(urljoin(page, value))
            if url.scheme != "https" or url.netloc != HOST:
                continue
            path = unquote(url.path)
            where = f"{html.relative_to(dist).as_posix()}: {attr} -> {path}"
            if path == base.rstrip("/"):
                path = base
            if not path.startswith(base):
                errors.append(f"{where} (outside base {base})")
            elif not _exists(dist, path[len(base) :]):
                errors.append(f"{where} (no such file in dist)")
    return errors


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: check_site_links.py <dist> <base>")
        return 2
    dist, base = Path(argv[0]), argv[1]
    if not any(dist.rglob("*.html")):
        print(f"no HTML files under {dist}")
        return 1
    errors = check_dist(dist, base)
    for error in errors:
        print(error)
    count = sum(1 for _ in dist.rglob("*.html"))
    print(f"{len(errors)} broken link(s) in {count} HTML file(s) under base {base}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
