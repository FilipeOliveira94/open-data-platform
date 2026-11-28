"""Tests for scripts/check_site_links.py, the base-path guard on the built site.

A wrong or missing Astro `base` still deploys green, but every asset and page
link 404s under the project URL. The check fails the build instead.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
BASE = "/open-data-platform/"
PLANTED = "PLANTED-ROW-VALUE-7f3a"


def _load():
    spec = importlib.util.spec_from_file_location(
        "check_site_links", ROOT / "scripts" / "check_site_links.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


check = _load()


def _page(dist: Path, rel: str, body: str) -> None:
    path = dist / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"<html><body><p>{PLANTED}</p>{body}</body></html>", encoding="utf-8"
    )


@pytest.fixture
def dist(tmp_path: Path) -> Path:
    root = tmp_path / "dist"
    (root / "_astro").mkdir(parents=True)
    (root / "_astro" / "a.css").write_text("body{}", encoding="utf-8")
    (root / "img.png").write_bytes(b"png")
    _page(root, "pipeline/index.html", "")
    return root


def test_prefixed_links_to_existing_targets_pass(dist: Path) -> None:
    _page(
        dist,
        "index.html",
        f'<link rel="stylesheet" href="{BASE}_astro/a.css">'
        f'<a href="{BASE}pipeline/">p</a><a href="{BASE}pipeline">p</a>'
        f'<a href="{BASE}">home</a><a href="{BASE.rstrip("/")}">home</a>'
        f'<a href="{BASE}pipeline/?q=1#top">p</a><img src="{BASE}img.png">',
    )
    assert check.check_dist(dist, BASE) == []


def test_unprefixed_root_link_fails(dist: Path) -> None:
    _page(dist, "index.html", '<link rel="stylesheet" href="/_astro/a.css">')
    errors = check.check_dist(dist, BASE)
    assert len(errors) == 1
    assert "index.html" in errors[0] and "/_astro/a.css" in errors[0]


def test_prefixed_link_to_missing_page_fails(dist: Path) -> None:
    _page(dist, "index.html", f'<a href="{BASE}missing/">x</a>')
    errors = check.check_dist(dist, BASE)
    assert len(errors) == 1 and "/open-data-platform/missing/" in errors[0]


def test_relative_links_resolve_against_the_page_url(dist: Path) -> None:
    _page(
        dist,
        "operations/index.html",
        '<a href="../pipeline/">ok</a><a href="../gone/">x</a>',
    )
    errors = check.check_dist(dist, BASE)
    assert len(errors) == 1 and "/open-data-platform/gone/" in errors[0]


def test_srcset_entries_are_each_checked(dist: Path) -> None:
    _page(dist, "index.html", f'<img srcset="{BASE}img.png 1x, /img.png 2x">')
    errors = check.check_dist(dist, BASE)
    assert len(errors) == 1 and "srcset" in errors[0] and "-> /img.png" in errors[0]


def test_external_fragment_and_scheme_links_are_skipped(dist: Path) -> None:
    _page(
        dist,
        "index.html",
        '<a href="https://github.com/x">g</a><a href="//cdn.example/x.js">c</a>'
        '<a href="#main">m</a><a href="mailto:a@b">e</a>'
        '<img src="data:image/png;base64,AA">',
    )
    assert check.check_dist(dist, BASE) == []


def test_errors_name_paths_not_page_text(dist: Path) -> None:
    # Every page carries PLANTED as body text. A link path may be shown;
    # page text never is.
    _page(dist, "index.html", '<a href="/x/">x</a><a href="/y/">y</a>')
    errors = check.check_dist(dist, BASE)
    assert len(errors) == 2
    assert not any(PLANTED in e for e in errors)


def test_main_exit_codes(dist: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _page(dist, "index.html", f'<a href="{BASE}pipeline/">p</a>')
    assert check.main([str(dist), BASE]) == 0
    _page(dist, "index.html", '<a href="/pipeline/">p</a>')
    assert check.main([str(dist), BASE]) == 1
    assert PLANTED not in capsys.readouterr().out


def test_empty_or_missing_dist_fails(tmp_path: Path) -> None:
    assert check.main([str(tmp_path), BASE]) == 1
    assert check.main([str(tmp_path / "nope"), BASE]) == 1


def test_base_must_have_both_slashes(dist: Path) -> None:
    with pytest.raises(ValueError):
        check.check_dist(dist, "/open-data-platform")


SITE = "https://filipeoliveira94.github.io"


def test_absolute_links_to_the_site_origin_are_checked(dist: Path) -> None:
    _page(
        dist,
        "index.html",
        f'<link rel="canonical" href="{SITE}{BASE}pipeline/">'
        f'<meta property="og:url" content="{SITE}{BASE}gone/">'
        f'<meta name="description" content="{SITE}{BASE}pipeline/">'
        f'<a href="{SITE}/other-repo/">x</a>',
    )
    errors = check.check_dist(dist, BASE, SITE)
    assert len(errors) == 2, errors
    assert any("meta content -> /open-data-platform/gone/" in e for e in errors)
    assert any("-> /other-repo/ (outside base" in e for e in errors)


def test_trailing_slash_on_a_file_fails(dist: Path) -> None:
    _page(dist, "index.html", f'<img src="{BASE}img.png/">')
    errors = check.check_dist(dist, BASE)
    assert len(errors) == 1 and "img.png/" in errors[0]


def test_css_urls_are_checked(dist: Path) -> None:
    _page(dist, "index.html", "")
    (dist / "_astro" / "a.css").write_text(
        f"@font-face{{src:url({BASE}_astro/font.woff2)}}"
        f"a{{background:url('img.png')}}b{{background:url(\"data:image/png;base64,AA\")}}"
        "c{background:url(../img.png)}",
        encoding="utf-8",
    )
    errors = check.check_dist(dist, BASE)
    assert len(errors) == 2, errors
    assert all(e.startswith("_astro/a.css: url() -> ") for e in errors)
    assert any("_astro/font.woff2" in e for e in errors)
    assert any("/open-data-platform/_astro/img.png" in e for e in errors)


def test_encoded_dot_segments_cannot_leave_dist(dist: Path) -> None:
    (dist.parent / "secret.txt").write_text("x", encoding="utf-8")
    _page(dist, "index.html", f'<a href="{BASE}%2e%2e/secret.txt">x</a>')
    errors = check.check_dist(dist, BASE)
    assert len(errors) == 1 and "resolves outside dist" in errors[0]


def test_404_page_may_name_its_own_url(dist: Path) -> None:
    _page(dist, "404.html", f'<link rel="canonical" href="{SITE}{BASE}404/">')
    assert check.check_dist(dist, BASE, SITE) == []
    (dist / "404.html").unlink()
    _page(dist, "index.html", f'<a href="{BASE}404/">x</a>')
    assert len(check.check_dist(dist, BASE, SITE)) == 1
