"""Checks on the GitHub Pages hosting setup (P9-05).

The deploy publishes to a public URL. These tests fail when the Astro URL drifts
from the README link, when a pull request or another branch could deploy, when
the deploy job loses its permission scoping, or when a path filter could skip a
redeploy.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
ASTRO_CONFIG = ROOT / "site" / "astro.config.mjs"
PAGES_WORKFLOW = ROOT / ".github" / "workflows" / "pages.yml"
DOCS_PAGES = ROOT / "site" / "src" / "content" / "docs"
PAGES_URL = "https://filipeoliveira94.github.io/open-data-platform/"
REPO_URL = "https://github.com/FilipeOliveira94/open-data-platform/"
MAIN_ONLY = "github.event_name != 'pull_request' && github.ref == 'refs/heads/main'"


def _astro_setting(name: str) -> str:
    match = re.search(
        rf"^\t{name}: '([^']+)',$", ASTRO_CONFIG.read_text(encoding="utf-8"), re.M
    )
    assert match, f"astro.config.mjs sets no top-level {name}"
    return match.group(1)


def _workflow() -> dict:
    return yaml.safe_load(PAGES_WORKFLOW.read_text(encoding="utf-8"))


def _steps(job: dict) -> list[dict]:
    return job["steps"]


def _step(job: dict, uses: str) -> dict:
    found = [s for s in _steps(job) if s.get("uses", "").startswith(f"{uses}@")]
    assert len(found) == 1, f"expected one {uses} step"
    return found[0]


def test_astro_site_and_base_match_the_pages_url() -> None:
    site, base = _astro_setting("site"), _astro_setting("base")
    assert not site.endswith("/") and base.startswith("/") and not base.endswith("/")
    assert f"{site}{base}/" == PAGES_URL


def test_triggers_have_no_path_filter() -> None:
    workflow = _workflow()
    # PyYAML reads the bare key `on` as True (YAML 1.1).
    triggers = workflow.get("on", workflow.get(True))
    assert set(triggers) == {"push", "pull_request", "workflow_dispatch"}
    assert triggers["push"] == {"branches": ["main"]}
    for event in triggers.values():
        assert not ({"paths", "paths-ignore"} & set(event or {})), (
            "path filter on a trigger"
        )


def test_permissions_are_scoped_to_the_deploy_job() -> None:
    workflow = _workflow()
    assert workflow["permissions"] == {"contents": "read"}
    jobs = workflow["jobs"]
    assert set(jobs) == {"build", "deploy"}
    assert "permissions" not in jobs["build"]
    assert jobs["deploy"]["permissions"] == {"pages": "write", "id-token": "write"}


def test_only_main_deploys() -> None:
    jobs = _workflow()["jobs"]
    deploy = jobs["deploy"]
    assert deploy["needs"] == "build"
    assert deploy["if"] == MAIN_ONLY
    assert deploy["environment"]["name"] == "github-pages"
    assert deploy["concurrency"] == {"group": "pages", "cancel-in-progress": False}
    _step(deploy, "actions/deploy-pages")
    build = jobs["build"]
    for uses in ("actions/configure-pages", "actions/upload-pages-artifact"):
        assert _step(build, uses)["if"] == MAIN_ONLY, f"{uses} runs outside main"


def test_build_checks_links_before_upload() -> None:
    build = _workflow()["jobs"]["build"]
    assert "github.ref" in build["concurrency"]["group"]
    assert "pages" != build["concurrency"]["group"]
    steps = _steps(build)
    runs = [s.get("run", "") for s in steps]
    base = _astro_setting("base") + "/"
    check = next(i for i, r in enumerate(runs) if "scripts/check_site_links.py" in r)
    assert f"site/dist {base}" in runs[check]
    build_step = next(i for i, r in enumerate(runs) if "npm run build" in r)
    upload = steps.index(_step(build, "actions/upload-pages-artifact"))
    assert build_step < check < upload
    assert steps[upload]["with"] == {"path": "site/dist"}


def test_workflow_uses_no_secrets() -> None:
    assert "secrets." not in PAGES_WORKFLOW.read_text(encoding="utf-8")


def test_site_pages_name_the_pages_workflow() -> None:
    link = f"]({REPO_URL}blob/main/.github/workflows/pages.yml)"
    assert link in (DOCS_PAGES / "operations.md").read_text(encoding="utf-8")
    assert link in (DOCS_PAGES / "reference.md").read_text(encoding="utf-8")
