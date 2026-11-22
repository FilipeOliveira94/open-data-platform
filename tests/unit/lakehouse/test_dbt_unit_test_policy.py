"""The Gold unit-test policy lists every model, and covered models have tests."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
DBT = ROOT / "dbt"


def _policy() -> dict[str, object]:
    return yaml.safe_load((DBT / "unit_test_policy.yml").read_text(encoding="utf-8"))


def _models() -> set[str]:
    return {path.stem for path in (DBT / "models").rglob("*.sql")}


def _unit_tested_models() -> set[str]:
    tested: set[str] = set()
    for path in (DBT / "models").rglob("unit_tests.yml"):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        tested.update(test["model"] for test in document.get("unit_tests", []))
    return tested


def test_policy_lists_every_model_once() -> None:
    policy = _policy()
    covered = list(policy["covered"])
    exempt = list(policy["exempt"])
    pending = list(policy["pending"])
    listed = covered + exempt + pending
    assert len(listed) == len(set(listed)), "a model is listed twice"
    assert set(listed) == _models()


def test_covered_models_have_unit_tests_and_no_others_do() -> None:
    policy = _policy()
    assert set(policy["covered"]) == _unit_tested_models()


def test_exempt_and_pending_models_give_a_reason() -> None:
    policy = _policy()
    for section in ("exempt", "pending"):
        for model, reason in policy[section].items():
            assert isinstance(reason, str) and reason.strip(), model
