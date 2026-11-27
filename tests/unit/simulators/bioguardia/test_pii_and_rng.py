"""Synthetic personal data can never be a real person's, and streams are stable."""

from __future__ import annotations

import re

from open_data_platform.simulators.bioguardia.pii import (
    EMAIL_DOMAINS,
    UNASSIGNED_AREA_CODES,
    _cnpj_digits,
    _cpf_digits,
    area_code_of,
    cnpj,
    cnpj_is_valid,
    cpf,
    cpf_is_valid,
    person,
    phone,
    pii_sentence,
)
from open_data_platform.simulators.bioguardia.rng import Rng, derive_seed, unit_of


def test_every_generated_cpf_and_cnpj_fails_its_check_digits() -> None:
    rng = Rng(1, "pii-test")
    for _ in range(2000):
        assert not cpf_is_valid(cpf(rng))
        assert not cnpj_is_valid(cnpj(rng))


def test_the_validators_accept_correct_check_digits() -> None:
    base = [5, 2, 9, 9, 8, 2, 2, 4, 7]
    first, second = _cpf_digits(base)
    assert cpf_is_valid("".join(map(str, base)) + f"{first}{second}")
    head = [1, 1, 2, 2, 2, 3, 3, 3, 0, 0, 0, 1]
    first, second = _cnpj_digits(head)
    assert cnpj_is_valid("".join(map(str, head)) + f"{first}{second}")


def test_phone_area_codes_are_outside_the_67_anatel_assigns() -> None:
    assigned = set(range(11, 100)) - set(UNASSIGNED_AREA_CODES)
    assert len(assigned) == 67
    # A few codes every Brazilian state capital uses are assigned.
    assert {11, 21, 27, 31, 41, 47, 51, 61, 71, 81, 91}.issubset(assigned)
    rng = Rng(2, "phones")
    for _ in range(500):
        value = phone(rng)
        assert re.fullmatch(r"\+55 \d{2} 9\d{4}-\d{4}", value)
        assert area_code_of(value) in UNASSIGNED_AREA_CODES


def test_emails_use_the_reserved_example_domain() -> None:
    from datetime import date

    rng = Rng(3, "people")
    for _ in range(300):
        someone = person(rng, date(2026, 1, 1))
        assert someone.email.rsplit("@", 1)[1] in EMAIL_DOMAINS
        assert someone.email.endswith(".example")
    assert all(domain.endswith(".example") for domain in EMAIL_DOMAINS)


def test_pii_sentences_report_the_types_they_hold() -> None:
    rng = Rng(4, "sentences")
    for _ in range(200):
        text, kinds = pii_sentence(rng)
        if "cpf" in kinds:
            assert re.search(r"\d{3}\.\d{3}\.\d{3}-\d{2}", text)
        if "email" in kinds:
            assert ".example" in text
        if "phone" in kinds:
            assert "+55 " in text


def test_streams_depend_only_on_their_parts() -> None:
    first = [Rng(7, "a", 1).random() for _ in range(3)]
    again = [Rng(7, "a", 1).random() for _ in range(3)]
    other = [Rng(7, "a", 2).random() for _ in range(3)]
    assert first == again
    assert first != other
    assert derive_seed("x", 1) == derive_seed("x", 1)
    assert 0.0 <= unit_of("x", 1) < 1.0
