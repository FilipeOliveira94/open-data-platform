"""Synthetic personal data that cannot belong to a real person.

- CPF and CNPJ have the right shape and wrong check digits, so a validator
  rejects every generated value and none is a real person's or company's.
- Emails use the ``.example`` top-level domain reserved by RFC 2606.
- Phones use area codes (Códigos Nacionais) ANATEL has not assigned. The
  assigned list is ANATEL open data ``PGCN.csv``, checked on 2026-11-26.
"""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from datetime import date, timedelta

from open_data_platform.simulators.bioguardia.names import (
    COMPANY_NAMES,
    COMPANY_SUFFIXES,
    COMPANY_WORDS,
    GIVEN_NAMES,
    STREET_NAMES,
    STREET_TYPES,
    SURNAMES,
)
from open_data_platform.simulators.bioguardia.rng import Rng

# Codes outside the 67 that ANATEL assigns (PGCN.csv, 2026-11-26).
UNASSIGNED_AREA_CODES: tuple[int, ...] = (
    20, 23, 25, 26, 29, 30, 36, 39, 40, 50, 52,
    56, 57, 58, 59, 60, 70, 72, 76, 78, 80, 90,
)  # fmt: skip
EMAIL_DOMAINS: tuple[str, ...] = (
    "bioguardia.example",
    "correio.example",
    "mail.example",
    "rede.example",
)


def _cpf_digits(base: list[int]) -> tuple[int, int]:
    first = sum(d * w for d, w in zip(base, range(10, 1, -1), strict=True)) * 10 % 11
    first = 0 if first == 10 else first
    second = (
        sum(d * w for d, w in zip([*base, first], range(11, 1, -1), strict=True))
        * 10
        % 11
    )
    return first, 0 if second == 10 else second


def cpf(rng: Rng) -> str:
    """A CPF-shaped value whose second check digit is wrong."""
    base = [rng.below(10) for _ in range(9)]
    first, second = _cpf_digits(base)
    wrong = (second + 1 + rng.below(9)) % 10
    text = "".join(map(str, base)) + f"{first}{wrong}"
    return f"{text[:3]}.{text[3:6]}.{text[6:9]}-{text[9:]}"


def cpf_is_valid(value: str) -> bool:
    """Check digits as Receita Federal computes them."""
    digits = [int(c) for c in value if c.isdigit()]
    if len(digits) != 11 or len(set(digits)) == 1:
        return False
    return _cpf_digits(digits[:9]) == (digits[9], digits[10])


_CNPJ_W1 = (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
_CNPJ_W2 = (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)


def _cnpj_digits(base: list[int]) -> tuple[int, int]:
    rest = sum(d * w for d, w in zip(base, _CNPJ_W1, strict=True)) % 11
    first = 0 if rest < 2 else 11 - rest
    rest = sum(d * w for d, w in zip([*base, first], _CNPJ_W2, strict=True)) % 11
    return first, 0 if rest < 2 else 11 - rest


def cnpj(rng: Rng) -> str:
    """A CNPJ-shaped value (head office ``0001``) whose second check digit is wrong."""
    base = [rng.below(10) for _ in range(8)] + [0, 0, 0, 1]
    first, second = _cnpj_digits(base)
    wrong = (second + 1 + rng.below(9)) % 10
    text = "".join(map(str, base)) + f"{first}{wrong}"
    return f"{text[:2]}.{text[2:5]}.{text[5:8]}/{text[8:12]}-{text[12:]}"


def cnpj_is_valid(value: str) -> bool:
    digits = [int(c) for c in value if c.isdigit()]
    if len(digits) != 14 or len(set(digits)) == 1:
        return False
    return _cnpj_digits(digits[:12]) == (digits[12], digits[13])


def phone(rng: Rng) -> str:
    """A Brazilian mobile shape with an unassigned area code."""
    code = rng.choice(UNASSIGNED_AREA_CODES)
    return f"+55 {code} 9{rng.between(1000, 9999)}-{rng.between(0, 9999):04d}"


def area_code_of(value: str) -> int:
    """The two-digit area code of a ``phone`` value."""
    return int(value.split(" ")[1])


def ascii_slug(text: str) -> str:
    folded = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return "".join(c for c in folded.lower() if c.isalnum())


@dataclass(frozen=True, slots=True)
class Person:
    """The identifiers of one generated person."""

    given: str
    surname: str
    full_name: str
    cpf: str
    email: str
    phone: str
    birth_date: date


def person(rng: Rng, reference: date) -> Person:
    """A full synthetic person, aged 18 to 70 on ``reference``."""
    given = rng.choice(GIVEN_NAMES)
    middle = rng.choice(SURNAMES)
    surname = rng.choice(SURNAMES)
    full = f"{given} {middle} {surname}" if rng.chance(0.6) else f"{given} {surname}"
    email = (
        f"{ascii_slug(given)}.{ascii_slug(surname)}{rng.below(100)}"
        f"@{rng.choice(EMAIL_DOMAINS)}"
    )
    birth = reference - timedelta(days=rng.between(18 * 365, 70 * 365))
    return Person(given, surname, full, cpf(rng), email, phone(rng), birth)


def full_name(rng: Rng) -> str:
    return f"{rng.choice(GIVEN_NAMES)} {rng.choice(SURNAMES)}"


def company_name(rng: Rng) -> str:
    words = (COMPANY_WORDS, COMPANY_NAMES, COMPANY_SUFFIXES)
    return " ".join(rng.choice(part) for part in words)


def street_address(rng: Rng) -> str:
    street = f"{rng.choice(STREET_TYPES)} {rng.choice(STREET_NAMES)}"
    return f"{street}, {rng.between(10, 2999)}"


# Free-text sentences that carry planted personal data. The PII type names
# are what the planted list records for each row.
PII_SENTENCES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Proprietário {name}, CPF {cpf}, foi notificado no local.", ("name", "cpf")),
    ("Contato do denunciante: {phone}.", ("phone",)),
    ("Enviar cópia do relatório para {email}.", ("email",)),
    ("Morador {name} relatou ter visto o grupo na madrugada.", ("name",)),
    (
        "Responsável pela área, {name} ({phone}), acompanhou a vistoria.",
        ("name", "phone"),
    ),
    ("Testemunha {name} pode ser contatada em {email}.", ("name", "email")),
)


def pii_sentence(rng: Rng) -> tuple[str, tuple[str, ...]]:
    """One sentence with planted personal data and the PII types it holds."""
    template, kinds = rng.choice(PII_SENTENCES)
    text = template.format(
        name=full_name(rng),
        cpf=cpf(rng),
        phone=phone(rng),
        email=f"{ascii_slug(rng.choice(GIVEN_NAMES))}{rng.below(1000)}"
        f"@{rng.choice(EMAIL_DOMAINS)}",
    )
    return text, kinds
