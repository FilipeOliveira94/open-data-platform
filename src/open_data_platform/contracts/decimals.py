"""Exact decimal values: JSON read and write, and contract bounds.

``json_loads`` reads a JSON number with a fraction or an exponent as a
``Decimal``, never as a binary float. ``json_dumps`` writes a ``Decimal`` as
its exact text, so ``10.50`` stays ``10.50``. ``fits`` is the ``decimal``
contract rule: at most ``scale`` digits after the point, as written, and at
most ``precision - scale`` digits before it.
"""

from __future__ import annotations

import json
from decimal import Decimal


def json_loads(text: str | bytes) -> object:
    """Parse JSON. A number with a fraction or an exponent becomes a ``Decimal``."""
    return json.loads(text, parse_float=Decimal)


def json_dumps(value: object) -> str:
    """Compact JSON, as ``json.dumps`` with ``separators=(",", ":")``.

    A ``Decimal`` is written as its exact text. Object keys must be strings.
    NaN and infinity raise ``ValueError``; any other value JSON cannot hold
    raises ``TypeError``.
    """
    if isinstance(value, Decimal):
        if not value.is_finite():
            msg = "a JSON number must be finite"
            raise ValueError(msg)
        return str(value)
    if isinstance(value, dict):
        members: list[str] = []
        for key, item in value.items():
            if not isinstance(key, str):
                msg = "JSON object keys must be strings"
                raise TypeError(msg)
            members.append(f"{_scalar(key)}:{json_dumps(item)}")
        return "{" + ",".join(members) + "}"
    if isinstance(value, list | tuple):
        return "[" + ",".join(json_dumps(item) for item in value) + "]"
    return _scalar(value)


def fits(value: Decimal, precision: int, scale: int) -> bool:
    """Whether a value is finite and fits ``DECIMAL(precision, scale)`` as written.

    Digits after the point count as written, so ``58.900`` has three. A value
    that needs rounding to fit does not fit.
    """
    if not value.is_finite():
        return False
    exponent = value.as_tuple().exponent
    if not isinstance(exponent, int):
        return False
    if (-exponent if exponent < 0 else 0) > scale:
        return False
    if value.is_zero():
        return True
    return max(value.adjusted() + 1, 0) <= precision - scale


def plain_text(value: Decimal) -> str:
    """Fixed-point text with no exponent: ``Decimal("1E+2")`` is ``100``.

    Call it on a value that ``fits`` a contract; the text of an unbounded
    exponent would be unbounded too.
    """
    return format(value, "f")


def _scalar(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False)
