"""Deterministic randomness for the BioGuardia generators.

Every stream is seeded from a stable hash of its name parts, so a value depends
only on (seed, entity, date, ...) and never on generation order. Only
``random.Random.random()`` is used: Python keeps that sequence stable across
versions for an integer seed. Distributions are implemented here on top of it.
"""

from __future__ import annotations

import hashlib
import math
import random
from bisect import bisect_right
from collections.abc import Sequence

_MASK64 = (1 << 64) - 1
_INV53 = 1.0 / (1 << 53)


def derive_seed(*parts: object) -> int:
    """A 64-bit seed from ``parts``; the same parts give the same seed anywhere."""
    text = "\x1f".join(str(part) for part in parts).encode("utf-8")
    return int.from_bytes(hashlib.blake2b(text, digest_size=8).digest(), "big")


def splitmix64(value: int) -> int:
    """One step of SplitMix64: a fast, stateless 64-bit mixer."""
    z = (value + 0x9E3779B97F4A7C15) & _MASK64
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & _MASK64
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & _MASK64
    return z ^ (z >> 31)


def unit(key: int) -> float:
    """Uniform float in [0, 1) from an integer key, without state."""
    return (splitmix64(key) >> 11) * _INV53


def unit_of(*parts: object) -> float:
    """Uniform float in [0, 1) from name parts (slower than ``unit``)."""
    return unit(derive_seed(*parts))


class Rng:
    """A seeded stream restricted to ``random()``, with the distributions we need."""

    __slots__ = ("random",)

    def __init__(self, *parts: object) -> None:
        source = random.Random()
        source.seed(derive_seed(*parts))
        self.random = source.random

    def uniform(self, low: float, high: float) -> float:
        return low + (high - low) * self.random()

    def below(self, upper: int) -> int:
        """Integer in [0, upper)."""
        value = int(self.random() * upper)
        return value if value < upper else upper - 1

    def between(self, low: int, high: int) -> int:
        """Integer in [low, high], both ends included."""
        return low + self.below(high - low + 1)

    def chance(self, probability: float) -> bool:
        return self.random() < probability

    def choice[T](self, items: Sequence[T]) -> T:
        return items[self.below(len(items))]

    def weighted(self, cumulative: Sequence[float]) -> int:
        """Index drawn from cumulative weights (last element is the total)."""
        index = bisect_right(cumulative, self.random() * cumulative[-1])
        return index if index < len(cumulative) else len(cumulative) - 1

    def gauss(self, mean: float, sigma: float) -> float:
        """Normal draw by Box-Muller, two uniforms per call."""
        u1 = 1.0 - self.random()
        u2 = self.random()
        return mean + sigma * math.sqrt(-2.0 * math.log(u1)) * math.cos(
            2.0 * math.pi * u2
        )

    def lognormal(self, mu: float, sigma: float) -> float:
        return math.exp(self.gauss(mu, sigma))

    def exponential(self, mean: float) -> float:
        return -mean * math.log(1.0 - self.random())

    def count(self, mean: float) -> int:
        """A non-negative count with the given mean (floor plus a Bernoulli rest)."""
        if mean <= 0:
            return 0
        whole = int(mean)
        return whole + (1 if self.random() < mean - whole else 0)

    def poisson(self, mean: float) -> int:
        """Poisson draw; inversion for small means, normal approximation above 30."""
        if mean <= 0:
            return 0
        if mean > 30:
            return max(0, round(self.gauss(mean, math.sqrt(mean))))
        limit = math.exp(-mean)
        k = 0
        product = self.random()
        while product > limit:
            k += 1
            product *= self.random()
        return k


def cumulative(weights: Sequence[float]) -> list[float]:
    """Running sums of ``weights`` for ``Rng.weighted``."""
    total = 0.0
    out: list[float] = []
    for weight in weights:
        total += weight
        out.append(total)
    return out


def zipf_weights(size: int, exponent: float) -> list[float]:
    """Weights proportional to 1 / rank**exponent, rank starting at 1."""
    return [1.0 / (rank**exponent) for rank in range(1, size + 1)]
