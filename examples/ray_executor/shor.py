"""Classical simulation of Shor's period-finding (educational).

This is **not** a quantum computer. It simulates the period-finding
subroutine Shor needs:

1. Build ``f(x) = a^x mod N`` over a counting register of size ``Q ≈ N²``
2. Collapse the work register, IQFT the counting register (NumPy FFT)
3. Continued-fraction post-processing → candidate order ``r``
4. Classical ``gcd(a^{r/2} ± 1, N)`` extraction

Cost is dominated by the ``Q ≈ N²`` modular exponentiations + an FFT of
length ``Q``, so runtime/memory scale as ``O(N²)``. That is why only
small semiprimes are in reach classically — a real quantum device would
scale as ``poly(log N)``.
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass
from fractions import Fraction

import numpy as np


@dataclass(frozen=True)
class ShorResult:
    n: int
    factors: tuple[int, int] | None
    attempts: int
    period: int | None
    base: int | None
    register_size: int
    elapsed_s: float
    ok: bool
    detail: str


def is_prime(n: int) -> bool:
    if n < 2:
        return False
    if n % 2 == 0:
        return n == 2
    factor = 3
    while factor * factor <= n:
        if n % factor == 0:
            return False
        factor += 2
    return True


def random_semiprime(bits: int, rng: random.Random) -> int:
    """Product of two distinct random primes totaling about ``bits`` bits."""
    if bits < 4:
        raise ValueError("need at least 4 bits")
    half = max(bits // 2, 2)

    def rand_prime(width: int) -> int:
        while True:
            candidate = rng.getrandbits(width) | 1 | (1 << (width - 1))
            if is_prime(candidate):
                return candidate

    while True:
        p = rand_prime(half)
        q = rand_prime(bits - half)
        if p == q:
            continue
        n = p * q
        if n.bit_length() == bits:
            return n


def _continued_fraction_candidates(phase: int, q: int, n: int) -> list[int]:
    """Denominators from convergents of phase/Q that might be the order."""
    candidates: list[int] = []
    for value in (Fraction(phase, q), Fraction(phase, q).limit_denominator(n)):
        for convergent in _convergents(value.numerator, value.denominator):
            denom = convergent.denominator
            if 1 < denom <= n and denom not in candidates:
                candidates.append(denom)
    return candidates


def _convergents(num: int, den: int) -> list[Fraction]:
    if den == 0:
        return []
    a_vals: list[int] = []
    while den:
        a_vals.append(num // den)
        num, den = den, num % den
    convergents: list[Fraction] = []
    for index in range(len(a_vals)):
        frac = Fraction(a_vals[index], 1)
        for a in reversed(a_vals[:index]):
            frac = a + Fraction(1, 1) / frac
        convergents.append(frac)
    return convergents


# IQFT state vector needs 16 bytes × Q; keep a safety margin for laptops.
_MAX_REGISTER = 1 << 26  # 64M bins ≈ 1 GiB complex128 + working set


def find_order_simulated(a: int, n: int, rng: random.Random) -> tuple[int | None, int]:
    """Simulate Shor period-finding for ``order of a mod n``.

    Returns ``(r_or_None, Q)`` where ``Q = 2^{2L}`` is the counting-register
    size (``L = bit_length(n)``), capped at ``2^26`` when the ideal
    ``N²`` register would not fit in memory.
    """
    if math.gcd(a, n) != 1:
        raise ValueError("a must be coprime to n")

    l_bits = n.bit_length()
    q_bits = 2 * l_bits  # ideal Q >= N²
    q = 1 << q_bits
    if q > _MAX_REGISTER:
        q = _MAX_REGISTER

    # Measure work register first: pick random x0, residue y = a^{x0} mod N,
    # then collect the counting-register support {x : a^x ≡ y}.
    x0 = rng.randrange(q)
    y = pow(a, x0, n)
    support: list[int] = []
    cur = 1  # a^0
    for index in range(q):
        if cur == y:
            support.append(index)
        cur = (cur * a) % n
    if not support:
        return None, q

    amplitudes = np.zeros(q, dtype=np.complex128)
    amplitudes[np.asarray(support, dtype=np.int64)] = 1.0 / math.sqrt(len(support))
    transformed = np.fft.ifft(amplitudes, norm="ortho")
    probs = np.abs(transformed) ** 2
    total = probs.sum()
    if total <= 0:
        return None, q
    probs /= total

    for phase in rng.choices(range(q), weights=probs, k=12):
        if phase == 0:
            continue
        for candidate in _continued_fraction_candidates(int(phase), q, n):
            for order in _order_candidates(candidate):
                if pow(a, order, n) == 1:
                    return order, q
    return None, q


def _order_candidates(r: int) -> list[int]:
    out = [r]
    # Even multiples sometimes appear; also try factors of r.
    for divisor in range(2, int(math.isqrt(r)) + 1):
        if r % divisor == 0:
            out.append(divisor)
            out.append(r // divisor)
    # Prefer smaller positive candidates first.
    return sorted({value for value in out if value > 0})


def factors_from_order(a: int, r: int, n: int) -> tuple[int, int] | None:
    if r % 2 == 1:
        return None
    if pow(a, r // 2, n) == n - 1:
        return None  # trivial
    g1 = math.gcd(pow(a, r // 2, n) - 1, n)
    g2 = math.gcd(pow(a, r // 2, n) + 1, n)
    for factor in (g1, g2):
        if 1 < factor < n:
            other = n // factor
            if factor * other == n:
                return (min(factor, other), max(factor, other))
    return None


def shor_factor(
    n: int,
    *,
    max_attempts: int = 12,
    seed: int | None = None,
) -> ShorResult:
    """Try to factor ``n`` with simulated Shor. Handles easy classical cases first."""
    import time

    t0 = time.perf_counter()
    rng = random.Random(seed)

    if n < 2:
        return ShorResult(n, None, 0, None, None, 0, 0.0, False, "n < 2")
    if n % 2 == 0:
        return ShorResult(
            n, (2, n // 2), 0, None, None, 0, time.perf_counter() - t0, True, "even"
        )
    root = int(math.isqrt(n))
    if root * root == n:
        return ShorResult(
            n, (root, root), 0, None, None, 0, time.perf_counter() - t0, True, "square"
        )
    if is_prime(n):
        return ShorResult(
            n, None, 0, None, None, 0, time.perf_counter() - t0, False, "prime"
        )

    register_size = 1 << (2 * n.bit_length())
    for attempt in range(1, max_attempts + 1):
        a = rng.randrange(2, n)
        shared = math.gcd(a, n)
        if 1 < shared < n:
            other = n // shared
            return ShorResult(
                n,
                (min(shared, other), max(shared, other)),
                attempt,
                None,
                a,
                register_size,
                time.perf_counter() - t0,
                True,
                "lucky gcd",
            )
        order, q = find_order_simulated(a, n, rng)
        register_size = q
        if order is None:
            continue
        factors = factors_from_order(a, order, n)
        if factors is not None:
            return ShorResult(
                n,
                factors,
                attempt,
                order,
                a,
                q,
                time.perf_counter() - t0,
                True,
                "shor",
            )

    return ShorResult(
        n,
        None,
        max_attempts,
        None,
        None,
        register_size,
        time.perf_counter() - t0,
        False,
        "no factor found",
    )
