from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import numpy as np

from rmbench.generation.name_mapping import (
    NameMapping,
    synth_tail_name,
)

# Upper bound on a metric's distinct values, to keep grid memory bounded
# at the largest scale factors.
MAX_GRID_SIZE = 65536
_GRID_CACHE_KEY = "_grid"


@dataclass
class CategoricalSampler:
    column: str
    names: np.ndarray
    probabilities: np.ndarray

    def sample(self, size: int, rng: np.random.Generator) -> np.ndarray:
        return rng.choice(self.names, size=size, p=self.probabilities)


def build_categorical_sampler(
    *,
    column: str,
    category_distribution: dict[str, Any],
    name_mapping: NameMapping,
) -> CategoricalSampler:
    tail_count = int(category_distribution["tail_distinct_count"])
    tail_share_sum = float(category_distribution["tail_share_sum"])
    tail_zipf_exponent = category_distribution.get("tail_zipf_exponent")

    top_names: list[str] = []
    top_probs: list[float] = []
    for entry in category_distribution["top_values"]:
        top_names.append(name_mapping.name_for(entry["category_id"]))
        top_probs.append(float(entry["share"]))

    tail_names: list[str] = []
    tail_probs: list[float] = []
    if tail_count > 0 and tail_share_sum > 0:
        if tail_zipf_exponent is not None:
            # calibrated skew: weight rank r by r^-a, normalized to the tail mass
            ranks = np.arange(1, tail_count + 1, dtype=np.float64)
            weights = ranks ** -float(tail_zipf_exponent)
            weights *= tail_share_sum / weights.sum()
        else:
            weights = np.full(tail_count, tail_share_sum / tail_count)
        for ordinal in range(1, tail_count + 1):
            tail_names.append(synth_tail_name(column, ordinal))
            tail_probs.append(float(weights[ordinal - 1]))

    all_names = np.array(top_names + tail_names, dtype=object)
    all_probs = np.array(top_probs + tail_probs, dtype=np.float64)
    return CategoricalSampler(
        column=column, names=all_names, probabilities=all_probs / all_probs.sum()
    )


def _greedy_share_partition(
    *,
    item_sampler: CategoricalSampler,
    group_sampler: CategoricalSampler,
) -> dict[str, list[str]]:
    """Assign every item to exactly one group, share-weighted.

    Items are placed heaviest-first into the group with the largest remaining
    share deficit, which keeps both marginals approximately calibrated.
    """
    deficits = {
        str(name): float(p)
        for name, p in zip(group_sampler.names, group_sampler.probabilities, strict=True)
    }
    groups: dict[str, list[str]] = {group: [] for group in deficits}
    for i in np.argsort(item_sampler.probabilities)[::-1]:
        item = str(item_sampler.names[i])
        group = max(deficits, key=deficits.get)
        groups[group].append(item)
        deficits[group] -= float(item_sampler.probabilities[i])
    return groups


def build_family_of_bucket(
    *,
    bucket_sampler: CategoricalSampler,
    family_sampler: CategoricalSampler,
) -> dict[str, str]:
    """Bucket -> family, each bucket belonging to exactly one family."""
    groups = _greedy_share_partition(item_sampler=bucket_sampler, group_sampler=family_sampler)
    return {bucket: family for family, buckets in groups.items() for bucket in buckets}


def build_route_market_subsets(
    *,
    route_sampler: CategoricalSampler,
    market_sampler: CategoricalSampler,
) -> dict[str, tuple[str, ...]]:
    """Partition the market pool among routes, one route per market.

    A market is an origin/destination pair and so belongs to a route; a service
    running that route sees only that route's markets.
    """
    groups = _greedy_share_partition(item_sampler=market_sampler, group_sampler=route_sampler)
    if not groups:
        return {}
    # a route with no market would leave its ods unassignable
    fallback = str(market_sampler.names[int(np.argmax(market_sampler.probabilities))])
    return {
        route: tuple(markets) if markets else (fallback,)
        for route, markets in groups.items()
    }


def sample_markets_within_routes(
    *,
    market_sampler: CategoricalSampler,
    route_market_subsets: dict[str, tuple[str, ...]],
    parent_routes: list[str],
    rng: np.random.Generator,
) -> list[str]:
    """One market per od, drawn from that od's parent route's market subset."""
    weight_of = {
        str(name): float(p)
        for name, p in zip(market_sampler.names, market_sampler.probabilities)
    }
    cache: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    out: list[str] = []
    for route in parent_routes:
        choice = cache.get(route)
        if choice is None:
            members = route_market_subsets.get(route) or tuple(
                str(n) for n in market_sampler.names[:1]
            )
            weights = np.array([weight_of.get(m, 0.0) for m in members], dtype=np.float64)
            if weights.sum() <= 0:
                weights = np.ones(len(members), dtype=np.float64)
            choice = (np.array(members, dtype=object), weights / weights.sum())
            cache[route] = choice
        names, probs = choice
        out.append(str(rng.choice(names, p=probs)))
    return out


def build_cabin_subset_fn(
    *,
    cabin_sampler: CategoricalSampler,
    size_histogram: dict[str, float],
) -> Callable[[str], tuple[str, ...]]:
    """Pick a stable set of cabins for a market.

    Seeded from the market name, so every pool, slice and table sees the same set.
    How many cabins comes from ``size_histogram``; which ones is weighted by each
    cabin's overall share.
    """
    sizes = np.array([int(k) for k in size_histogram], dtype=np.int64)
    size_probs = np.array([float(v) for v in size_histogram.values()], dtype=np.float64)
    size_probs = size_probs / size_probs.sum()
    names = cabin_sampler.names
    probs = cabin_sampler.probabilities
    cache: dict[str, tuple[str, ...]] = {}

    def subset(market: str) -> tuple[str, ...]:
        got = cache.get(market)
        if got is not None:
            return got
        seed = int.from_bytes(hashlib.blake2b(market.encode(), digest_size=8).digest(), "big")
        rng = np.random.default_rng(seed)
        k = min(int(rng.choice(sizes, p=size_probs)), len(names))
        members = tuple(sorted(str(v) for v in rng.choice(names, size=k, replace=False, p=probs)))
        cache[market] = members
        return members

    return subset


def get_metric_grid(profile: dict[str, Any], rng: np.random.Generator) -> np.ndarray | None:
    """Value grid sized to the profile's calibrated distinct_count.

    Built once per profile and cached on the profile dict itself,
    so every slice snaps onto the same grid.
    """
    if _GRID_CACHE_KEY in profile:
        return profile[_GRID_CACHE_KEY]
    grid = _build_metric_grid(profile, rng)
    profile[_GRID_CACHE_KEY] = grid
    return grid


def _build_metric_grid(profile: dict[str, Any], rng: np.random.Generator) -> np.ndarray | None:
    distinct = profile.get("distinct_count")
    family = profile.get("family")
    # numeric profiles carry only a distinct_count: no family, no bounds, no grid
    if not distinct or family not in ("lognormal", "bounded"):
        return None
    k = min(int(distinct), MAX_GRID_SIZE)
    bounds = profile["bounds"]
    lower = bounds.get("lower")
    upper = bounds.get("upper")
    if family == "lognormal":
        log_stddev = (profile.get("shape") or {}).get("log_stddev")
        scale_mean = resolve_scale_mean(scale_decade=profile.get("scale_decade"))
        if log_stddev is not None and log_stddev > 0:
            values = rng.lognormal(mean=float(np.log(scale_mean)), sigma=float(log_stddev), size=k)
        else:
            values = np.full(k, scale_mean)
    else:
        lo = 0.0 if lower is None else float(lower)
        hi = 1.0 if upper is None else float(upper)
        if hi <= lo:
            hi = lo + 1.0
        # cardinality-only: evenly-spaced values across the publisher-chosen
        # bounds; location and spread are not modelled
        values = np.linspace(lo, hi, k)
    if lower is not None:
        values = np.maximum(values, float(lower))
    if upper is not None:
        values = np.minimum(values, float(upper))
    grid = np.unique(np.round(values, 2))
    return grid if grid.size else None


def snap_to_grid(values: np.ndarray, grid: np.ndarray | None) -> np.ndarray:
    """Nearest-neighbor snap of non-zero values onto the grid.

    Zeros are left alone: a zero encodes "not populated" (snapping it would
    corrupt the calibrated non_zero_rate).
    """
    if grid is None or grid.size == 0:
        return values
    out = np.asarray(values, dtype=np.float64).copy()
    mask = out != 0
    if not mask.any():
        return out
    v = out[mask]
    if grid.size == 1:
        out[mask] = grid[0]
        return out
    idx = np.clip(np.searchsorted(grid, v), 1, grid.size - 1)
    left = grid[idx - 1]
    right = grid[idx]
    out[mask] = np.where(v - left <= right - v, left, right)
    return out


def resolve_scale_mean(*, scale_decade: int | None) -> float:
    if scale_decade is not None:
        return float(10 ** int(scale_decade))
    # only reached by profiles whose every value is masked out (100% null, or a
    # non_zero_rate of 0) so the magnitude is never observable
    return 1.0


def sample_lognormal_metric(
    *,
    n: int,
    log_stddev: float | None,
    non_zero_rate: float | None,
    null_rate: float | None,
    bounds_lower: float | None,
    bounds_upper: float | None,
    scale_mean: float,
    rng: np.random.Generator,
) -> tuple[np.ndarray, np.ndarray]:
    nz = 0.0 if non_zero_rate is None else float(non_zero_rate)
    nl = 0.0 if null_rate is None else float(null_rate)

    null_mask = rng.uniform(size=n) < nl
    non_null = ~null_mask
    populate_mask = non_null & (rng.uniform(size=n) < nz)

    values = np.zeros(n, dtype=np.float64)
    populate_count = int(populate_mask.sum())
    if populate_count > 0:
        if log_stddev is not None and log_stddev > 0:
            mu = float(np.log(scale_mean))
            values[populate_mask] = rng.lognormal(mean=mu, sigma=float(log_stddev), size=populate_count)
        else:
            # constant column (stddev 0 / not computable): honour non_zero_rate
            # at the calibrated scale instead of collapsing to 0
            values[populate_mask] = scale_mean
    if bounds_lower is not None:
        values = np.maximum(values, float(bounds_lower))
    if bounds_upper is not None:
        values = np.minimum(values, float(bounds_upper))
    return values, null_mask


def sample_bounded_metric(
    *,
    n: int,
    non_zero_rate: float | None,
    null_rate: float | None,
    bounds_lower: float | None,
    bounds_upper: float | None,
    rng: np.random.Generator,
) -> tuple[np.ndarray, np.ndarray]:
    lower = 0.0 if bounds_lower is None else float(bounds_lower)
    upper = 1.0 if bounds_upper is None else float(bounds_upper)
    if upper <= lower:
        upper = lower + 1.0

    nz = 0.0 if non_zero_rate is None else float(non_zero_rate)
    nl = 0.0 if null_rate is None else float(null_rate)

    null_mask = rng.uniform(size=n) < nl
    non_null = ~null_mask
    populate_mask = non_null & (rng.uniform(size=n) < nz)

    # cardinality-only: values spread uniformly across the publisher-chosen
    # bounds so the grid snap covers the full distinct set
    values = np.zeros(n, dtype=np.float64)
    populate_count = int(populate_mask.sum())
    if populate_count > 0:
        values[populate_mask] = rng.uniform(lower, upper, size=populate_count)
    values = np.clip(values, lower, upper)
    return values, null_mask
