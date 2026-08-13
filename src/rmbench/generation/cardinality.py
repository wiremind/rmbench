"""Two-anchor cardinality scaling.

Each calibrated distinct count goes out as `lo` at SF1 and `hi` at the largest
reference window, interpolated in log-SF space and extrapolated beyond `hi`.
"""

from __future__ import annotations

import math
from typing import Any

# fields carrying a two-anchor pair, by bundle profile section
_SCALED_FIELDS = {
    "category_distributions": ("distinct_count", "tail_distinct_count"),
    "metric_profiles": ("distinct_count",),
    "numeric_profiles": ("distinct_count",),
}


def anchor_pair(value: Any) -> tuple[float, float]:
    return float(value["lo"]), float(value["hi"])


def _interpolate(value: Any, *, sf: float, sf_hi: float) -> float:
    """Geometric interpolation between the two anchors, parameterised by log(sf).

    ``t = log(sf) / log(sf_hi)``, so t=0 returns ``lo`` and t=1 returns ``hi``.
    Log space is required: most of the growth occurs within the first decade.
    Past the high anchor it extrapolates instead of flattening, so a column whose
    real distinct count levels off is overestimated at very large scale factors.
    """
    lo, hi = anchor_pair(value)
    if lo <= 0.0 or hi <= 0.0:
        # an anchor with no value carries no curve
        return max(lo, hi, 0.0)
    if sf <= 1.0 or sf_hi <= 1.0:
        return lo
    return lo * (hi / lo) ** (math.log(sf) / math.log(sf_hi))


def scale_cardinality(value: Any, *, sf: float, sf_hi: float) -> int:
    """Interpolated distinct count, at least 1 for a column that has any values."""
    count = _interpolate(value, sf=sf, sf_hi=sf_hi)
    return max(1, round(count)) if count > 0 else 0


def scale_rate(value: Any, *, sf: float, sf_hi: float) -> float:
    """Interpolated rate, clamped to [0, 1]."""
    return min(max(_interpolate(value, sf=sf, sf_hi=sf_hi), 0.0), 1.0)


def departure_linear_cardinality(
    value: Any,
    *,
    departure_days: int,
    departure_days_hi: int,
) -> int:
    """Entity ids that grow linearly in departure_days rather than in SF."""
    _, hi = anchor_pair(value)
    rate = hi / float(departure_days_hi)
    return max(1, int(round(rate * max(int(departure_days), 0))))


def resolve_bundle_cardinality(
    *,
    tables: dict[str, dict[str, Any]],
    anchors: dict[str, Any],
    sf: float,
) -> dict[str, int]:
    """Replace every two-anchor pair in ``tables`` with its value at ``sf``."""
    sf_hi = float(anchors["sf_hi"])
    resolved: dict[str, int] = {}
    for table_name, table in tables.items():
        # bool rates carry the same two anchors, interpolated as a rate not a count
        for profile in table["bool_profiles"].values():
            profile["true_rate"] = scale_rate(profile["true_rate"], sf=sf, sf_hi=sf_hi)
        for section, fields in _SCALED_FIELDS.items():
            for column, profile in table[section].items():
                for field in fields:
                    if field not in profile:
                        continue
                    count = scale_cardinality(profile[field], sf=sf, sf_hi=sf_hi)
                    profile[field] = count
                    resolved[f"{table_name}.{section}.{column}.{field}"] = count
    return resolved


def resolve_entity_pool_sizes(
    *,
    entity_cardinality: dict[str, Any],
    anchors: dict[str, Any],
    sf: float,
    departure_days: int,
    departure_linear: tuple[str, ...],
) -> dict[str, int]:
    """Pool size per entity, from the published anchors rather than hardcoded constants."""
    sf_hi = float(anchors["sf_hi"])
    departure_days_hi = int(anchors["departure_days_hi"])
    sizes: dict[str, int] = {}
    for entity, value in entity_cardinality.items():
        if entity in departure_linear:
            sizes[entity] = departure_linear_cardinality(
                value,
                departure_days=departure_days,
                departure_days_hi=departure_days_hi,
            )
        else:
            sizes[entity] = scale_cardinality(value, sf=sf, sf_hi=sf_hi)
    return sizes
