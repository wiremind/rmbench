from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Bundle:
    tables: dict[str, dict[str, Any]]
    cross_table: dict[str, Any]
    sf_anchor: dict[str, str]
    sf_catalog: dict[str, Any]
    cardinality_anchors: dict[str, Any]
    entity_cardinality: dict[str, Any]

    def sf_window(self, sf: int) -> dict[str, Any]:
        sale_days, departure_days = self.sf_shape(sf)
        offset_days = self.sf_booking_offset(sf)
        # a sale window past the booking offset would silently drop snapshot
        # slices (sale_date > departure_date) and undershoot the SF
        if sale_days > offset_days + 1:
            raise ValueError(
                f"SF={sf} has sale_days={sale_days}, exceeding the "
                f"{offset_days}-day booking offset for that scale."
            )
        anchor_sale_start = date.fromisoformat(self.sf_anchor["sale_start"])
        return {
            "sale_start": self.sf_anchor["sale_start"],
            "sale_days": sale_days,
            # anchor plus this scale's own booking offset: a wide sale window
            # needs a proportionally longer horizon ahead of it
            "departure_start": (anchor_sale_start + timedelta(days=offset_days)).isoformat(),
            "departure_days": departure_days,
        }

    def sf_booking_offset(self, sf: int) -> int:
        """Days between sale_start and departure_start for ``sf``.

        Sets the window's day_x range, and widens with scale.
        """
        entry = self.sf_catalog.get(str(sf))
        if entry is not None:
            return int(entry["booking_offset_days"])
        # No sf_catalog entry: take the nearest entry's offset, then floor it at
        # sale_days - 1 so the sale window still fits ahead of the first departure.
        offsets = sorted(
            (float(key), int(value["booking_offset_days"]))
            for key, value in self.sf_catalog.items()
        )
        target = float(max(sf, 1))
        best = min(offsets, key=lambda point: abs(math.log(point[0]) - math.log(target)))
        return max(best[1], self.sf_shape(sf)[0] - 1)

    def sf_shape(self, sf: int) -> tuple[int, int]:
        """(sale_days, departure_days), whose product is ``sf``.

        From sf_catalog when it lists this scale factor, otherwise departure_days is
        interpolated from the nearest entries and snapped to a divisor of sf. od and
        leg counts grow with departure_days, so the shape matters, not just sf.
        """
        entry = self.sf_catalog.get(str(sf))
        if entry is not None:
            return int(entry["sale_days"]), int(entry["departure_days"])
        return _interpolated_shape(self.sf_catalog, sf)

    @property
    def base_table(self) -> str:
        return self.cross_table["base_table"]

    def table(self, name: str) -> dict[str, Any]:
        return self.tables[name]

    def per_table_row_count_ratio(self, table_name: str) -> float:
        if table_name == self.base_table:
            return 1.0
        return self.cross_table["row_count_ratios"][table_name]


def _interpolated_shape(catalog: dict[str, Any], sf: int) -> tuple[int, int]:
    """departure_days interpolated log-log from the catalog, sale_days = sf / dep."""
    points = sorted(
        (float(key), int(value["departure_days"])) for key, value in catalog.items()
    )
    target = float(max(sf, 1))
    if target <= points[0][0]:
        departure_days = points[0][1]
    elif target >= points[-1][0]:
        departure_days = points[-1][1]
    else:
        for (lo_sf, lo_dep), (hi_sf, hi_dep) in zip(points, points[1:]):
            if lo_sf <= target <= hi_sf:
                break
        span = math.log(hi_sf) - math.log(lo_sf)
        t = (math.log(target) - math.log(lo_sf)) / span if span > 0 else 0.0
        departure_days = int(round(lo_dep * (hi_dep / lo_dep) ** t)) if lo_dep > 0 else hi_dep
    # snap onto a divisor of sf so sale_days * departure_days == sf exactly,
    # otherwise the slice count misses the requested scale
    target_sf = max(sf, 1)
    divisors = {d for d in range(1, int(target_sf**0.5) + 1) if target_sf % d == 0}
    divisors |= {target_sf // d for d in divisors}
    departure_days = min(
        sorted(divisors),
        key=lambda d: abs(math.log(d) - math.log(max(departure_days, 1))),
    )
    return target_sf // departure_days, departure_days


def load_bundle(path: Path | str) -> Bundle:
    raw = json.loads(Path(path).read_text())
    return Bundle(
        tables=raw["tables"],
        cross_table=raw["cross_table"],
        sf_anchor=raw.get("sf_anchor"),
        sf_catalog=raw.get("sf_catalog"),
        cardinality_anchors=raw.get("cardinality_anchors"),
        entity_cardinality=raw.get("entity_cardinality"),
    )
