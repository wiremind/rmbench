from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta


@dataclass(frozen=True)
class SnapshotSlice:
    sale_date: date
    departure_date: date

    @property
    def day_x(self) -> int:
        return (self.sale_date - self.departure_date).days

    @property
    def event_datetime(self) -> datetime:
        # snapshot stamp is end-of-sale-day (22:59:59 UTC)
        return datetime.combine(self.sale_date, datetime.min.time()) + timedelta(
            hours=22, minutes=59, seconds=59
        )


def iter_snapshot_slices(
    *,
    sale_start: date,
    departure_start: date,
    sale_days: int,
    departure_days: int,
) -> Iterator[SnapshotSlice]:
    for sale_offset in range(sale_days):
        sale_date = sale_start + timedelta(days=sale_offset)
        for departure_offset in range(departure_days):
            departure_date = departure_start + timedelta(days=departure_offset)
            if sale_date > departure_date:
                continue
            yield SnapshotSlice(sale_date=sale_date, departure_date=departure_date)
