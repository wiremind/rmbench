from __future__ import annotations

import math
import statistics
import time
from collections.abc import Callable, Hashable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

PROBE_POLL_INTERVAL_SECONDS = 0.1
PROBE_TIMEOUT_SECONDS = 300.0


@dataclass
class TableTiming:
    submit_monotonic: float
    task_done_monotonic: float
    visible_wall: datetime | None = None
    visible_monotonic: float | None = None
    physical_done_wall: datetime | None = None
    physical_done_monotonic: float | None = None
    async_task_ids: tuple[str, ...] = ()


def monotonic_duration_ms(start_monotonic: float, end_monotonic: float) -> int:
    return int(round((end_monotonic - start_monotonic) * 1000))


def record_milestone[Observed](
    *,
    table_timings: Mapping[str, TableTiming],
    unresolved: set[str],
    observed_values: dict[str, Observed],
    milestone: str,
    is_complete: Callable[[str, Observed], bool],
    validate: Callable[[str, Observed], None] | None = None,
) -> dict[str, Observed]:
    """Stamp ``milestone`` on every table that has reached it, at one shared instant."""
    observed_wall = datetime.now(UTC)
    observed_monotonic = time.monotonic()
    # a table whose statement has not been submitted yet has no timing to stamp
    for table_name in tuple(unresolved & table_timings.keys()):
        observed = observed_values[table_name]
        if validate is not None:
            validate(table_name, observed)
        if is_complete(table_name, observed):
            setattr(table_timings[table_name], f"{milestone}_wall", observed_wall)
            setattr(table_timings[table_name], f"{milestone}_monotonic", observed_monotonic)
            unresolved.remove(table_name)
    return observed_values


def observe_count_milestone[ClientT](
    client: ClientT,
    *,
    probe: Callable[[ClientT], dict[str, int]],
    table_timings: Mapping[str, TableTiming],
    unresolved: set[str],
    expected_counts: dict[str, int],
    milestone: str,
) -> dict[str, int]:
    return record_milestone(
        table_timings=table_timings,
        unresolved=unresolved,
        observed_values=probe(client),
        milestone=milestone,
        is_complete=lambda table_name, count: count == expected_counts[table_name],
    )


def aggregate_milestone(
    table_timings: Mapping[str, TableTiming], milestone: str
) -> tuple[datetime, float]:
    """The run reaches a milestone when its slowest table does."""
    slowest = max(
        table_timings,
        key=lambda name: float(getattr(table_timings[name], f"{milestone}_monotonic")),
    )
    return getattr(table_timings[slowest], f"{milestone}_wall"), float(
        getattr(table_timings[slowest], f"{milestone}_monotonic")
    )


def wait_for_completion[Visible, Physical](
    *,
    unresolved_visible: set[str],
    unresolved_physical: set[str],
    observed_visible: dict[str, Visible],
    observed_physical: dict[str, Physical],
    observe_visible: Callable[[], dict[str, Visible]],
    observe_physical: Callable[[], dict[str, Physical]],
    timeout_message: Callable[[dict[str, Visible], dict[str, Physical]], str],
    observe_progress: Callable[[], Hashable | None] | None = None,
) -> tuple[dict[str, Visible], dict[str, Physical]]:
    """Poll until every table has reached both milestones.

    The deadline resets whenever anything moves, so a slow-but-progressing
    operation runs to completion while a genuinely stuck one still fails.
    """
    deadline = time.monotonic() + PROBE_TIMEOUT_SECONDS
    next_poll = time.monotonic()
    last_observed_state: object = None
    while unresolved_visible or unresolved_physical:
        if unresolved_visible:
            observed_visible = observe_visible()
        if unresolved_physical:
            observed_physical = observe_physical()
        if not unresolved_visible and not unresolved_physical:
            break

        observed_state = (
            observed_visible,
            observed_physical,
            observe_progress() if observe_progress is not None else None,
        )
        if observed_state != last_observed_state:
            last_observed_state = observed_state
            deadline = time.monotonic() + PROBE_TIMEOUT_SECONDS

        now = time.monotonic()
        if now >= deadline:
            raise TimeoutError(timeout_message(observed_visible, observed_physical))
        next_poll += PROBE_POLL_INTERVAL_SECONDS
        time.sleep(min(max(0.0, next_poll - now), max(0.0, deadline - now)))
    return observed_visible, observed_physical


def summarize_distribution(samples: list[float]) -> dict[str, float]:
    return {
        "p50": round(percentile(samples, 50), 3),
        "p95": round(percentile(samples, 95), 3),
        "p99": round(percentile(samples, 99), 3),
        "stddev": round(0.0 if len(samples) < 2 else statistics.stdev(samples), 3),
    }


def summarize_observed(values: list[int]) -> dict[str, int]:
    unique = sorted(set(values))
    return {"min": unique[0], "max": unique[-1], "distinct_count": len(unique)}


def percentile(samples: list[float], percentile_rank: int) -> float:
    values = sorted(samples)
    if len(values) == 1:
        return values[0]
    rank = (len(values) - 1) * percentile_rank / 100
    lower, upper = math.floor(rank), math.ceil(rank)
    if lower == upper:
        return values[lower]
    return values[lower] + (values[upper] - values[lower]) * (rank - lower)
