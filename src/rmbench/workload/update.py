from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from rmbench.results.envelope import (
    build_count_checks,
    build_envelope,
    build_run_timing,
    utc_timestamp,
)
from rmbench.results.timing import (
    PROBE_POLL_INTERVAL_SECONDS,
    PROBE_TIMEOUT_SECONDS,
    TableTiming,
    aggregate_milestone,
    monotonic_duration_ms,
    observe_count_milestone,
    record_milestone,
    wait_for_completion,
)
from rmbench.workload.engine import TABLE_NAMES, Engine


@dataclass(frozen=True)
class PhaseResult:
    timings: dict[str, TableTiming]
    observed_visible: dict[str, dict[str, int]]
    observed_physical: dict[str, Any]
    visible_wall: datetime
    visible_monotonic: float
    physical_done_wall: datetime
    physical_done_monotonic: float


def run_update(
    *,
    engine: Engine,
    scale_factor: int,
    s3_prefix: str,
    replay_counts: dict[str, int],
    sale_window: tuple[datetime, datetime],
    scenario: dict[str, object],
) -> dict[str, object]:
    """Insert the replacement rows over the baseline, then delete the old batch."""
    source_root = engine.source_root(s3_prefix)
    conn = engine.connect()
    try:
        baseline = engine.count_rows(conn)
        if baseline != replay_counts:
            raise ValueError(
                "Update benchmark expects the baseline dataset already loaded. "
                f"Visible counts={baseline}, replay counts={replay_counts}."
            )

        submit_wall = datetime.now(UTC)
        submit_monotonic = time.monotonic()
        batch_boundary = submit_wall.strftime("%Y-%m-%dT%H:%M:%S")

        insert_result = _run_insert_phase(
            engine, conn, source_root=source_root, replay_counts=replay_counts, boundary=batch_boundary
        )

        delete_submit_wall = datetime.now(UTC)
        delete_submit_monotonic = time.monotonic()
        delete_result = _run_delete_phase(
            engine,
            conn,
            boundary=batch_boundary,
            replay_counts=replay_counts,
            sale_window=sale_window,
            batch_timestamp=submit_wall,
        )
    finally:
        engine.close(conn)

    # baseline == replay_counts was asserted above, so both batches is exactly twice
    insert_phase_visible = (
        replay_counts
        if engine.logical_view_deduplicates
        else {table_name: count * 2 for table_name, count in replay_counts.items()}
    )

    insert_task_done_ms = _phase_task_done_ms(insert_result.timings)
    delete_task_done_ms = _phase_task_done_ms(delete_result.timings)
    overall = _phase_payload(
        submit_wall=submit_wall,
        submit_monotonic=submit_monotonic,
        visible_wall=delete_result.visible_wall,
        visible_monotonic=delete_result.visible_monotonic,
        physical_done_wall=delete_result.physical_done_wall,
        physical_done_monotonic=delete_result.physical_done_monotonic,
        submit_to_task_done_ms=insert_task_done_ms + delete_task_done_ms,
    )
    insert_payload = _phase_payload(
        submit_wall=submit_wall,
        submit_monotonic=submit_monotonic,
        visible_wall=insert_result.visible_wall,
        visible_monotonic=insert_result.visible_monotonic,
        physical_done_wall=insert_result.physical_done_wall,
        physical_done_monotonic=insert_result.physical_done_monotonic,
        submit_to_task_done_ms=insert_task_done_ms,
    )
    delete_payload = _phase_payload(
        submit_wall=delete_submit_wall,
        submit_monotonic=delete_submit_monotonic,
        visible_wall=delete_result.visible_wall,
        visible_monotonic=delete_result.visible_monotonic,
        physical_done_wall=delete_result.physical_done_wall,
        physical_done_monotonic=delete_result.physical_done_monotonic,
        submit_to_task_done_ms=delete_task_done_ms,
    )

    return build_envelope(
        run_id=f"update-{uuid4().hex[:12]}",
        benchmark="synthetic_update",
        database=engine.name,
        scale_factor=scale_factor,
        workload={"category": "update"},
        scenario=scenario,
        protocol={
            "visibility_probe": "batch_visibility_counts",
            **engine.protocol_metadata("update"),
        },
        timing=build_run_timing(
            started_wall=submit_wall,
            completed_wall=delete_result.physical_done_wall,
            total_ms=overall["durations_ms"]["submit_to_physical_done"],
        ),
        result={
            "timing": overall,
            "count_checks": [
                *build_count_checks(
                    scope="visible_rows",
                    expected_counts=insert_phase_visible,
                    observed_counts=_total_counts(insert_result.observed_visible),
                    phase_name="insert",
                ),
                *build_count_checks(
                    scope="current_batch_active_rows",
                    expected_counts=replay_counts,
                    observed_counts=insert_result.observed_physical,
                    phase_name="insert",
                ),
                *build_count_checks(
                    scope="visible_rows",
                    expected_counts=replay_counts,
                    observed_counts=_total_counts(delete_result.observed_visible),
                    phase_name="delete",
                ),
            ],
            "phases": [
                {"phase_name": "insert", "timing": insert_payload},
                {"phase_name": "delete", "timing": delete_payload},
            ],
            "table_phase_timings": [
                {
                    "table_name": table_name,
                    "phase_name": phase_name,
                    "durations_ms": _table_durations(phase.timings[table_name]),
                }
                for table_name in TABLE_NAMES
                for phase_name, phase in (("insert", insert_result), ("delete", delete_result))
            ],
        },
    )


def _run_insert_phase(
    engine: Engine, conn: Any, *, source_root: str, replay_counts: dict[str, int], boundary: str
) -> PhaseResult:
    timings: dict[str, TableTiming] = {}
    unresolved_visible = set(TABLE_NAMES)
    unresolved_physical = set(TABLE_NAMES)
    observed_visible = engine.batch_counts(conn, boundary, logical_view=True)
    observed_physical: dict[str, int] = dict.fromkeys(TABLE_NAMES, 0)
    normalized_root = source_root.rstrip("/")

    # merges can shrink the total, but replay rows win, so this only rises
    def physical_probe(c: Any) -> dict[str, int]:
        return {
            table_name: counts["current_batch_count"]
            for table_name, counts in engine.batch_counts(c, boundary, logical_view=False).items()
        }

    def observe_visible() -> dict[str, dict[str, int]]:
        return _observe_visibility(
            engine, conn, boundary=boundary, table_timings=timings,
            unresolved=unresolved_visible, expected_counts=replay_counts, logical_view=True,
            complete=_replay_rows_present,
        )

    def observe_physical() -> dict[str, int]:
        return observe_count_milestone(
            conn,
            probe=physical_probe,
            table_timings=timings,
            unresolved=unresolved_physical,
            expected_counts=replay_counts,
            milestone="physical_done",
        )

    for table_name in TABLE_NAMES:
        source = f"{normalized_root}/{table_name}.csv.gz"
        timings[table_name] = _execute_timed(
            engine, conn, engine.build_insert_statement(table_name, source)
        )
        if unresolved_visible:
            observed_visible = observe_visible()
        if unresolved_physical:
            observed_physical = observe_physical()

    observed_visible, observed_physical = wait_for_completion(
        unresolved_visible=unresolved_visible,
        unresolved_physical=unresolved_physical,
        observed_visible=observed_visible,
        observed_physical=observed_physical,
        observe_visible=observe_visible,
        observe_physical=observe_physical,
        observe_progress=lambda: engine.progress_snapshot(conn),
        timeout_message=lambda visible, physical: (
            f"Insert phase stalled: no progress. Expected {replay_counts}, "
            f"visible={visible}, current_batch_rows={physical}."
        ),
    )
    return _phase_result(timings, observed_visible, observed_physical)


def _run_delete_phase(
    engine: Engine,
    conn: Any,
    *,
    boundary: str,
    replay_counts: dict[str, int],
    sale_window: tuple[datetime, datetime],
    batch_timestamp: datetime,
) -> PhaseResult:
    baseline_task_ids = {
        table_name: set(engine.async_task_ids(conn, table_name)) for table_name in TABLE_NAMES
    }
    timings: dict[str, TableTiming] = {}
    unresolved_visible = set(TABLE_NAMES)
    unresolved_physical = set(TABLE_NAMES)
    observed_visible = engine.batch_counts(conn, boundary, logical_view=False)
    observed_physical: dict[str, Any] = {}

    def observe_visible() -> dict[str, dict[str, int]]:
        return _observe_visibility(
            engine, conn, boundary=boundary, table_timings=timings,
            unresolved=unresolved_visible, expected_counts=replay_counts, logical_view=False,
            complete=_baseline_replaced,
        )

    def observe_physical() -> dict[str, Any]:
        return _observe_async_tasks(engine, conn, table_timings=timings, unresolved=unresolved_physical)

    for table_name in TABLE_NAMES:
        timing = _execute_timed(
            engine,
            conn,
            engine.build_delete_statement(
                table_name=table_name, sale_window=sale_window, batch_timestamp=batch_timestamp
            ),
        )
        timings[table_name] = TableTiming(
            submit_monotonic=timing.submit_monotonic,
            task_done_monotonic=timing.task_done_monotonic,
            async_task_ids=_await_new_task_ids(
                engine, conn, table_name=table_name, known=baseline_task_ids[table_name]
            ),
        )
        if unresolved_visible:
            observed_visible = observe_visible()
        if unresolved_physical:
            observed_physical = observe_physical()

    engine.settle_physical(conn)

    observed_visible, observed_physical = wait_for_completion(
        unresolved_visible=unresolved_visible,
        unresolved_physical=unresolved_physical,
        observed_visible=observed_visible,
        observed_physical=observed_physical,
        observe_visible=observe_visible,
        observe_physical=observe_physical,
        observe_progress=lambda: engine.progress_snapshot(conn),
        timeout_message=lambda visible, physical: (
            f"Delete phase stalled: no progress. Expected {replay_counts}, "
            f"visible={visible}, tasks={physical}."
        ),
    )
    return _phase_result(timings, observed_visible, observed_physical)


def _observe_visibility(
    engine: Engine,
    conn: Any,
    *,
    boundary: str,
    table_timings: dict[str, TableTiming],
    unresolved: set[str],
    expected_counts: dict[str, int],
    logical_view: bool,
    complete: Callable[[dict[str, int], int], bool],
) -> dict[str, dict[str, int]]:
    return record_milestone(
        table_timings=table_timings,
        unresolved=unresolved,
        observed_values=engine.batch_counts(conn, boundary, logical_view=logical_view),
        milestone="visible",
        is_complete=lambda table_name, counts: complete(counts, expected_counts[table_name]),
    )


def _replay_rows_present(counts: dict[str, int], expected: int) -> bool:
    """Insert phase: every replay row is queryable, baseline or not."""
    return counts["current_batch_count"] == expected


def _baseline_replaced(counts: dict[str, int], expected: int) -> bool:
    """Delete phase: only the replay rows are left."""
    return (
        counts["total_count"] == expected
        and counts["current_batch_count"] == expected
        and counts["previous_batch_count"] == 0
    )


def _observe_async_tasks(
    engine: Engine, conn: Any, *, table_timings: dict[str, TableTiming], unresolved: set[str]
) -> dict[str, Any]:
    states = engine.async_task_states(
        conn, {name: timing.async_task_ids for name, timing in table_timings.items()}
    )
    return record_milestone(
        table_timings=table_timings,
        unresolved=unresolved,
        observed_values=states,
        milestone="physical_done",
        is_complete=lambda _table_name, state: state.is_done,
        validate=lambda table_name, state: _raise_if_failed(engine, table_name, state),
    )


def _raise_if_failed(engine: Engine, table_name: str, state: Any) -> None:
    reason = state.failure_reason
    if reason:
        raise RuntimeError(f"Delete failed for {table_name} on {engine.name}: {reason}")


def _await_new_task_ids(
    engine: Engine, conn: Any, *, table_name: str, known: set[str]
) -> tuple[str, ...]:
    """Ids the delete just spawned; empty where deletes are synchronous."""
    if not engine.deletes_are_async:
        return ()
    deadline = time.monotonic() + PROBE_TIMEOUT_SECONDS
    while True:
        new_ids = tuple(sorted(set(engine.async_task_ids(conn, table_name)) - known))
        if new_ids:
            return new_ids
        if time.monotonic() >= deadline:
            raise TimeoutError(f"No background task appeared for the delete on {table_name}.")
        time.sleep(PROBE_POLL_INTERVAL_SECONDS)


def _execute_timed(engine: Engine, conn: Any, sql: str) -> TableTiming:
    submit_monotonic = time.monotonic()
    engine.execute(conn, sql)
    return TableTiming(submit_monotonic=submit_monotonic, task_done_monotonic=time.monotonic())


def _phase_result(
    timings: dict[str, TableTiming], observed_visible: Any, observed_physical: Any
) -> PhaseResult:
    visible_wall, visible_monotonic = aggregate_milestone(timings, "visible")
    physical_done_wall, physical_done_monotonic = aggregate_milestone(timings, "physical_done")
    return PhaseResult(
        timings=timings,
        observed_visible=observed_visible,
        observed_physical=observed_physical,
        visible_wall=visible_wall,
        visible_monotonic=visible_monotonic,
        physical_done_wall=physical_done_wall,
        physical_done_monotonic=physical_done_monotonic,
    )


def _phase_task_done_ms(table_timings: dict[str, TableTiming]) -> int:
    return sum(
        monotonic_duration_ms(t.submit_monotonic, t.task_done_monotonic) for t in table_timings.values()
    )


def _table_durations(timing: TableTiming) -> dict[str, int | None]:
    return {
        "submit_to_ack": None,
        "submit_to_task_done": monotonic_duration_ms(timing.submit_monotonic, timing.task_done_monotonic),
        "submit_to_visible": monotonic_duration_ms(timing.submit_monotonic, timing.visible_monotonic),
        "submit_to_physical_done": monotonic_duration_ms(
            timing.submit_monotonic, timing.physical_done_monotonic
        ),
    }


def _phase_payload(
    *,
    submit_wall: datetime,
    submit_monotonic: float,
    visible_wall: datetime,
    visible_monotonic: float,
    physical_done_wall: datetime,
    physical_done_monotonic: float,
    submit_to_task_done_ms: int,
) -> dict[str, Any]:
    return {
        "timestamps": {
            "submit_utc": utc_timestamp(submit_wall),
            "ack_utc": None,
            "task_done_utc": None,
            "visible_utc": utc_timestamp(visible_wall),
            "physical_done_utc": utc_timestamp(physical_done_wall),
        },
        "durations_ms": {
            "submit_to_ack": None,
            "submit_to_task_done": submit_to_task_done_ms,
            "submit_to_visible": monotonic_duration_ms(submit_monotonic, visible_monotonic),
            "submit_to_physical_done": monotonic_duration_ms(submit_monotonic, physical_done_monotonic),
        },
    }


def _total_counts(visibility_counts: dict[str, dict[str, int]]) -> dict[str, int]:
    return {table_name: counts["total_count"] for table_name, counts in visibility_counts.items()}
