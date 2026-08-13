from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from rmbench.io_utils import DATA_MANIFEST_FILENAME
from rmbench.results.envelope import (
    build_count_checks,
    build_envelope,
    build_operation_timing,
    build_run_timing,
)
from rmbench.results.timing import (
    TableTiming,
    aggregate_milestone,
    monotonic_duration_ms,
    observe_count_milestone,
    wait_for_completion,
)
from rmbench.workload.engine import TABLE_NAMES, Engine


def expected_table_counts(input_dir: Path) -> dict[str, int]:
    """Row counts per table, from the generator's manifest."""
    manifest_path = input_dir / DATA_MANIFEST_FILENAME
    if not manifest_path.exists():
        raise ValueError(f"Missing {manifest_path}. Generate the scale factor first.")
    tables = json.loads(manifest_path.read_text())["tables"]
    return {
        table_name: sum(member["row_count"] for member in tables[table_name])
        for table_name in TABLE_NAMES
    }


def run_insert(
    *,
    engine: Engine,
    scale_factor: int,
    input_dir: Path,
    s3_prefix: str,
    scenario: dict[str, object],
) -> dict[str, object]:
    """Insert every table from object storage, timing each one."""
    source_root = engine.source_root(s3_prefix).rstrip("/")
    expected_counts = expected_table_counts(input_dir)
    conn = engine.connect()
    try:
        existing = engine.count_rows(conn)
        if any(existing.values()):
            raise ValueError(f"Insert benchmark expects empty target tables, found: {existing}")

        submit_wall = datetime.now(UTC)
        submit_monotonic = time.monotonic()
        table_timings: dict[str, TableTiming] = {}
        unresolved_visible = set(TABLE_NAMES)
        unresolved_physical = set(TABLE_NAMES)
        observed_counts = dict.fromkeys(TABLE_NAMES, 0)
        observed_physical = dict.fromkeys(TABLE_NAMES, 0)

        def observe_visible() -> dict[str, int]:
            return observe_count_milestone(
                conn,
                probe=engine.count_rows,
                table_timings=table_timings,
                unresolved=unresolved_visible,
                expected_counts=expected_counts,
                milestone="visible",
            )

        def observe_physical() -> dict[str, int]:
            # settle first
            engine.settle_physical(conn)
            return observe_count_milestone(
                conn,
                probe=engine.physical_rows,
                table_timings=table_timings,
                unresolved=unresolved_physical,
                expected_counts=expected_counts,
                milestone="physical_done",
            )

        for table_name in TABLE_NAMES:
            source = f"{source_root}/{table_name}.csv.gz"
            table_submit_monotonic = time.monotonic()
            engine.execute(conn, engine.build_insert_statement(table_name, source))
            table_timings[table_name] = TableTiming(
                submit_monotonic=table_submit_monotonic,
                task_done_monotonic=time.monotonic(),
            )
            # assign only when a probe ran
            if unresolved_visible:
                observed_counts = observe_visible()
            if unresolved_physical:
                observed_physical = observe_physical()

        observed_counts, observed_physical = wait_for_completion(
            unresolved_visible=unresolved_visible,
            unresolved_physical=unresolved_physical,
            observed_visible=observed_counts,
            observed_physical=observed_physical,
            observe_visible=observe_visible,
            observe_physical=observe_physical,
            observe_progress=lambda: engine.progress_snapshot(conn),
            timeout_message=lambda visible, physical: (
                f"Insert stalled: no progress for the probe timeout. Expected "
                f"{expected_counts}, visible={visible}, physical={physical}."
            ),
        )
    finally:
        engine.close(conn)

    visible_wall, visible_monotonic = aggregate_milestone(table_timings, "visible")
    physical_wall, physical_monotonic = aggregate_milestone(table_timings, "physical_done")

    table_durations: dict[str, dict[str, Any]] = {}
    execution_ms = 0
    for table_name in TABLE_NAMES:
        timing = table_timings[table_name]
        submit_to_task_done = monotonic_duration_ms(
            timing.submit_monotonic, timing.task_done_monotonic
        )
        execution_ms += submit_to_task_done
        table_durations[table_name] = {
            "submit_to_ack": None,
            "submit_to_task_done": submit_to_task_done,
            "submit_to_visible": monotonic_duration_ms(
                timing.submit_monotonic, timing.visible_monotonic
            ),
            "submit_to_physical_done": monotonic_duration_ms(
                timing.submit_monotonic, timing.physical_done_monotonic
            ),
        }

    return build_envelope(
        run_id=f"insert-{uuid4().hex[:12]}",
        benchmark="synthetic_insert",
        database=engine.name,
        scale_factor=scale_factor,
        workload={"category": "insert"},
        scenario=scenario,
        protocol={
            "timing_scope": "statement_submit_to_visibility_and_physical_completion",
            **engine.protocol_metadata("insert"),
        },
        timing=build_run_timing(
            started_wall=submit_wall,
            completed_wall=physical_wall,
            total_ms=monotonic_duration_ms(submit_monotonic, physical_monotonic),
        ),
        result={
            "timing": build_operation_timing(
                timestamps={
                    "submit_utc": submit_wall,
                    "ack_utc": None,
                    "task_done_utc": None,
                    "visible_utc": visible_wall,
                    "physical_done_utc": physical_wall,
                },
                durations_ms={
                    "submit_to_ack": None,
                    "submit_to_task_done": execution_ms,
                    "submit_to_visible": monotonic_duration_ms(submit_monotonic, visible_monotonic),
                    "submit_to_physical_done": monotonic_duration_ms(
                        submit_monotonic, physical_monotonic
                    ),
                },
            ),
            "count_checks": [
                *build_count_checks(
                    scope="visible_rows",
                    expected_counts=expected_counts,
                    observed_counts=observed_counts,
                ),
                *build_count_checks(
                    scope=engine.physical_count_scope,
                    expected_counts=expected_counts,
                    observed_counts=observed_physical,
                ),
            ],
            "table_timings": [
                {"table_name": table_name, "durations_ms": table_durations[table_name]}
                for table_name in TABLE_NAMES
            ],
        },
    )
