from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "1.0.0"
RESULTS_DIR = Path("data") / "benchmark_results"


def utc_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_envelope(
    *,
    run_id: str,
    benchmark: str,
    scale_factor: int,
    database: str,
    workload: dict[str, Any],
    scenario: dict[str, Any],
    protocol: dict[str, Any],
    timing: dict[str, Any],
    result: dict[str, Any],
    status: str = "ok",
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "campaign_id": os.getenv("RMBENCH_CAMPAIGN_ID") or run_id,
        "run_id": run_id,
        "benchmark": benchmark,
        "database": database,
        "status": status,
        "scale_factor": scale_factor,
        "workload": workload,
        "scenario": scenario,
        "protocol": protocol,
        "timing": timing,
        "result": result,
    }


def build_run_timing(*, started_wall: datetime, completed_wall: datetime, total_ms: int) -> dict[str, Any]:
    return {
        "started_utc": utc_timestamp(started_wall),
        "completed_utc": utc_timestamp(completed_wall),
        "total_ms": total_ms,
    }


def build_operation_timing(
    *, timestamps: dict[str, datetime | None], durations_ms: dict[str, int | None]
) -> dict[str, Any]:
    return {
        "timestamps": {name: utc_timestamp(value) for name, value in timestamps.items()},
        "durations_ms": durations_ms,
    }


def build_count_checks(
    *,
    scope: str,
    expected_counts: dict[str, int],
    observed_counts: dict[str, int],
    phase_name: str | None = None,
) -> list[dict[str, Any]]:
    return [
        {
            "scope": scope,
            "table_name": table_name,
            "expected_count": expected_counts[table_name],
            "observed_count": observed_counts[table_name],
            **({"phase_name": phase_name} if phase_name is not None else {}),
        }
        for table_name in sorted(expected_counts)
    ]
