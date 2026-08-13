from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from rmbench.io_utils import write_json_file
from rmbench.results.envelope import RESULTS_DIR, SCHEMA_VERSION

COLLECTED_DIR = Path("data") / "collected"

RUNS_COLUMNS = [
    "campaign_id",
    "scale_factor",
    "run_id",
    "benchmark",
    "database",
    "status",
    "workload_category",
    "workload_family",
    "workload_mode",
    "scenario_label",
    "sale_days",
    "departure_days",
    "sale_start",
    "sale_end",
    "departure_start",
    "departure_end",
    "source_prefix",
    "row_change_percent",
    "field_change_percent",
    "scenario_concurrency",
    "started_utc",
    "completed_utc",
    "total_ms",
]

TIMINGS_COLUMNS = [
    *RUNS_COLUMNS,
    "entity_type",
    "group_name",
    "phase_name",
    "table_name",
    "metric_name",
    "metric_value_ms",
]

QUERY_METRICS_COLUMNS = [
    *RUNS_COLUMNS,
    "group_name",
    "group_runs",
    "group_runs_per_worker",
    "group_concurrency",
    "query_name",
    "latency_p50_ms",
    "latency_p95_ms",
    "latency_p99_ms",
    "latency_stddev_ms",
    "query_log_p50_ms",
    "query_log_p95_ms",
    "query_log_p99_ms",
    "query_log_stddev_ms",
    "read_rows_min",
    "read_rows_max",
    "read_rows_distinct_count",
    "read_bytes_min",
    "read_bytes_max",
    "read_bytes_distinct_count",
    "memory_usage_p50_bytes",
    "memory_usage_p95_bytes",
    "memory_usage_p99_bytes",
    "memory_usage_stddev_bytes",
    "logged_query_count",
    "missing_query_count",
    "throughput_qps",
    "measured_wall_ms",
    "total_executions",
]

COUNT_CHECKS_COLUMNS = [
    *RUNS_COLUMNS,
    "phase_name",
    "scope",
    "table_name",
    "expected_count",
    "observed_count",
    "matches",
]

REQUIRED_RESULT_KEYS = {
    "schema_version",
    "campaign_id",
    "run_id",
    "benchmark",
    "database",
    "status",
    "scale_factor",
    "workload",
    "scenario",
    "protocol",
    "timing",
    "result",
}


def collect_results(
    *, campaign_id: str, scale_factors: tuple[int, ...] = (), results_dir: Path = RESULTS_DIR
) -> dict[str, Any]:
    """Flatten one campaign's result JSONs into CSV tables.

    Rebuilt from the raw results every time, so re-running after more runs land
    simply widens the snapshot.
    """
    selected = tuple(sorted(set(scale_factors)))
    label = "all_scale_factors" if not selected else "sf" + "_".join(str(sf) for sf in selected)
    output_dir = COLLECTED_DIR / campaign_id / label
    output_dir.mkdir(parents=True, exist_ok=True)

    runs_rows: list[dict[str, Any]] = []
    timings_rows: list[dict[str, Any]] = []
    query_rows: list[dict[str, Any]] = []
    count_check_rows: list[dict[str, Any]] = []
    collected_files: list[str] = []
    skipped_files: list[dict[str, str]] = []

    for path in sorted(results_dir.glob("*.json")):
        data = json.loads(path.read_text())
        if data.get("campaign_id") != campaign_id:
            continue
        if selected and int(data.get("scale_factor", -1)) not in selected:
            continue
        if not REQUIRED_RESULT_KEYS <= set(data) or data.get("schema_version") != SCHEMA_VERSION:
            skipped_files.append({"path": str(path), "reason": "not the current result schema"})
            continue

        run_base = _run_base_row(data)
        runs_rows.append(run_base)
        collected_files.append(str(path))
        timings_rows.extend(_flatten_timings(data, run_base))
        query_rows.extend(_flatten_query_metrics(data, run_base))
        count_check_rows.extend(_flatten_count_checks(data, run_base))

    _write_csv(output_dir / "runs.csv", RUNS_COLUMNS, runs_rows)
    _write_csv(output_dir / "timings.csv", TIMINGS_COLUMNS, timings_rows)
    _write_csv(output_dir / "query_metrics.csv", QUERY_METRICS_COLUMNS, query_rows)
    _write_csv(output_dir / "count_checks.csv", COUNT_CHECKS_COLUMNS, count_check_rows)

    summary = {
        "campaign_id": campaign_id,
        "scale_factors": list(selected),
        "output_dir": str(output_dir),
        "collected_result_file_count": len(collected_files),
        "skipped_result_file_count": len(skipped_files),
        "collected_files": collected_files,
        "skipped_files": skipped_files,
        "row_counts": {
            "runs": len(runs_rows),
            "timings": len(timings_rows),
            "query_metrics": len(query_rows),
            "count_checks": len(count_check_rows),
        },
    }
    write_json_file(output_dir / "summary.json", summary)
    return summary


def _run_base_row(data: dict[str, Any]) -> dict[str, Any]:
    workload = data["workload"]
    window = data["scenario"]["window"]
    parameters = data["scenario"].get("parameters", {})
    timing = data["timing"]
    return {
        "campaign_id": data["campaign_id"],
        "scale_factor": data["scale_factor"],
        "run_id": data["run_id"],
        "benchmark": data["benchmark"],
        "database": data["database"],
        "status": data["status"],
        "workload_category": workload.get("category"),
        "workload_family": workload.get("family"),
        "workload_mode": workload.get("mode"),
        "scenario_label": window.get("label"),
        "sale_days": window.get("sale_days"),
        "departure_days": window.get("departure_days"),
        "sale_start": window.get("sale_start"),
        "sale_end": window.get("sale_end"),
        "departure_start": window.get("departure_start"),
        "departure_end": window.get("departure_end"),
        "source_prefix": parameters.get("source_prefix"),
        "row_change_percent": parameters.get("row_change_percent"),
        "field_change_percent": parameters.get("field_change_percent"),
        "scenario_concurrency": parameters.get("concurrency"),
        "started_utc": timing.get("started_utc"),
        "completed_utc": timing.get("completed_utc"),
        "total_ms": timing.get("total_ms"),
    }


def _flatten_timings(data: dict[str, Any], run_base: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    result = data["result"]
    benchmark = data["benchmark"]

    if benchmark in {"synthetic_insert", "synthetic_update"}:
        rows.extend(_duration_rows(run_base, result["timing"]["durations_ms"], entity_type="run"))

    if benchmark == "synthetic_insert":
        for entry in result.get("table_timings", []):
            rows.extend(
                _duration_rows(
                    run_base, entry["durations_ms"], entity_type="table", table_name=entry["table_name"]
                )
            )

    if benchmark == "synthetic_update":
        for entry in result.get("phases", []):
            rows.extend(
                _duration_rows(
                    run_base,
                    entry["timing"]["durations_ms"],
                    entity_type="phase",
                    phase_name=entry["phase_name"],
                )
            )
        for entry in result.get("table_phase_timings", []):
            rows.extend(
                _duration_rows(
                    run_base,
                    entry["durations_ms"],
                    entity_type="table_phase",
                    phase_name=entry["phase_name"],
                    table_name=entry["table_name"],
                )
            )
    return rows


def _duration_rows(
    run_base: dict[str, Any],
    durations_ms: dict[str, Any],
    *,
    entity_type: str,
    phase_name: str | None = None,
    table_name: str | None = None,
) -> list[dict[str, Any]]:
    return [
        {
            **run_base,
            "entity_type": entity_type,
            "group_name": None,
            "phase_name": phase_name,
            "table_name": table_name,
            "metric_name": metric_name,
            "metric_value_ms": metric_value,
        }
        for metric_name, metric_value in durations_ms.items()
    ]


def _flatten_query_metrics(data: dict[str, Any], run_base: dict[str, Any]) -> list[dict[str, Any]]:
    if data["benchmark"] != "synthetic_query":
        return []

    rows: list[dict[str, Any]] = []
    for group in data["result"].get("groups", []):
        for query in group.get("queries", []):
            query_log = query["query_log"]
            duration = query_log.get("query_duration_ms", {})
            memory = query_log.get("memory_usage_bytes", {})
            read_rows = query_log.get("read_rows", {})
            read_bytes = query_log.get("read_bytes", {})
            latency = query["latency_ms"]
            rows.append({
                **run_base,
                "group_name": group["group_name"],
                "group_runs": group.get("runs"),
                "group_runs_per_worker": group.get("runs_per_worker"),
                "group_concurrency": group.get("concurrency"),
                "query_name": query["query_name"],
                "latency_p50_ms": latency.get("p50"),
                "latency_p95_ms": latency.get("p95"),
                "latency_p99_ms": latency.get("p99"),
                "latency_stddev_ms": latency.get("stddev"),
                "query_log_p50_ms": duration.get("p50"),
                "query_log_p95_ms": duration.get("p95"),
                "query_log_p99_ms": duration.get("p99"),
                "query_log_stddev_ms": duration.get("stddev"),
                "read_rows_min": read_rows.get("min"),
                "read_rows_max": read_rows.get("max"),
                "read_rows_distinct_count": read_rows.get("distinct_count"),
                "read_bytes_min": read_bytes.get("min"),
                "read_bytes_max": read_bytes.get("max"),
                "read_bytes_distinct_count": read_bytes.get("distinct_count"),
                "memory_usage_p50_bytes": memory.get("p50"),
                "memory_usage_p95_bytes": memory.get("p95"),
                "memory_usage_p99_bytes": memory.get("p99"),
                "memory_usage_stddev_bytes": memory.get("stddev"),
                "logged_query_count": query_log.get("logged_query_count"),
                "missing_query_count": query_log.get("missing_query_count"),
                "throughput_qps": query.get("throughput_qps"),
                "measured_wall_ms": query.get("measured_wall_ms"),
                "total_executions": query.get("total_executions"),
            })
    return rows


def _flatten_count_checks(data: dict[str, Any], run_base: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            **run_base,
            "phase_name": entry.get("phase_name"),
            "scope": entry["scope"],
            "table_name": entry["table_name"],
            "expected_count": entry["expected_count"],
            "observed_count": entry["observed_count"],
            "matches": entry["expected_count"] == entry["observed_count"],
        }
        for entry in data["result"].get("count_checks", [])
    ]


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
