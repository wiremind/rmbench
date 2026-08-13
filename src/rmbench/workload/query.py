from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from rmbench.results.envelope import build_envelope, build_run_timing
from rmbench.results.timing import summarize_distribution
from rmbench.workload.engine import Engine

COLD_RUNS = 3
WARM_RUNS = 3
HOT_RUNS = 21
WARM_PRIMING_RUNS = 1
HOT_WARMUP_RUNS = 3
CONCURRENT_WARMUP_RUNS = 3
CONCURRENT_RUNS = 11
START_SIGNAL_TIMEOUT_SECONDS = 120.0


@dataclass(frozen=True)
class QuerySpec:
    name: str
    sql: str


def available_query_families(query_dir: Path) -> tuple[str, ...]:
    return tuple(sorted(path.stem for path in query_dir.glob("*.sql")))


def window_placeholders(window: dict[str, object]) -> dict[str, str]:
    """The four date tokens the query families expect, from a bundle sf_window."""
    sale_start = date.fromisoformat(str(window["sale_start"]))
    departure_start = date.fromisoformat(str(window["departure_start"]))
    return {
        "__SALE_START__": sale_start.isoformat(),
        "__SALE_END__": (sale_start + timedelta(days=int(window["sale_days"]))).isoformat(),
        "__DEPARTURE_START__": departure_start.isoformat(),
        "__DEPARTURE_END__": (
            departure_start + timedelta(days=int(window["departure_days"]))
        ).isoformat(),
    }


def load_queries(query_dir: Path, family: str, placeholders: dict[str, str]) -> list[QuerySpec]:
    family_path = query_dir / f"{family}.sql"
    if not family_path.exists():
        raise ValueError(f"Unknown query family {family}. Expected {family_path}.")
    text = family_path.read_text()
    for token, value in placeholders.items():
        text = text.replace(token, value)
    return parse_query_specs(text)


def parse_query_specs(text: str) -> list[QuerySpec]:
    queries: list[QuerySpec] = []
    current_name: str | None = None
    current_lines: list[str] = []

    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("-- name:"):
            if current_name is not None:
                raise ValueError(f"Query {current_name} is missing a terminating semicolon.")
            current_name = stripped.removeprefix("-- name:").strip()
            if not current_name:
                raise ValueError("Query names cannot be empty.")
            current_lines = []
            continue

        if current_name is None:
            if stripped:
                raise ValueError("Every query must be introduced by a '-- name:' line.")
            continue

        current_lines.append(line)
        if stripped.endswith(";"):
            sql = "\n".join(current_lines).strip()
            queries.append(QuerySpec(name=current_name, sql=sql[:-1].strip()))
            current_name = None
            current_lines = []

    if current_name is not None:
        raise ValueError(f"Query {current_name} is missing a terminating semicolon.")
    if not queries:
        raise ValueError("Query family did not contain any named queries.")
    return queries


def run_query_family(
    *, engine: Engine, sf: int, family: str, window: dict[str, object], scenario: dict[str, object]
) -> dict[str, object]:
    """Cold restarts before every run, warm restarts then primes once, hot restarts
    once then loops."""
    started_wall = datetime.now(UTC)
    started_monotonic = time.perf_counter()
    queries = load_queries(engine.query_dir, family, window_placeholders(window))
    _prepare(engine)

    groups = {
        "cold": _run_restart_group(engine, queries, runs=COLD_RUNS, priming_runs=0),
        "warm": _run_restart_group(engine, queries, runs=WARM_RUNS, priming_runs=WARM_PRIMING_RUNS),
        "hot": _run_hot_group(engine, queries),
    }
    results = {
        query.name: {
            name: _build_query_result(
                engine, groups[name][0][query.name], groups[name][1][query.name], sql=query.sql
            )
            for name in ("cold", "warm", "hot")
        }
        for query in queries
    }
    group_runs = {"cold": COLD_RUNS, "warm": WARM_RUNS, "hot": HOT_RUNS}

    return build_envelope(
        run_id=f"query-{uuid4().hex[:12]}",
        benchmark="synthetic_query",
        database=engine.name,
        scale_factor=sf,
        workload={"category": "query", "family": family, "mode": "sequential"},
        scenario=scenario,
        protocol={
            "cold_runs": COLD_RUNS,
            "warm_runs": WARM_RUNS,
            "hot_runs": HOT_RUNS,
            "warm_priming_runs": WARM_PRIMING_RUNS,
            "hot_warmup_runs": HOT_WARMUP_RUNS,
            "os_cache_purge": "unavailable",
            "timing_scope": "client_execute_full_fetch",
            **engine.protocol_metadata("query_sequential"),
        },
        timing=build_run_timing(
            started_wall=started_wall,
            completed_wall=datetime.now(UTC),
            total_ms=int(round((time.perf_counter() - started_monotonic) * 1000)),
        ),
        result={
            "groups": [
                {
                    "group_name": name,
                    "runs": group_runs[name],
                    "queries": [{"query_name": q.name, **results[q.name][name]} for q in queries],
                }
                for name in ("cold", "warm", "hot")
            ]
        },
    )


def run_query_family_concurrent(
    *,
    engine: Engine,
    sf: int,
    family: str,
    window: dict[str, object],
    scenario: dict[str, object],
    concurrency: int,
) -> dict[str, object]:
    """Every worker runs the same query, released from a shared start barrier."""
    started_wall = datetime.now(UTC)
    started_monotonic = time.perf_counter()
    queries = load_queries(engine.query_dir, family, window_placeholders(window))
    _prepare(engine)

    results: dict[str, dict[str, object]] = {}
    for query in queries:
        samples, measured_wall_ms, query_ids = _collect_concurrent_samples(
            engine, query.sql, concurrency=concurrency
        )
        results[query.name] = _build_query_result(
            engine,
            samples,
            query_ids,
            sql=query.sql,
            extra={
                "throughput_qps": round(len(samples) / (measured_wall_ms / 1000.0), 3),
                "measured_wall_ms": round(measured_wall_ms, 3),
                "total_executions": len(samples),
            },
        )

    return build_envelope(
        run_id=f"query-{uuid4().hex[:12]}",
        benchmark="synthetic_query",
        database=engine.name,
        scale_factor=sf,
        workload={"category": "query", "family": family, "mode": "concurrent"},
        scenario=scenario,
        protocol={
            "concurrency": concurrency,
            "runs_per_worker": CONCURRENT_RUNS,
            "concurrent_warmup_runs": CONCURRENT_WARMUP_RUNS,
            "concurrent_method": "single_query_same_start_fixed_count_per_worker",
            "os_cache_purge": "unavailable",
            "timing_scope": "client_execute_full_fetch",
            **engine.protocol_metadata("query_concurrent"),
        },
        timing=build_run_timing(
            started_wall=started_wall,
            completed_wall=datetime.now(UTC),
            total_ms=int(round((time.perf_counter() - started_monotonic) * 1000)),
        ),
        result={
            "groups": [
                {
                    "group_name": "concurrent",
                    "runs_per_worker": CONCURRENT_RUNS,
                    "concurrency": concurrency,
                    "queries": [{"query_name": q.name, **results[q.name]} for q in queries],
                }
            ]
        },
    )


def _prepare(engine: Engine) -> None:
    engine.ensure_up()
    conn = engine.connect()
    try:
        engine.assert_source_ready(conn)
    finally:
        engine.close(conn)


def _run_restart_group(
    engine: Engine, queries: list[QuerySpec], *, runs: int, priming_runs: int
) -> tuple[dict[str, list[float]], dict[str, list[str]]]:
    samples: dict[str, list[float]] = {}
    query_ids: dict[str, list[str]] = {}
    for query in queries:
        samples[query.name], query_ids[query.name] = _collect_samples_after_restart(
            engine, query.sql, runs=runs, priming_runs=priming_runs
        )
    return samples, query_ids


def _run_hot_group(
    engine: Engine, queries: list[QuerySpec]
) -> tuple[dict[str, list[float]], dict[str, list[str]]]:
    samples: dict[str, list[float]] = {}
    query_ids: dict[str, list[str]] = {}
    engine.restart()
    conn = engine.connect()
    try:
        engine.execute(conn, engine.warmup_query)
        for query in queries:
            samples[query.name], query_ids[query.name] = _measure_runs(
                engine, conn, query.sql, runs=HOT_RUNS, warmup_runs=HOT_WARMUP_RUNS
            )
    finally:
        engine.close(conn)
    return samples, query_ids


def _collect_samples_after_restart(
    engine: Engine, query: str, *, runs: int, priming_runs: int
) -> tuple[list[float], list[str]]:
    samples: list[float] = []
    query_ids: list[str] = []
    for _ in range(runs):
        engine.restart()
        conn = engine.connect()
        try:
            engine.execute(conn, engine.warmup_query)
            run_samples, run_ids = _measure_runs(
                engine, conn, query, warmup_runs=priming_runs, runs=1
            )
            samples.extend(run_samples)
            query_ids.extend(run_ids)
        finally:
            engine.close(conn)
    return samples, query_ids


def _collect_concurrent_samples(
    engine: Engine, query: str, *, concurrency: int
) -> tuple[list[float], float, list[str]]:
    engine.restart()
    ready_barrier = threading.Barrier(concurrency + 1)
    start_event = threading.Event()
    errors: list[str] = []
    worker_samples: list[list[float]] = [[] for _ in range(concurrency)]
    worker_query_ids: list[list[str]] = [[] for _ in range(concurrency)]

    def worker(index: int) -> None:
        # one connection per worker
        conn = engine.connect()
        try:
            engine.execute(conn, engine.warmup_query)
            for _ in range(CONCURRENT_WARMUP_RUNS):
                engine.execute(conn, query)
            # no timeout: cold warmup scales with SF; a failed worker aborts the
            # barrier, so this cannot deadlock
            ready_barrier.wait()
            if not start_event.wait(timeout=START_SIGNAL_TIMEOUT_SECONDS):
                raise TimeoutError("Concurrent start signal was not released.")
            worker_samples[index], worker_query_ids[index] = _measure_runs(
                engine, conn, query, warmup_runs=0, runs=CONCURRENT_RUNS
            )
        except Exception as exc:
            errors.append(f"worker {index}: {exc!r}")
            try:
                ready_barrier.abort()
            except threading.BrokenBarrierError:
                pass
        finally:
            engine.close(conn)

    workers = [threading.Thread(target=worker, args=(i,)) for i in range(concurrency)]
    for thread in workers:
        thread.start()

    try:
        ready_barrier.wait()
    except threading.BrokenBarrierError as exc:
        for thread in workers:
            thread.join()
        raise RuntimeError(
            "Concurrent query setup failed." + (f" {'; '.join(errors)}" if errors else "")
        ) from exc
    except BaseException:
        ready_barrier.abort()
        raise

    start = time.perf_counter()
    start_event.set()
    for thread in workers:
        thread.join()
    measured_wall_ms = (time.perf_counter() - start) * 1000

    if errors:
        raise RuntimeError("Concurrent query execution failed. " + "; ".join(errors))
    return (
        [s for worker_result in worker_samples for s in worker_result],
        measured_wall_ms,
        [q for worker_result in worker_query_ids for q in worker_result],
    )


def _measure_runs(
    engine: Engine, conn: Any, query: str, *, warmup_runs: int, runs: int
) -> tuple[list[float], list[str]]:
    for _ in range(warmup_runs):
        engine.execute(conn, query)
    query_ids: list[str] = []
    samples: list[float] = []
    for _ in range(runs):
        query_id = f"query-bench-{uuid4().hex}"
        query_ids.append(query_id)
        start = time.perf_counter()
        engine.execute(conn, query, query_id=query_id)
        samples.append((time.perf_counter() - start) * 1000)
    return samples, query_ids


def _build_query_result(
    engine: Engine,
    samples: list[float],
    query_ids: list[str],
    *,
    sql: str,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    result = {
        "latency_ms": summarize_distribution(samples),
        "query_log": engine.server_side_stats(query_ids, sql),
    }
    if extra:
        result.update(extra)
    return result
