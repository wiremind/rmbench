from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from rmbench.duckdb import statements
from rmbench.duckdb.schema import table_columns, upgrade_to_head
from rmbench.results.timing import summarize_observed
from rmbench.workload.engine import TABLE_NAMES
from rmbench.workload.resources import duckdb_budget
from rmbench.workload.storage import ACCESS_KEY, BUCKET, SECRET_KEY, UPLOAD_ENDPOINT

DATABASE_DIR = Path("data") / "duckdb"


@dataclass(frozen=True)
class SettledState:
    """Deletes are synchronous, so the work is done by the time this is read."""

    is_done: bool = True
    failure_reason: str | None = None


class DuckDBEngine:
    """DuckDB behind the shared workload interface.

    Given the same cpu and memory budget as the ClickHouse container. Nothing runs in
    the background, so `physical_done` is a `CHECKPOINT` inside the measurement.
    """

    name = "duckdb"
    warmup_query = "SELECT 1"
    physical_count_scope = "physical_rows"
    deletes_are_async = False
    logical_view_deduplicates = False

    def __init__(self, *, scale_factor: int) -> None:
        self._path = DATABASE_DIR / f"sf{scale_factor}.duckdb"
        self._threads, self._memory_limit = duckdb_budget()
        self._root: duckdb.DuckDBPyConnection | None = None
        self._lock = threading.Lock()
        self._profile_path = DATABASE_DIR / f"sf{scale_factor}.profile.json"
        self._columns: dict[str, dict[str, str]] = {}
        self._rows_scanned: dict[str, dict[str, Any]] = {}

    @property
    def query_dir(self) -> Path:
        return Path(__file__).resolve().parent / "queries"

    def source_root(self, prefix: str) -> str:
        return f"s3://{BUCKET}/{prefix.strip(chr(47))}"

    # --- connection ------------------------------------------------------------

    def connect(self) -> duckdb.DuckDBPyConnection:
        """A cursor per caller: opening the file N times measures N databases."""
        with self._lock:
            if self._root is None:
                self._root = self._open()
            cursor = self._root.cursor()
        self._configure_session(cursor)
        return cursor

    def close(self, conn: duckdb.DuckDBPyConnection) -> None:
        try:
            conn.close()
        except Exception:
            pass

    def execute(
        self, conn: duckdb.DuckDBPyConnection, sql: str, *, query_id: str | None = None
    ) -> None:
        conn.execute(sql).fetchall()

    def _open(self) -> duckdb.DuckDBPyConnection:
        DATABASE_DIR.mkdir(parents=True, exist_ok=True)
        upgrade_to_head(self._path)
        conn = duckdb.connect(str(self._path))
        conn.execute("INSTALL httpfs")
        # global settings, unlike the per-connection ones in _configure_session
        conn.execute(f"SET threads = {self._threads}")
        conn.execute(f"SET memory_limit = '{self._memory_limit}'")
        conn.execute(f"SET temp_directory = '{DATABASE_DIR / 'tmp'}'")
        self._configure_session(conn)
        self._columns = {table: dict(cols) for table, cols in table_columns(conn).items()}
        return conn

    def _configure_session(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Settings a new cursor does not inherit."""
        conn.execute("LOAD httpfs")
        # `now()` is zoned; without UTC, insertion_datetime is stored in local time
        conn.execute("SET TimeZone = 'UTC'")
        conn.execute(f"SET s3_endpoint='{UPLOAD_ENDPOINT.removeprefix('http://')}'")
        conn.execute("SET s3_url_style='path'")
        conn.execute("SET s3_use_ssl=false")
        conn.execute(f"SET s3_access_key_id='{ACCESS_KEY}'")
        conn.execute(f"SET s3_secret_access_key='{SECRET_KEY}'")

    # --- lifecycle -------------------------------------------------------------

    def ensure_up(self) -> None:
        conn = self.connect()
        self.close(conn)

    def restart(self) -> None:
        """Reopen with an empty buffer pool; the host page cache stays warm."""
        with self._lock:
            if self._root is not None:
                try:
                    self._root.close()
                finally:
                    self._root = None

    def assert_source_ready(self, conn: duckdb.DuckDBPyConnection) -> None:
        rows = conn.execute("SELECT count(*) FROM fact_daily_od_bucket").fetchone()
        if not rows or rows[0] == 0:
            raise ValueError(
                "Query benchmark requires data. Run `rmbench insert --engine duckdb` first."
            )

    # --- statements ------------------------------------------------------------

    def build_insert_statement(self, table_name: str, source: str) -> str:
        return statements.insert_statement(table_name, source, self._columns[table_name])

    def build_delete_statement(
        self, *, table_name: str, sale_window: tuple[datetime, datetime], batch_timestamp: datetime
    ) -> str:
        return statements.delete_statement(
            table_name=table_name, sale_window=sale_window, batch_timestamp=batch_timestamp
        )

    # --- probes ----------------------------------------------------------------

    def count_rows(self, conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
        return {
            table_name: int(conn.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0])
            for table_name in TABLE_NAMES
        }

    def physical_rows(self, conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
        """Same as `count_rows`; there is no active-parts equivalent here."""
        return self.count_rows(conn)

    def batch_counts(
        self, conn: duckdb.DuckDBPyConnection, boundary: str, *, logical_view: bool
    ) -> dict[str, dict[str, int]]:
        counts: dict[str, dict[str, int]] = {}
        for table_name in TABLE_NAMES:
            row = conn.execute(
                f"""
                SELECT
                    count(*) AS total_count,
                    count_if(insertion_datetime >= TIMESTAMP '{boundary}') AS current_batch_count,
                    count_if(insertion_datetime <  TIMESTAMP '{boundary}') AS previous_batch_count
                FROM {table_name}
                """
            ).fetchone()
            counts[table_name] = {
                "total_count": int(row[0]),
                "current_batch_count": int(row[1]),
                "previous_batch_count": int(row[2]),
            }
        return counts

    def settle_physical(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Fold the WAL into the database file."""
        conn.execute("CHECKPOINT")

    def async_task_ids(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> tuple[str, ...]:
        return ()

    def async_task_states(
        self, conn: duckdb.DuckDBPyConnection, ids_by_table: dict[str, tuple[str, ...]]
    ) -> dict[str, SettledState]:
        return dict.fromkeys(ids_by_table, SettledState())

    def progress_snapshot(self, conn: duckdb.DuckDBPyConnection) -> None: ...

    def server_side_stats(self, query_ids: list[str], sql: str) -> dict[str, Any]:
        """Rows scanned. Runs the query again with profiling on, because there is no
        query log and profiling is too slow to leave enabled."""
        if sql in self._rows_scanned:
            return self._rows_scanned[sql]

        profile = self._profile_path
        profile.unlink(missing_ok=True)
        conn = self.connect()
        try:
            conn.execute("PRAGMA enable_profiling = 'json'")
            conn.execute(f"SET profiling_output = '{profile}'")
            conn.execute(sql).fetchall()
        finally:
            self.close(conn)

        if profile.exists():
            scanned = int(json.loads(profile.read_text())["cumulative_rows_scanned"])
            profile.unlink()
            stats = {
                "logged_query_count": 1,
                "missing_query_count": 0,
                "read_rows": summarize_observed([scanned]),
            }
        else:
            # DuckDB 1.5.5 writes no profile for an ungrouped `count(...)`
            stats = {"logged_query_count": 0, "missing_query_count": 1}
        self._rows_scanned[sql] = stats
        return stats

    def protocol_metadata(self, workload: str) -> dict[str, Any]:
        return {
            "insert": {
                "visibility_probe": "select_count",
                "physical_probe": "checkpoint_then_select_count",
            },
            "query_sequential": {
                "server_stats_source": "separate_profiled_execution",
                "cold_method": "reopen_database_per_query_after_startup_query",
                "warm_method": "reopen_database_prime_same_query_once_then_measure",
                "hot_method": "single_reopen_prime_same_query_then_measure_loop",
                "startup_query": self.warmup_query,
            },
            "query_concurrent": {
                "server_stats_source": "separate_profiled_execution",
                "startup_query": self.warmup_query,
            },
            "update": {
                "update_method": "insert_replacement_batch_then_synchronous_delete_previous_batch",
                "physical_probe": "checkpoint",
                "timing_scope": "statement_submit_to_visibility_and_checkpoint",
            },
        }[workload]

