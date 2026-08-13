from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.request import urlopen

from clickhouse_driver import Client

from rmbench.clickhouse import statements
from rmbench.results.timing import summarize_distribution, summarize_observed
from rmbench.workload.engine import TABLE_NAMES
from rmbench.workload.resources import write_compose_env
from rmbench.workload.storage import ACCESS_KEY, COMPOSE_ENDPOINT, SECRET_KEY, s3_source_root

HOST = "127.0.0.1"
PORT = 9000
USER = "benchmark"
PASSWORD = "benchmark"
DATABASE = "rmbench"

COMPOSE_FILE = Path("clickhouse/compose.yaml")
READY_TIMEOUT_SECONDS = 120.0
READY_POLL_INTERVAL_SECONDS = 1.0


@dataclass(frozen=True)
class MutationState:
    """One table's `system.mutations` rows."""

    task_ids: tuple[str, ...] = ()
    found_task_ids: tuple[str, ...] = ()
    is_done: bool = False
    parts_to_do: int | None = None
    failure_reason: str | None = None


class ClickHouseEngine:
    """ClickHouse behind the shared workload interface.

    Visible is a `FINAL` count; physical is rows in active parts, plus the mutation
    reporting done for deletes. `cold` restarts the container, clearing ClickHouse's
    caches but not the host page cache.
    """

    name = "clickhouse"
    warmup_query = "SELECT 1"
    physical_count_scope = "active_part_rows"
    deletes_are_async = True
    logical_view_deduplicates = True

    def __init__(self, *, access_key: str = ACCESS_KEY, secret_key: str = SECRET_KEY) -> None:
        self._access_key = access_key
        self._secret_key = secret_key

    @property
    def query_dir(self) -> Path:
        return Path(__file__).resolve().parent / "queries"

    def source_root(self, prefix: str) -> str:
        # the s3 table function fetches over HTTP from inside the compose network
        return s3_source_root(prefix=prefix, endpoint_url=COMPOSE_ENDPOINT)

    # --- connection ------------------------------------------------------------

    def connect(self) -> Client:
        return Client(host=HOST, port=PORT, user=USER, password=PASSWORD, database=DATABASE)

    def close(self, conn: Client) -> None:
        try:
            conn.disconnect_connection()
        except Exception:
            pass

    def execute(self, conn: Client, sql: str, *, query_id: str | None = None) -> None:
        conn.execute(sql, query_id=query_id) if query_id else conn.execute(sql)

    # --- lifecycle -------------------------------------------------------------

    def ensure_up(self) -> None:
        self._compose("up", "-d", "clickhouse")
        self._wait_ready()

    def restart(self) -> None:
        self._compose("restart", "clickhouse")
        self._wait_ready()

    def assert_source_ready(self, conn: Client) -> None:
        if int(conn.execute("EXISTS TABLE fact_daily_od_bucket")[0][0]) != 1:
            raise ValueError("Query benchmark requires fact_daily_od_bucket. Run the migrations first.")
        if int(conn.execute("SELECT count() FROM fact_daily_od_bucket")[0][0]) == 0:
            raise ValueError("Query benchmark requires data. Run `rmbench insert` first.")

    # --- statements ------------------------------------------------------------

    def build_insert_statement(self, table_name: str, source: str) -> str:
        return statements.insert_statement(
            source, access_key=self._access_key, secret_key=self._secret_key
        )

    def build_delete_statement(
        self, *, table_name: str, sale_window: tuple[datetime, datetime], batch_timestamp: datetime
    ) -> str:
        return statements.delete_query(
            table_name=table_name, sale_window=sale_window, batch_start_timestamp=batch_timestamp
        )

    # --- probes ----------------------------------------------------------------

    def count_rows(self, conn: Client) -> dict[str, int]:
        return {
            table_name: int(conn.execute(f"SELECT count() FROM {table_name} FINAL")[0][0])
            for table_name in TABLE_NAMES
        }

    def physical_rows(self, conn: Client) -> dict[str, int]:
        counts: dict[str, int] = {}
        for table_name in TABLE_NAMES:
            rows = conn.execute(
                "SELECT sum(rows) FROM system.parts "
                "WHERE active AND database = %(database)s AND table = %(table)s",
                params={"database": DATABASE, "table": table_name},
            )
            counts[table_name] = 0 if rows[0][0] is None else int(rows[0][0])
        return counts

    def batch_counts(
        self, conn: Client, boundary: str, *, logical_view: bool
    ) -> dict[str, dict[str, int]]:
        clause = " FINAL" if logical_view else ""
        counts: dict[str, dict[str, int]] = {}
        for table_name in TABLE_NAMES:
            rows = conn.execute(
                f"""
                SELECT
                    count() AS total_count,
                    countIf(insertion_datetime >= toDateTime(%(batch_time)s, 'UTC')) AS current_batch_count,
                    countIf(insertion_datetime < toDateTime(%(batch_time)s, 'UTC')) AS previous_batch_count
                FROM {table_name}{clause}
                """,
                params={"batch_time": boundary},
            )
            total, current, previous = rows[0]
            counts[table_name] = {
                "total_count": int(total),
                "current_batch_count": int(current),
                "previous_batch_count": int(previous),
            }
        return counts

    def settle_physical(self, conn: Client) -> None:
        """Nothing to do; merges settle in the background."""

    def async_task_ids(self, conn: Client, table_name: str) -> tuple[str, ...]:
        rows = conn.execute(
            "SELECT mutation_id FROM system.mutations "
            "WHERE database = %(database)s AND table = %(table)s",
            params={"database": DATABASE, "table": table_name},
        )
        return tuple(str(row[0]) for row in rows)

    def async_task_states(
        self, conn: Client, ids_by_table: dict[str, tuple[str, ...]]
    ) -> dict[str, MutationState]:
        states: dict[str, MutationState] = {}
        for table_name, task_ids in ids_by_table.items():
            if not task_ids:
                states[table_name] = MutationState()
                continue
            rows = conn.execute(
                """
                SELECT mutation_id, is_done, parts_to_do, latest_fail_reason
                FROM system.mutations WHERE database = %(database)s AND table = %(table)s
                """,
                params={"database": DATABASE, "table": table_name},
            )
            selected = [row for row in rows if str(row[0]) in task_ids]
            found = tuple(str(row[0]) for row in selected)
            states[table_name] = MutationState(
                task_ids=task_ids,
                found_task_ids=found,
                is_done=len(found) == len(task_ids) and all(bool(row[1]) for row in selected),
                parts_to_do=sum(int(row[2]) for row in selected),
                failure_reason=next((str(row[3]) for row in selected if row[3]), None),
            )
        return states

    def progress_snapshot(self, conn: Client) -> tuple[Any, ...] | None:
        rows = conn.execute(
            """
            SELECT result_part_name, progress, rows_read, rows_written,
                   bytes_read_uncompressed, bytes_written_uncompressed
            FROM system.merges WHERE database = %(database)s
            """,
            params={"database": DATABASE},
        )
        return tuple(
            (str(row[0]), float(row[1]), int(row[2]), int(row[3]), int(row[4]), int(row[5]))
            for row in rows
        )

    def server_side_stats(self, query_ids: list[str], sql: str) -> dict[str, Any]:
        """Read back from the query log, matched on query_id. `sql` is unused."""
        if not query_ids:
            return {"logged_query_count": 0, "missing_query_count": 0}

        quoted = ", ".join(f"'{query_id}'" for query_id in query_ids)
        sql = (
            "SELECT query_duration_ms, read_rows, read_bytes, memory_usage "
            "FROM system.query_log "
            f"WHERE type = 'QueryFinish' AND query_id IN ({quoted})"
        )
        deadline = time.monotonic() + 5.0
        rows: list[tuple[int, int, int, int]] = []
        conn = self.connect()
        try:
            while time.monotonic() < deadline:
                conn.execute("SYSTEM FLUSH LOGS")
                rows = conn.execute(sql)
                if len(rows) >= len(query_ids):
                    break
                time.sleep(0.2)
        finally:
            self.close(conn)

        result: dict[str, Any] = {
            "logged_query_count": len(rows),
            "missing_query_count": max(0, len(query_ids) - len(rows)),
        }
        if rows:
            result["query_duration_ms"] = summarize_distribution([float(row[0]) for row in rows])
            result["read_rows"] = summarize_observed([int(row[1]) for row in rows])
            # uncompressed bytes scanned; not a compressed-size measure
            result["read_bytes"] = summarize_observed([int(row[2]) for row in rows])
            result["memory_usage_bytes"] = summarize_distribution([float(row[3]) for row in rows])
        return result

    def protocol_metadata(self, workload: str) -> dict[str, Any]:
        return {
            "insert": {
                "visibility_probe": "select_count_final",
                "physical_probe": "system.parts_active_rows",
            },
            "query_sequential": {
                "cold_method": "clickhouse_restart_per_query_after_startup_query",
                "warm_method": "clickhouse_restart_prime_same_query_once_then_measure",
                "hot_method": "single_restart_prime_same_query_then_measure_loop",
                "startup_query": self.warmup_query,
            },
            "query_concurrent": {"startup_query": self.warmup_query},
            "update": {
                "update_method": "insert_replacement_batch_then_async_delete_previous_batch",
                "physical_probe": "batch_visibility_counts_and_system.mutations",
                "timing_scope": "statement_submit_to_visibility_and_mutation_completion",
            },
        }[workload]

    # --- internals -------------------------------------------------------------

    def _compose(self, *args: str) -> None:
        if not COMPOSE_FILE.exists():
            raise ValueError(f"Missing {COMPOSE_FILE}. Run from the repository root.")
        subprocess.run(
            ["docker", "compose", "--env-file", str(write_compose_env()), "-f", str(COMPOSE_FILE), *args],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def _wait_ready(self) -> None:
        deadline = time.monotonic() + READY_TIMEOUT_SECONDS
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                with urlopen(f"http://{HOST}:8123/ping", timeout=1) as response:
                    if response.read() == b"Ok.\n":
                        return
            except Exception as exc:
                last_error = exc
                time.sleep(READY_POLL_INTERVAL_SECONDS)
                continue

            conn = self.connect()
            try:
                conn.execute(self.warmup_query)
                return
            except Exception as exc:
                last_error = exc
                time.sleep(READY_POLL_INTERVAL_SECONDS)
            finally:
                self.close(conn)
        raise TimeoutError("ClickHouse did not become ready after restart.") from last_error
