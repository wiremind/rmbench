from __future__ import annotations

from collections.abc import Hashable
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from rmbench.generation.schema import TABLE_COLUMNS

TABLE_NAMES: tuple[str, ...] = tuple(TABLE_COLUMNS)


class AsyncTaskState(Protocol):
    is_done: bool
    failure_reason: str | None


class Engine(Protocol):
    """What the shared insert/query/update workloads need from a database."""

    name: str
    query_dir: Path
    warmup_query: str

    physical_count_scope: str

    deletes_are_async: bool
    """Whether a delete spawns background work to wait for."""

    logical_view_deduplicates: bool
    """Whether the replay's insert phase shows the replay alone or both batches."""

    def source_root(self, prefix: str) -> str:
        """Where this engine reads an uploaded prefix from."""

    def connect(self) -> Any: ...
    def close(self, conn: Any) -> None: ...
    def execute(self, conn: Any, sql: str, *, query_id: str | None = None) -> None: ...
    def ensure_up(self) -> None: ...

    def restart(self) -> None:
        """Drop the engine's caches. This is what makes a run cold."""

    def assert_source_ready(self, conn: Any) -> None: ...
    def build_insert_statement(self, table_name: str, source: str) -> str: ...

    def build_delete_statement(
        self, *, table_name: str, sale_window: tuple[datetime, datetime], batch_timestamp: datetime
    ) -> str: ...

    def count_rows(self, conn: Any) -> dict[str, int]:
        """Rows a query would see."""

    def physical_rows(self, conn: Any) -> dict[str, int]:
        """Rows settled in storage. Same as `count_rows` when there is no separate stage."""

    def batch_counts(
        self, conn: Any, boundary: str, *, logical_view: bool
    ) -> dict[str, dict[str, int]]:
        """Per table: total, current and previous batch counts, split on
        `insertion_datetime` against `boundary`."""

    def settle_physical(self, conn: Any) -> None:
        """Flush pending writes. Does nothing on engines that flush in the
        background as those report through `async_task_states`."""

    def async_task_ids(self, conn: Any, table_name: str) -> tuple[str, ...]: ...

    def async_task_states(
        self, conn: Any, ids_by_table: dict[str, tuple[str, ...]]
    ) -> dict[str, AsyncTaskState]: ...

    def progress_snapshot(self, conn: Any) -> Hashable | None:
        """Anything that changes while work is happening."""

    def server_side_stats(self, query_ids: list[str], sql: str) -> dict[str, Any]:
        """The engine's own numbers for these executions."""

    def protocol_metadata(self, workload: str) -> dict[str, Any]:
        """How this engine measured the workload. Written into the result envelope."""
