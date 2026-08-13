from __future__ import annotations

from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config

from rmbench.workload.engine import TABLE_NAMES

MIGRATIONS_DIR = Path("duckdb") / "migrations"


def upgrade_to_head(database_path: Path) -> None:
    """Migrate one database file to the latest revision."""
    config = Config()
    config.set_main_option("script_location", str(MIGRATIONS_DIR))
    config.set_main_option("sqlalchemy.url", f"duckdb:///{database_path}")
    command.upgrade(config, "head")


def table_columns(conn: Any) -> dict[str, list[tuple[str, str]]]:
    """Per table: (column, DuckDB type) in ordinal order."""
    columns = {}
    for table_name in TABLE_NAMES:
        rows = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ? ORDER BY ordinal_position",
            [table_name],
        ).fetchall()
        if not rows:
            raise ValueError(f"{table_name} does not exist. Migrations did not run.")
        columns[table_name] = [(str(name), str(dtype)) for name, dtype in rows]
    return columns

