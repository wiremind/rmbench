from __future__ import annotations

from datetime import datetime

from rmbench.generation.schema import output_columns

ARRAY_DELIMITER = "#~#"
# read as text, then projected: the generator writes 1/0 and joined arrays
_READ_AS_TEXT = {"BOOLEAN", "VARCHAR[]"}


def insert_statement(table_name: str, source: str, declared: dict[str, str]) -> str:
    """Read the gz CSV from object storage and project it into the table."""
    csv_columns = output_columns(table_name)

    read_types = ", ".join(
        f"'{column}': '{'VARCHAR' if declared[column] in _READ_AS_TEXT else declared[column]}'"
        for column in csv_columns
    )

    projections = ["event_datetime AS time"]
    for column in csv_columns:
        duck_type = declared[column]
        if duck_type == "BOOLEAN":
            projections.append(f"TRY_CAST({column} AS BOOLEAN) AS {column}")
        elif duck_type == "VARCHAR[]":
            projections.append(
                f"CASE WHEN {column} IS NULL OR {column} = '' THEN []::VARCHAR[] "
                f"ELSE str_split({column}, '{ARRAY_DELIMITER}') END AS {column}"
            )
        else:
            projections.append(column)
    projections.append("now() AS insertion_datetime")

    target_columns = ["time", *csv_columns, "insertion_datetime"]
    return (
        f"INSERT INTO {table_name} ({', '.join(target_columns)})\n"
        "SELECT\n    " + ",\n    ".join(projections) + "\n"
        f"FROM read_csv('{source}', header = true, compression = 'gzip', "
        f"columns = {{{read_types}}})"
    )


def delete_statement(
    *, table_name: str, sale_window: tuple[datetime, datetime], batch_timestamp: datetime
) -> str:
    start, end = (value.strftime("%Y-%m-%d %H:%M:%S") for value in sale_window)
    batch_time = batch_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    return f"""
        DELETE FROM {table_name}
        WHERE event_datetime >= TIMESTAMP '{start}'
            AND event_datetime < TIMESTAMP '{end}'
            AND insertion_datetime < TIMESTAMP '{batch_time}'
    """
