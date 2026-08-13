from __future__ import annotations

import csv
import gzip
import io
from collections.abc import Iterable
from pathlib import Path
from typing import Any

COMPRESSLEVEL = 6


class _MemberWriter:
    """One RFC 1952 gz member; hydrate-update-data reads members in parallel."""

    def __init__(self, raw: io.BufferedWriter) -> None:
        # mtime=0 keeps output byte-deterministic for a given seed
        self._gz = gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=COMPRESSLEVEL, mtime=0)
        self._text = io.TextIOWrapper(self._gz, encoding="utf-8", newline="")
        self.csv_writer = csv.writer(self._text)
        self.row_count = 0

    def close(self) -> None:
        # closes the gz member too, but not the file it is being appended to
        self._text.close()


def write_csv_gz(
    *,
    path: Path,
    columns: list[str],
    chunks: Iterable[list[list[Any]]],
    member_target_rows: int,
) -> list[dict[str, int]]:
    """Write a multi-member csv.gz and return the data-manifest member list."""
    members: list[dict[str, int]] = []
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as raw:
        member_start = 0
        member = _MemberWriter(raw)
        member.csv_writer.writerow(columns)

        def finish_member() -> None:
            nonlocal member_start
            member.close()
            end = raw.tell()
            members.append(
                {
                    "chunk_idx": len(members),
                    "byte_offset": member_start,
                    "byte_length": end - member_start,
                    "row_count": member.row_count,
                }
            )
            member_start = end

        for chunk in chunks:
            if member.row_count >= member_target_rows:
                finish_member()
                member = _MemberWriter(raw)
            member.csv_writer.writerows(chunk)
            member.row_count += len(chunk)
        finish_member()
    return members


def write_gz_member(
    *,
    path: Path,
    columns: list[str],
    chunks: Iterable[list[list[Any]]],
    header: bool,
) -> int:
    """Write one gz member to its own file and return the data row count.

    Used by the parallel path: each slice becomes one member, and the parent
    byte-concatenates the parts (RFC 1952 allows it).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as raw:
        member = _MemberWriter(raw)
        if header:
            member.csv_writer.writerow(columns)
        for chunk in chunks:
            member.csv_writer.writerows(chunk)
            member.row_count += len(chunk)
        rows = member.row_count
        member.close()
    return rows
