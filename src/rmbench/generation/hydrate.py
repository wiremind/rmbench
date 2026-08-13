from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import math
import os
import shutil
from multiprocessing import get_context
from pathlib import Path
from uuid import uuid4

from rmbench.generation.formatters import NULL_TOKEN
from rmbench.generation.schema import TABLE_COLUMNS, output_columns
from rmbench.generation.writer import COMPRESSLEVEL
from rmbench.io_utils import (
    DATA_MANIFEST_FILENAME,
    INPUT_SIGNATURE_FILENAME,
    output_signature_matches,
    write_json_file,
)

UPDATE_MANIFEST_FILENAME = "update_manifest.json"

# Columns a replay may change: the measures a source system revises between
# snapshots. Keys, dates, labels and structure stay fixed, so a replay row
# still matches its baseline row on the storage key.
MUTABLE_METRIC_COLUMNS = {
    "fact_daily_od_bucket": frozenset({
        "service_budget_objective",
        "service_yield_objective",
        "market_cabin_max_leg_load_factor",
        "market_cabin_max_leg_cumulative_sum_net_bookings",
        "od_cabin_forecasted_traffic",
        "od_cabin_forecasted_revenue_vat_inc",
        "od_cabin_forecasted_revenue_vat_exc",
        "od_cabin_optimized_traffic",
        "od_cabin_optimized_revenue_vat_inc",
        "od_cabin_optimized_revenue_vat_exc",
        "od_cabin_last_predicted",
        "od_cabin_last_observed",
        "bucket_authorization_start_day",
        "bucket_authorization_end_day",
        "availability_start_day",
        "availability_end_day",
        "cumul_availability_start_day",
        "cumul_availability_end_day",
        "price_vat_inc",
        "cumulative_sum_net_bookings",
        "cumulative_sum_net_revenue_vat_inc",
        "cumulative_sum_net_revenue_vat_exc",
        "cumulative_sum_net_ancillary_revenue_vat_inc",
        "cumulative_sum_net_ancillary_revenue_vat_exc",
        "sum_confirmed_bookings",
        "sum_net_bookings",
        "sum_net_revenue_vat_inc",
        "sum_net_revenue_vat_exc",
        "sum_net_ancillary_revenue_vat_inc",
        "sum_net_ancillary_revenue_vat_exc",
        "unconstrained_demand_bookings",
        "unconstrained_demand_revenue",
        "unconstrained_forecast_bookings",
        "unconstrained_forecast_revenue",
    }),
    "fact_daily_leg_physical_inventory": frozenset({
        "physical_inventory_lid",
        "cumulative_sum_net_bookings",
        "cumulative_sum_net_revenue_vat_inc",
        "cumulative_sum_net_revenue_vat_exc",
        "cumulative_sum_net_ancillary_revenue_vat_inc",
        "cumulative_sum_net_ancillary_revenue_vat_exc",
        "sum_net_bookings",
        "sum_net_revenue_vat_inc",
        "sum_net_revenue_vat_exc",
        "sum_net_ancillary_revenue_vat_inc",
        "sum_net_ancillary_revenue_vat_exc",
        "unconstrained_demand_bookings",
        "unconstrained_forecast_bookings",
        "final_forecast_bookings",
    }),
    "fact_passenger_event": frozenset({
        "cabin_capacity",
        "cabin_lid",
        "bucket_price_vat_inc",
        "price_vat_inc",
        "price_vat_exc",
        "base_price_vat_inc",
        "ancillary_revenue_vat_inc",
        "ancillary_revenue_vat_exc",
        "no_show",
    }),
}

UPDATE_MUTABLE_COLUMNS = {
    table_name: tuple(
        column for column in output_columns(table_name) if column in MUTABLE_METRIC_COLUMNS[table_name]
    )
    for table_name in TABLE_COLUMNS
}

# (table_name, output_columns, input_path, output_part_path, chunk_idx,
#  byte_offset, byte_length, starting_row_number, has_header,
#  row_change_percent, field_change_percent)
HydrateWorkItem = tuple[str, tuple[str, ...], str, str, int, int, int, int, bool, float, float]


def change_tokens(row_change_percent: float, field_change_percent: float) -> str:
    return (
        f"rows{int(round(row_change_percent * 100))}bp"
        f"_fields{int(round(field_change_percent * 100))}bp"
    )


def hydrate_update_data(
    *,
    sf: int,
    row_change_percent: float,
    field_change_percent: float,
    input_dir: Path,
    output_dir: Path,
) -> tuple[Path, bool]:
    """Write a replay copy of the baseline with a deterministic subset of measures changed.

    One worker per gz member, so the parts can be built in parallel and then
    byte-concatenated back into a multi-member file.
    """
    if not input_dir.exists():
        raise ValueError(f"Missing {input_dir}. Generate the baseline scale factor first.")

    data_manifest_path = input_dir / DATA_MANIFEST_FILENAME
    if not data_manifest_path.exists():
        raise ValueError(
            f"Missing {data_manifest_path}. Hydrate dispatches one worker per gz member "
            f"and needs each member's byte range and global row-number offset."
        )
    data_manifest_bytes = data_manifest_path.read_bytes()
    data_manifest = json.loads(data_manifest_bytes)["tables"]

    signature = {
        "data_manifest_sha256": hashlib.sha256(data_manifest_bytes).hexdigest(),
        "row_change_percent": row_change_percent,
        "field_change_percent": field_change_percent,
    }
    manifest_out_path = output_dir / UPDATE_MANIFEST_FILENAME
    required = [manifest_out_path, *(output_dir / f"{t}.csv.gz" for t in TABLE_COLUMNS)]
    if output_signature_matches(output_dir, signature, required):
        return manifest_out_path, True

    output_dir.mkdir(parents=True, exist_ok=True)

    work_items: list[HydrateWorkItem] = []
    table_part_paths: dict[str, list[Path]] = {}
    for table_name in TABLE_COLUMNS:
        members = data_manifest.get(table_name)
        if not members:
            raise ValueError(f"Data manifest has no entry for {table_name!r}.")
        columns = tuple(output_columns(table_name))
        cumulative_rows = 0
        parts: list[Path] = []
        for member in sorted(members, key=lambda m: m["chunk_idx"]):
            chunk_idx = member["chunk_idx"]
            starting_row_number = cumulative_rows + 1
            cumulative_rows += member["row_count"]
            part_path = output_dir / f"{table_name}.part_{chunk_idx:03d}.csv.gz"
            parts.append(part_path)
            work_items.append((
                table_name,
                columns,
                str(input_dir / f"{table_name}.csv.gz"),
                str(part_path),
                chunk_idx,
                member["byte_offset"],
                member["byte_length"],
                starting_row_number,
                chunk_idx == 0,
                row_change_percent,
                field_change_percent,
            ))
        table_part_paths[table_name] = parts

    chunk_stats: dict[tuple[str, int], dict[str, int]] = {}
    ctx = get_context("spawn")
    with ctx.Pool(processes=min(len(work_items), os.cpu_count() or 1)) as pool:
        for table_name, chunk_idx, stats in pool.imap_unordered(_hydrate_member_worker, work_items):
            chunk_stats[(table_name, chunk_idx)] = stats

    for table_name, parts in table_part_paths.items():
        with open(output_dir / f"{table_name}.csv.gz", "wb") as final_file:
            for part_path in parts:
                with open(part_path, "rb") as part_file:
                    shutil.copyfileobj(part_file, final_file)
        for part_path in parts:
            part_path.unlink()

    aggregated = {
        table_name: {"row_count": 0, "changed_row_count": 0, "changed_field_count": 0}
        for table_name in TABLE_COLUMNS
    }
    for (table_name, _), stats in chunk_stats.items():
        for key in aggregated[table_name]:
            aggregated[table_name][key] += stats[key]

    write_json_file(
        manifest_out_path,
        {
            "run_id": f"update-hydrate-{uuid4().hex[:12]}",
            "scale_factor": sf,
            "row_change_percent": row_change_percent,
            "field_change_percent": field_change_percent,
            "tables": aggregated,
        },
        sort_keys=False,
    )
    write_json_file(output_dir / INPUT_SIGNATURE_FILENAME, signature)
    return manifest_out_path, False


def replay_table_counts(manifest_path: Path) -> dict[str, int]:
    tables = json.loads(manifest_path.read_text())["tables"]
    return {table_name: int(tables[table_name]["row_count"]) for table_name in TABLE_COLUMNS}


def _hydrate_member_worker(args: HydrateWorkItem) -> tuple[str, int, dict[str, int]]:
    (
        table_name,
        columns,
        input_path_str,
        output_part_path_str,
        chunk_idx,
        byte_offset,
        byte_length,
        starting_row_number,
        has_header,
        row_change_percent,
        field_change_percent,
    ) = args
    mutable_columns = UPDATE_MUTABLE_COLUMNS[table_name]

    with open(input_path_str, "rb") as src_file:
        src_file.seek(byte_offset)
        member_bytes = src_file.read(byte_length)

    row_count = 0
    changed_row_count = 0
    changed_field_count = 0

    with (
        gzip.open(io.BytesIO(member_bytes), "rt", encoding="utf-8", newline="") as source_file,
        open(output_part_path_str, "wb") as raw_file,
    ):
        # mtime=0 and a fixed member name, matching the generator, so a re-hydrate
        # of the same input is byte-identical
        gz_file = gzip.GzipFile(
            fileobj=raw_file,
            mode="wb",
            compresslevel=COMPRESSLEVEL,
            filename=f"{table_name}.csv",
            mtime=0,
        )
        target_file = io.TextIOWrapper(gz_file, encoding="utf-8", newline="")
        if has_header:
            reader = csv.DictReader(source_file)
            if tuple(reader.fieldnames or ()) != columns:
                raise ValueError(
                    f"Unexpected header in {input_path_str} member {chunk_idx}: {reader.fieldnames!r}"
                )
        else:
            reader = csv.DictReader(source_file, fieldnames=columns)

        writer = csv.writer(target_file)
        if has_header:
            writer.writerow(columns)

        for offset, row in enumerate(reader):
            row_count += 1
            row_key = _row_key(row, columns, row_number=starting_row_number + offset)

            if _stable_percent(row_key, field="row") < row_change_percent:
                changed_columns = 0
                for column in _select_mutated_columns(
                    row=row,
                    row_key=row_key,
                    columns=mutable_columns,
                    field_change_percent=field_change_percent,
                ):
                    mutated = _mutate_value(column=column, value=row[column], row_key=row_key)
                    if mutated != row[column]:
                        row[column] = mutated
                        changed_columns += 1
                if changed_columns:
                    changed_row_count += 1
                    changed_field_count += changed_columns

            writer.writerow([row[column] for column in columns])

        target_file.close()

    return table_name, chunk_idx, {
        "row_count": row_count,
        "changed_row_count": changed_row_count,
        "changed_field_count": changed_field_count,
    }


def _select_mutated_columns(
    *, row: dict[str, str], row_key: str, columns: tuple[str, ...], field_change_percent: float
) -> list[str]:
    candidates = [column for column in columns if row[column] != NULL_TOKEN]
    if not candidates or field_change_percent <= 0:
        return []
    target_count = max(1, math.ceil(len(candidates) * field_change_percent / 100.0))
    # ranked by a per-row hash so the same input row always mutates the same subset
    ranked = sorted(candidates, key=lambda column: _stable_percent(row_key, field=f"column:{column}"))
    return ranked[:target_count]


def _mutate_value(*, column: str, value: str, row_key: str) -> str:
    if value == NULL_TOKEN:
        return value
    if column == "no_show":
        return "0" if value == "1" else "1"

    if "." in value:
        original = float(value)
        # deterministic scale inside a +/-15% band
        factor = 0.85 + (_stable_percent(row_key, field=f"decimal:{column}") / 100.0) * 0.30
        mutated = round(original * factor, 2)
        # rounding can collapse back onto the original for small numbers
        if mutated == original:
            mutated += 0.01 if original >= 0 else -0.01
        return f"{mutated:.2f}"

    original = int(value)
    factor = 0.85 + (_stable_percent(row_key, field=f"int:{column}") / 100.0) * 0.30
    mutated = int(round(original * factor))
    if mutated == original:
        direction = 1 if _stable_percent(row_key, field=f"direction:{column}") >= 50.0 else -1
        mutated = 1 if original == 0 else original + direction * max(1, math.ceil(abs(original) * 0.05))
    return str(mutated)


def _row_key(row: dict[str, str], columns: tuple[str, ...], *, row_number: int) -> str:
    payload = "|".join(row.get(column, "") for column in columns) + f"|{row_number}"
    return hashlib.blake2b(payload.encode(), digest_size=16).hexdigest()


def _stable_percent(row_key: str, *, field: str) -> float:
    digest = hashlib.blake2b(f"{row_key}|{field}".encode(), digest_size=8).digest()
    return (int.from_bytes(digest, "big") / 2**64) * 100.0
