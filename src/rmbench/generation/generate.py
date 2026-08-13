from __future__ import annotations

import hashlib
import os
import shutil
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from multiprocessing import get_context
from pathlib import Path
from typing import Any

import numpy as np

from rmbench.generation.bundle import Bundle, load_bundle
from rmbench.generation.cardinality import (
    resolve_bundle_cardinality,
    resolve_entity_pool_sizes,
)
from rmbench.generation.constraints import apply_constraints
from rmbench.generation.formatters import (
    format_array,
    format_bool,
    format_date,
    format_datetime_ms,
    format_decimal,
    format_integer,
    format_string,
)
from rmbench.generation.name_mapping import NameMapping, build_name_mapping
from rmbench.generation.sampling import (
    CategoricalSampler,
    build_cabin_subset_fn,
    build_categorical_sampler,
    build_family_of_bucket,
    build_route_market_subsets,
    get_metric_grid,
    resolve_scale_mean,
    sample_bounded_metric,
    sample_lognormal_metric,
    sample_markets_within_routes,
    snap_to_grid,
)
from rmbench.generation.schema import (
    KIND_ARRAY,
    KIND_BOOL,
    KIND_DATE,
    KIND_DATETIME_MS,
    KIND_DECIMAL,
    KIND_INTEGER,
    KIND_STRING,
    SOURCE_BOOL_RATE,
    SOURCE_CATEGORY,
    SOURCE_DAY_X,
    SOURCE_DERIVED_IS_LAST,
    SOURCE_DERIVED_LEG_ARRIVAL_DATETIME,
    SOURCE_DERIVED_LEG_DEPARTURE_DATETIME,
    SOURCE_DERIVED_METRIC,
    SOURCE_DERIVED_SERVICE_ARRIVAL_DATETIME,
    SOURCE_DERIVED_SERVICE_DEPARTURE_DATETIME,
    SOURCE_EMPTY,
    SOURCE_EVENT_DATETIME,
    SOURCE_FIXED,
    SOURCE_METRIC,
    SOURCE_SERVICE_DEPARTURE_DATE,
    SOURCE_SYNTH_ARRAY,
    SOURCE_SYNTH_ID,
    SOURCE_SYNTH_TOKEN,
    TABLE_COLUMNS,
    TABLE_SORT_KEYS,
    ColumnSpec,
    output_columns,
)
from rmbench.generation.spec import (
    load_constraints,
    load_entity_hierarchy,
    load_fanout,
    load_generator_config,
)
from rmbench.generation.time_generation import (
    SnapshotSlice,
    iter_snapshot_slices,
)
from rmbench.generation.writer import write_csv_gz, write_gz_member
from rmbench.io_utils import DATA_MANIFEST_FILENAME, write_json_file

# tuning constants -> spec/generator.yaml. Domain facts (hierarchy, fanout,
# constraint rules) -> spec/constraints.yaml
CONFIG = load_generator_config()
CONSTRAINTS = load_constraints()
ENTITY_HIERARCHY = load_entity_hierarchy()
TABLE_FANOUT = load_fanout()
DEFAULT_CHUNK_SIZE = CONFIG.row_scaling.default_chunk_size

# Peak generation memory is workers x (entity pools + one slice)
POOL_BYTES_PER_CELL = 60
SLICE_BYTES_PER_CELL = 20
MEMORY_HEADROOM = 0.8
DEFAULT_SEED = CONFIG.row_scaling.default_seed

_FORMATTER_BY_KIND = {
    KIND_DECIMAL: format_decimal,
    KIND_INTEGER: format_integer,
    KIND_BOOL: format_bool,
    KIND_STRING: format_string,
    KIND_ARRAY: format_array,
    KIND_DATE: format_date,
    KIND_DATETIME_MS: format_datetime_ms,
}

_ENTITY_POOLABLE_SOURCES = (
    SOURCE_SYNTH_ID,
    SOURCE_SYNTH_TOKEN,
    SOURCE_SYNTH_ARRAY,
    SOURCE_CATEGORY,
    SOURCE_METRIC,
    SOURCE_DERIVED_METRIC,
)


def generate_public_data(
    *,
    sale_start: date,
    departure_start: date,
    sale_days: int,
    departure_days: int,
    bundle_path: Path,
    output_dir: Path,
    seed: int = DEFAULT_SEED,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, dict[str, Any]]:
    if sale_days < 1 or departure_days < 1:
        raise ValueError(
            f"sale_days and departure_days must be >= 1, got {sale_days=}, {departure_days=}"
        )
    bundle = load_bundle(bundle_path)
    # every calibrated distinct count ships as a two-anchor pair; resolve them to
    # this window's values up front so all downstream samplers read plain ints
    _resolve_cardinality(bundle=bundle, sale_days=sale_days, departure_days=departure_days)
    name_mapping = build_name_mapping(bundle, CONFIG.literal_values)
    rng = np.random.default_rng(seed)
    # one shared pool so entity identities overlap across tables; with the
    # ablation off each table builds its own
    entity_pools = (
        _build_shared_entity_pools(bundle=bundle, name_mapping=name_mapping, rng=rng)
        if CONFIG.ablation.shared_entity_pools
        else None
    )

    slices = list(
        iter_snapshot_slices(
            sale_start=sale_start,
            departure_start=departure_start,
            sale_days=sale_days,
            departure_days=departure_days,
        )
    )

    # One worker per snapshot slice. Requires the shared pool, since the
    # per-table ablation off-path threads a single rng across tables and cannot be
    # reproduced independently per slice.
    parallel = (
        len(slices) > 1
        and (os.cpu_count() or 1) > 1
        and entity_pools is not None
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    summary: dict[str, dict[str, Any]] = {}
    manifest_tables: dict[str, list[dict[str, int]]] = {}

    table_rows = [
        (
            table_index,
            table_name,
            int(round(CONFIG.row_scaling.sf1_od_row_count
                      * bundle.per_table_row_count_ratio(table_name))),
        )
        for table_index, table_name in enumerate(TABLE_COLUMNS)
    ]

    if parallel:
        manifest_tables = _generate_tables_parallel(
            bundle_path=bundle_path,
            tables=table_rows,
            slices=slices,
            chunk_size=chunk_size,
            sale_days=sale_days,
            departure_days=departure_days,
            seed=seed,
            output_dir=output_dir,
        )
    else:
        for table_index, table_name, per_slice_rows in table_rows:
            manifest_tables[table_name] = _generate_table(
                bundle=bundle,
                name_mapping=name_mapping,
                table_name=table_name,
                slices=slices,
                per_slice_rows=per_slice_rows,
                chunk_size=chunk_size,
                rng=rng,
                output_path=output_dir / f"{table_name}.csv.gz",
                entity_pools=entity_pools,
                seed=seed,
                table_index=table_index,
                departure_days=departure_days,
            )

    for _, table_name, per_slice_rows in table_rows:
        members = manifest_tables[table_name]
        summary[table_name] = {
            "path": str(output_dir / f"{table_name}.csv.gz"),
            "rows_per_snapshot": per_slice_rows,
            "snapshot_count": len(slices),
            "rows_written": sum(member["row_count"] for member in members),
        }

    write_json_file(output_dir / DATA_MANIFEST_FILENAME, {"tables": manifest_tables}, sort_keys=False)

    return summary


@dataclass(frozen=True)
class _TableContext:
    """Everything a table's slices share. Slice-independent, so a worker can
    rebuild it once and reuse it for every slice it is handed."""

    table_name: str
    columns: list[str]
    specs: tuple[ColumnSpec, ...]
    per_slice_rows: int
    entity_pools: dict[str, dict[str, Any]]
    category_samplers: dict[str, CategoricalSampler]
    metric_profiles: dict[str, Any]
    array_profiles: dict[str, Any]
    bool_profiles: dict[str, Any]
    numeric_profiles: dict[str, Any]
    fanout_config: dict[str, Any] | None
    fanout_active: bool
    family_of_bucket: dict[str, str] | None
    subsets_by_primary: list[tuple[str, ...]] | None
    ladder_depth_hist: dict[str, float] | None
    departure_days: int
    band_primary: bool


def _build_table_context(
    *,
    bundle: Bundle,
    name_mapping: NameMapping,
    table_name: str,
    per_slice_rows: int,
    entity_pools: dict[str, dict[str, Any]] | None,
    seed: int,
    table_index: int,
    departure_days: int,
    rng: np.random.Generator | None = None,
) -> _TableContext:
    """Build the slice-independent state for one table.

    ``rng`` is consumed only when ``entity_pools`` is None (the shared-pool
    ablation off-path builds per-table pools); otherwise this is deterministic.
    """
    specs = TABLE_COLUMNS[table_name]
    table_data = bundle.table(table_name)
    category_samplers: dict[str, CategoricalSampler] = {}
    for spec in specs:
        if spec.source != SOURCE_CATEGORY:
            continue
        cat_col = spec.name
        category_samplers[cat_col] = build_categorical_sampler(
            column=cat_col,
            category_distribution=_find_category_distribution(bundle, table_name, cat_col),
            name_mapping=name_mapping,
        )

    metric_profiles = table_data["metric_profiles"]
    array_profiles = table_data["array_profiles"]
    bool_profiles = table_data["bool_profiles"]
    numeric_profiles = table_data["numeric_profiles"]

    # shared_entity_pools off: build per-table pools from this table's own
    # distributions, dropping cross-table entity overlap
    if entity_pools is None:
        if rng is None:
            raise ValueError("per-table entity pools need an rng")
        entity_pools = _build_entity_pools(
            specs=specs,
            rng=rng,
            category_samplers=category_samplers,
            metric_profiles=metric_profiles,
            array_profiles=array_profiles,
            numeric_profiles=numeric_profiles,
        )

    fanout_config = TABLE_FANOUT.get(table_name)
    fanout_active = fanout_config is not None and fanout_config["primary"] in entity_pools

    family_of_bucket: dict[str, str] | None = None
    subsets_by_primary: list[tuple[str, ...]] | None = None
    ladder_depth_hist: dict[str, float] | None = None
    if fanout_active:
        fanout_cols = set(fanout_config["fan_out"])
        pair_structure = table_data.get("pair_structure") or {}
        if CONFIG.ablation.family_of_bucket and {"family_name", "bucket_name"} <= fanout_cols:
            bucket_sampler = category_samplers.get("bucket_name")
            family_sampler = category_samplers.get("family_name")
            if bucket_sampler is not None and family_sampler is not None:
                family_of_bucket = build_family_of_bucket(
                    bucket_sampler=bucket_sampler, family_sampler=family_sampler
                )
        cabins_per_market = pair_structure.get("cabins_per_market")
        cabin_sampler = category_samplers.get("cabin_name")
        primary_markets = entity_pools[fanout_config["primary"]].get("market_name")
        if (
            CONFIG.ablation.cabin_subset
            and cabins_per_market
            and "cabin_name" in fanout_cols
            and cabin_sampler is not None
            and primary_markets is not None
        ):
            subset_fn = build_cabin_subset_fn(
                cabin_sampler=cabin_sampler, size_histogram=cabins_per_market
            )
            # Route-scoped: the cabin set belongs to the route's rolling stock,
            # and a service runs exactly one route, so all ods of a service share
            # a cabin set.
            primary_entity_name = fanout_config["primary"]
            parent_name = ENTITY_HIERARCHY.get(primary_entity_name)
            parent_idx_of_primary = (
                entity_pools[primary_entity_name].get(f"_parent_{parent_name}_idx")
                if parent_name
                else None
            )
            parent_routes = (
                entity_pools[parent_name].get("route_name") if parent_name else None
            )
            if parent_idx_of_primary is not None and parent_routes is not None:
                subsets_by_primary = [
                    subset_fn(str(parent_routes[int(pi)])) for pi in parent_idx_of_primary
                ]
            else:
                subsets_by_primary = [subset_fn(str(m)) for m in primary_markets]
        # calibrated per-od ladder depth (rows per od-cabin from the bundle histogram)
        ladder_depth_hist = pair_structure.get("ladder_depth")

    # Draw the metric grids here, from their own stream and in a fixed column order,
    # so every worker builds identical grids regardless of which slice runs first.
    grid_rng = np.random.default_rng(np.random.SeedSequence(seed, spawn_key=(0, table_index)))
    for profiles in (metric_profiles, numeric_profiles):
        for column in sorted(profiles):
            profile = profiles[column]
            if isinstance(profile, dict):
                get_metric_grid(profile, grid_rng)

    return _TableContext(
        table_name=table_name,
        columns=output_columns(table_name),
        specs=specs,
        per_slice_rows=per_slice_rows,
        entity_pools=entity_pools,
        category_samplers=category_samplers,
        metric_profiles=metric_profiles,
        array_profiles=array_profiles,
        bool_profiles=bool_profiles,
        numeric_profiles=numeric_profiles,
        fanout_config=fanout_config,
        fanout_active=fanout_active,
        family_of_bucket=family_of_bucket,
        subsets_by_primary=subsets_by_primary,
        ladder_depth_hist=ladder_depth_hist,
        departure_days=departure_days,
        band_primary=(
            fanout_active
            and fanout_config is not None
            and fanout_config["primary"] in CONFIG.entity_pools.departure_linear
        ),
    )


def _slice_chunks(
    ctx: _TableContext,
    snapshot: SnapshotSlice,
    rng: np.random.Generator,
    chunk_size: int,
) -> Iterator[list[list[Any]]]:
    """Formatted row chunks for one snapshot slice."""
    if ctx.fanout_active:
        primary_entity = ctx.fanout_config["primary"]
        fan_out_cols = tuple(ctx.fanout_config["fan_out"])
        parent_entity = ENTITY_HIERARCHY.get(primary_entity)
        parent_indices = (
            ctx.entity_pools[primary_entity].get(f"_parent_{parent_entity}_idx")
            if parent_entity
            else None
        )
        primary_pool_size = int(ctx.entity_pools[primary_entity]["_size"])
        fanout_tuples, primary_indices, sub_indices = _build_fanout_tuples(
            fan_out_cols=fan_out_cols,
            primary_pool_size=primary_pool_size,
            primary_subset=_departure_band(
                pool_size=primary_pool_size,
                departure_days=ctx.departure_days,
                departure_date=snapshot.departure_date,
            )
            if ctx.band_primary
            else None,
            total_rows=ctx.per_slice_rows,
            category_samplers=ctx.category_samplers,
            rng=rng,
            parent_indices=parent_indices,
            family_of_bucket=ctx.family_of_bucket,
            subsets_by_primary=ctx.subsets_by_primary,
            ladder_depth_hist=ctx.ladder_depth_hist,
        )
        column_arrays, row_order = _build_slice_columns(
            table_name=ctx.table_name,
            specs=ctx.specs,
            category_samplers=ctx.category_samplers,
            metric_profiles=ctx.metric_profiles,
            array_profiles=ctx.array_profiles,
            bool_profiles=ctx.bool_profiles,
            numeric_profiles=ctx.numeric_profiles,
            n=ctx.per_slice_rows,
            rng=rng,
            snapshot=snapshot,
            entity_pools=ctx.entity_pools,
            departure_days=ctx.departure_days,
            primary_entity=primary_entity,
            primary_indices=primary_indices,
            sub_indices=sub_indices,
            fan_out_cols=fan_out_cols,
            fanout_tuples=fanout_tuples,
            per_rung_cols=ctx.fanout_config["per_rung"],
        )
    else:
        column_arrays, row_order = _build_slice_columns(
            table_name=ctx.table_name,
            specs=ctx.specs,
            category_samplers=ctx.category_samplers,
            metric_profiles=ctx.metric_profiles,
            array_profiles=ctx.array_profiles,
            bool_profiles=ctx.bool_profiles,
            numeric_profiles=ctx.numeric_profiles,
            n=ctx.per_slice_rows,
            rng=rng,
            snapshot=snapshot,
            entity_pools=ctx.entity_pools,
            departure_days=ctx.departure_days,
        )
    yield from _emit_slice_rows(ctx.specs, column_arrays, row_order, chunk_size)


def _slice_rng(seed: int, slice_idx: int) -> np.random.Generator:
    """Independent stream per slice, keyed on the slice INDEX."""
    return np.random.default_rng(np.random.SeedSequence(seed, spawn_key=(1, slice_idx)))


# Cached per worker process. Every worker rebuilds the same bundle, name mapping
# and entity pools (the pools from the seed alone) so they are built in-process.
_WORKER_STATE: dict[str, Any] = {}


def _worker_state(bundle_path: str, sale_days: int, departure_days: int, seed: int) -> dict[str, Any]:
    state = _WORKER_STATE.get("state")
    if state is None:
        bundle = load_bundle(Path(bundle_path))
        _resolve_cardinality(bundle=bundle, sale_days=sale_days, departure_days=departure_days)
        name_mapping = build_name_mapping(bundle, CONFIG.literal_values)
        # same fresh generator the serial path uses before any slice work, so the
        # pools (and therefore every entity identity) are bit-identical
        pools = _build_shared_entity_pools(
            bundle=bundle, name_mapping=name_mapping, rng=np.random.default_rng(seed)
        )
        state = {"bundle": bundle, "name_mapping": name_mapping,
                 "pools": pools, "contexts": {}}
        _WORKER_STATE["state"] = state
    return state


def _generate_slice_part(item: tuple[Any, ...]) -> tuple[str, int, int]:
    """Generate one snapshot slice into its own gz member. Runs in a worker."""
    (bundle_path, table_name, slice_idx, snapshot, per_slice_rows,
     chunk_size, sale_days, departure_days, seed, out_dir, table_index) = item
    state = _worker_state(bundle_path, sale_days, departure_days, seed)
    ctx = state["contexts"].get(table_name)
    if ctx is None:
        ctx = _build_table_context(
            bundle=state["bundle"],
            name_mapping=state["name_mapping"],
            table_name=table_name,
            per_slice_rows=per_slice_rows,
            entity_pools=state["pools"],
            seed=seed,
            table_index=table_index,
            departure_days=departure_days,
        )
        state["contexts"][table_name] = ctx
    rng = _slice_rng(seed, slice_idx)
    rows = write_gz_member(
        path=Path(out_dir) / f"{table_name}.part_{slice_idx:05d}.csv.gz",
        columns=ctx.columns,
        chunks=_slice_chunks(ctx, snapshot, rng, chunk_size),
        header=slice_idx == 0,
    )
    return table_name, slice_idx, rows


def _available_memory_bytes() -> int:
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")


def _worker_count(*, items: int, tables: list[tuple[int, str, int]]) -> int:
    """Workers that fit in RAM, since each holds the entity pools plus one slice."""
    limit = min(items, os.cpu_count() or 1)
    override = os.environ.get("RMBENCH_MAX_WORKERS")
    if override:
        return max(1, min(limit, int(override)))
    # each (column, entity) pair is one column of that entity's pool
    pool_cells = sum(
        _ENTITY_POOL_SIZES[entity]
        for _, entity in {
            (spec.name, spec.options["entity"])
            for specs in TABLE_COLUMNS.values()
            for spec in specs
            if "entity" in spec.options
        }
        if entity in _ENTITY_POOL_SIZES
    )
    slice_cells = max(rows * len(output_columns(name)) for _, name, rows in tables)
    per_worker = pool_cells * POOL_BYTES_PER_CELL + slice_cells * SLICE_BYTES_PER_CELL
    return max(1, min(limit, int(_available_memory_bytes() * MEMORY_HEADROOM // per_worker)))


def _generate_tables_parallel(
    *,
    bundle_path: Path,
    tables: list[tuple[int, str, int]],
    slices: list[SnapshotSlice],
    chunk_size: int,
    sale_days: int,
    departure_days: int,
    seed: int,
    output_dir: Path,
) -> dict[str, list[dict[str, int]]]:
    """One worker per (table, slice), across a single pool.

    All tables share one pool so each worker process builds the entity pools once
    and amortises that over many slices.
    """
    items = [
        (str(bundle_path), table_name, idx, snapshot, per_slice_rows,
         chunk_size, sale_days, departure_days, seed, str(output_dir), table_index)
        for table_index, table_name, per_slice_rows in tables
        for idx, snapshot in enumerate(slices)
    ]
    row_counts: dict[tuple[str, int], int] = {}
    workers = _worker_count(items=len(items), tables=tables)
    print(f"Generating with {workers} of {os.cpu_count()} cores "
          f"({_available_memory_bytes() / 1e9:.0f} GB available)")
    ctx = get_context("spawn")
    with ctx.Pool(processes=workers) as pool:
        for table_name, slice_idx, rows in pool.imap_unordered(_generate_slice_part, items):
            row_counts[(table_name, slice_idx)] = rows

    # concatenated in SLICE order, not completion order, so the output does not
    # depend on how many workers ran
    manifest: dict[str, list[dict[str, int]]] = {}
    for _, table_name, _ in tables:
        members: list[dict[str, int]] = []
        byte_offset = 0
        output_path = output_dir / f"{table_name}.csv.gz"
        with open(output_path, "wb") as final_file:
            for slice_idx in range(len(slices)):
                part_path = output_dir / f"{table_name}.part_{slice_idx:05d}.csv.gz"
                byte_length = part_path.stat().st_size
                with open(part_path, "rb") as part:
                    shutil.copyfileobj(part, final_file)
                members.append(
                    {
                        "chunk_idx": slice_idx,
                        "byte_offset": byte_offset,
                        "byte_length": byte_length,
                        "row_count": row_counts[(table_name, slice_idx)],
                    }
                )
                byte_offset += byte_length
                part_path.unlink()
        manifest[table_name] = members
    return manifest


def _generate_table(
    *,
    bundle: Bundle,
    name_mapping: NameMapping,
    table_name: str,
    slices: list[SnapshotSlice],
    per_slice_rows: int,
    chunk_size: int,
    rng: np.random.Generator,
    output_path: Path,
    entity_pools: dict[str, dict[str, Any]] | None,
    seed: int,
    table_index: int,
    departure_days: int,
) -> list[dict[str, int]]:
    columns = output_columns(table_name)
    ctx = _build_table_context(
        bundle=bundle,
        name_mapping=name_mapping,
        table_name=table_name,
        per_slice_rows=per_slice_rows,
        entity_pools=entity_pools,
        seed=seed,
        table_index=table_index,
        departure_days=departure_days,
        rng=rng,
    )

    def chunks() -> Iterator[list[list[Any]]]:
        for snapshot in slices:
            yield from _slice_chunks(ctx, snapshot, rng, chunk_size)

    return write_csv_gz(
        path=output_path,
        columns=columns,
        chunks=chunks(),
        member_target_rows=CONFIG.row_scaling.member_target_rows,
    )


def _parent_route_names(
    *,
    pools: dict[str, dict[str, Any]],
    entity: str,
    parent_indices: np.ndarray | None,
) -> list[str] | None:
    """route_name of each pool member's parent, or None if the parent has no route."""
    if parent_indices is None:
        return None
    parent = ENTITY_HIERARCHY.get(entity)
    routes = pools[parent].get("route_name") if parent else None
    if routes is None:
        return None
    return [str(routes[int(i)]) for i in parent_indices]


_ENTITY_POOL_SIZES: dict[str, int] = {}


def _resolve_cardinality(*, bundle: Bundle, sale_days: int, departure_days: int) -> None:
    """Collapse the bundle's two-anchor cardinality onto this window."""
    anchors = bundle.cardinality_anchors
    sf = float(sale_days * departure_days)
    resolve_bundle_cardinality(tables=bundle.tables, anchors=anchors, sf=sf)
    _ENTITY_POOL_SIZES.clear()
    _ENTITY_POOL_SIZES.update(
        resolve_entity_pool_sizes(
            entity_cardinality=bundle.entity_cardinality,
            anchors=anchors,
            sf=sf,
            departure_days=departure_days,
            departure_linear=CONFIG.entity_pools.departure_linear,
        )
    )


def _build_shared_entity_pools(
    *,
    bundle: Bundle,
    name_mapping: NameMapping,
    rng: np.random.Generator,
) -> dict[str, dict[str, Any]]:
    """Build one entity pool set from the union of every table's entity-bound specs."""
    union_specs: list[ColumnSpec] = []
    seen: set[tuple[str, str]] = set()
    for table_name in TABLE_COLUMNS:
        for spec in TABLE_COLUMNS[table_name]:
            entity = spec.options.get("entity")
            if spec.source not in _ENTITY_POOLABLE_SOURCES or not entity:
                continue
            key = (entity, spec.name)
            if key in seen:
                continue
            seen.add(key)
            union_specs.append(spec)

    primary_table = next(iter(TABLE_COLUMNS))
    category_samplers: dict[str, CategoricalSampler] = {}
    for spec in union_specs:
        if spec.source != SOURCE_CATEGORY:
            continue
        cat_col = spec.name
        source_distribution = _find_category_distribution(bundle, primary_table, cat_col)
        if source_distribution is None:
            continue
        category_samplers[cat_col] = build_categorical_sampler(
            column=cat_col,
            category_distribution=source_distribution,
            name_mapping=name_mapping,
        )

    metric_profiles: dict[str, dict[str, Any]] = {}
    array_profiles: dict[str, dict[str, Any]] = {}
    numeric_profiles: dict[str, dict[str, Any]] = {}
    for table_name in TABLE_COLUMNS:
        table_data = bundle.table(table_name)
        for target, section in (
            (metric_profiles, "metric_profiles"),
            (array_profiles, "array_profiles"),
            (numeric_profiles, "numeric_profiles"),
        ):
            for col, profile in table_data[section].items():
                target.setdefault(col, profile)

    return _build_entity_pools(
        specs=tuple(union_specs),
        rng=rng,
        category_samplers=category_samplers,
        metric_profiles=metric_profiles,
        array_profiles=array_profiles,
        numeric_profiles=numeric_profiles,
    )


def _build_entity_pools(
    *,
    specs: tuple[ColumnSpec, ...],
    rng: np.random.Generator,
    category_samplers: dict[str, CategoricalSampler],
    metric_profiles: dict[str, dict[str, Any]],
    array_profiles: dict[str, dict[str, Any]],
    numeric_profiles: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    entities: dict[str, list[ColumnSpec]] = {}
    for spec in specs:
        if spec.source not in _ENTITY_POOLABLE_SOURCES:
            continue
        entity = spec.options.get("entity")
        if not entity:
            continue
        entities.setdefault(entity, []).append(spec)

    # Every table gets all three pools, so the hierarchy is the same whether the
    # specs came from one table or from the union of all of them.
    for implied in ("service", "od", "leg"):
        entities.setdefault(implied, [])

    # parent assignment happens before column draws so parent-aware columns
    # (clustered ids, parent-scoped tokens) can reference it during the build
    pool_sizes = {entity: _ENTITY_POOL_SIZES[entity] for entity in entities}
    parent_idx_arrays: dict[str, np.ndarray] = {}
    for child, parent in ENTITY_HIERARCHY.items():
        parent_idx_arrays[child] = rng.integers(
            low=0, high=pool_sizes[parent], size=pool_sizes[child],
        )

    # markets are route-scoped, so a child's market draw needs its parent's route
    # already built -- visit parents before children
    def _depth(entity: str) -> int:
        depth, cursor = 0, entity
        while cursor in ENTITY_HIERARCHY:
            cursor = ENTITY_HIERARCHY[cursor]
            depth += 1
        return depth

    route_market_subsets: dict[str, tuple[str, ...]] = {}
    if CONFIG.ablation.market_of_route:
        route_sampler = category_samplers.get("route_name")
        market_sampler = category_samplers.get("market_name")
        if route_sampler is not None and market_sampler is not None:
            route_market_subsets = build_route_market_subsets(
                route_sampler=route_sampler, market_sampler=market_sampler
            )

    pools: dict[str, dict[str, Any]] = {}
    for entity, entity_specs in sorted(entities.items(), key=lambda kv: _depth(kv[0])):
        size = pool_sizes[entity]
        columns: dict[str, Any] = {"_size": size}
        parent_indices = parent_idx_arrays.get(entity)
        parent_size = (
            pool_sizes[ENTITY_HIERARCHY[entity]] if parent_indices is not None else 0
        )
        parent_token_subsets: dict[tuple[str, int], np.ndarray] = {}
        deferred_derived: list[ColumnSpec] = []
        deferred_id_maps: list[ColumnSpec] = []
        for spec in entity_specs:
            if spec.source == SOURCE_CATEGORY:
                cat_col = spec.name
                sampler = category_samplers[cat_col]
                parent_routes = _parent_route_names(
                    pools=pools, entity=entity, parent_indices=parent_indices
                )
                if (
                    cat_col == "market_name"
                    and route_market_subsets
                    and parent_routes is not None
                ):
                    # confine each parent's ods to its route's markets, so a
                    # service spans a handful of markets instead of the whole pool
                    columns[spec.name] = sample_markets_within_routes(
                        market_sampler=sampler,
                        route_market_subsets=route_market_subsets,
                        parent_routes=parent_routes,
                        rng=rng,
                    )
                else:
                    columns[spec.name] = list(sampler.sample(size, rng))
            elif spec.source == SOURCE_METRIC:
                columns[spec.name] = _sample_metric_values(spec, size, rng, metric_profiles)
            elif spec.source == SOURCE_DERIVED_METRIC:
                deferred_derived.append(spec)
            elif spec.source == SOURCE_SYNTH_ID and spec.options.get("derive_from"):
                deferred_id_maps.append(spec)
            elif (
                spec.source == SOURCE_SYNTH_ID
                and spec.options.get("cluster_by_parent")
                and parent_indices is not None
            ):
                columns[spec.name] = _clustered_child_ids(
                    low=int(spec.options.get("low", 1)),
                    high=int(spec.options.get("high", 10_000_000)),
                    parent_indices=parent_indices,
                    parent_size=parent_size,
                    rng=rng,
                )
            elif (
                spec.source == SOURCE_SYNTH_ID
                and spec.options.get("ordinal_within_parent")
                and parent_indices is not None
            ):
                # a position within the parent, not a draw: 1..n over the
                # parent's children, so it is constant per child
                seen_per_parent: dict[int, int] = {}
                ordinals: list[int] = []
                for parent_idx in parent_indices:
                    nxt = seen_per_parent.get(int(parent_idx), 0) + 1
                    seen_per_parent[int(parent_idx)] = nxt
                    ordinals.append(nxt)
                columns[spec.name] = ordinals
            elif (
                spec.source == SOURCE_SYNTH_TOKEN
                and spec.options.get("scope_by_parent")
                and parent_indices is not None
            ):
                columns[spec.name] = _parent_scoped_tokens(
                    spec=spec,
                    parent_indices=parent_indices,
                    parent_size=parent_size,
                    subsets_cache=parent_token_subsets,
                    rng=rng,
                )
            elif spec.source == SOURCE_SYNTH_TOKEN and spec.options.get("unique_per_member"):
                # one distinct token per member -> distinct count == pool size,
                # which is SF-scaled (identity keys like ticket_key)
                prefix = spec.options["prefix"]
                columns[spec.name] = [f"{prefix}_{i:07d}" for i in range(size)]
            else:
                columns[spec.name] = _draw_synth_values(
                    spec=spec,
                    n=size,
                    rng=rng,
                    array_profiles=array_profiles,
                    numeric_profiles=numeric_profiles,
                )
        for spec in deferred_id_maps:
            source_col = spec.options["derive_from"]
            if source_col not in columns:
                raise ValueError(
                    f"Entity {entity!r} pool: {spec.name} derives from {source_col!r} "
                    "which is not bound to the same entity"
                )
            low = int(spec.options.get("low", 1))
            high = int(spec.options.get("high", 10_000_000))
            source_values = columns[source_col]
            subdivide = spec.options.get("subdivide")
            if subdivide:
                # refine each source value into k distinct ids, k sized so the
                # total distinct count matches the named pool key
                n_source = len(set(source_values))
                target = _ENTITY_POOL_SIZES[subdivide]
                k = round(target / n_source)
                subs = rng.integers(low=0, high=k, size=len(source_values))
                columns[spec.name] = [
                    _stable_id(f"{value}#{int(sub)}", low=low, high=high)
                    for value, sub in zip(source_values, subs)
                ]
            else:
                columns[spec.name] = [
                    _stable_id(str(value), low=low, high=high) for value in source_values
                ]
        while deferred_derived:
            made_progress = False
            still_deferred: list[ColumnSpec] = []
            for spec in deferred_derived:
                if spec.options["anchor"] not in columns:
                    still_deferred.append(spec)
                    continue
                columns[spec.name] = _generate_derived_metric_values(
                    spec=spec,
                    n=size,
                    rng=rng,
                    metric_profiles=metric_profiles,
                    column_arrays=columns,
                )
                made_progress = True
            if not made_progress:
                missing = [s.name for s in still_deferred]
                raise ValueError(
                    f"Entity {entity!r} pool: derived anchors unresolved: {missing}"
                )
            deferred_derived = still_deferred
        # Only service and leg carry a departure time, so only their pools need
        # the intra-day offset and a duration to derive an arrival from.
        if entity in ("service", "leg"):
            columns["_intra_day_seconds"] = rng.integers(
                low=CONFIG.durations.intra_day.low_seconds,
                high=CONFIG.durations.intra_day.high_seconds,
                size=size,
            )
        if entity == "service":
            columns["_service_duration_seconds"] = rng.integers(
                low=CONFIG.durations.service.min_seconds,
                high=CONFIG.durations.service.max_seconds,
                size=size,
            )
        if entity == "leg":
            columns["_leg_duration_seconds"] = rng.integers(
                low=CONFIG.durations.leg.min_seconds,
                high=CONFIG.durations.leg.max_seconds,
                size=size,
            )
        pools[entity] = columns

    for child, parent_indices in parent_idx_arrays.items():
        pools[child][f"_parent_{ENTITY_HIERARCHY[child]}_idx"] = parent_indices
    return pools


# Children of one parent take ids a fixed stride apart, so od_id is a
# constant-delta sequence in sort order and compresses well. Prime, so the
# modulo below cannot land two children on one id.
CHILD_ID_STRIDE = 71

# How many of its parent's tokens a child draws from, e.g. how many stations the
# ODs of one service span. Fabricated, not calibrated.
PARENT_TOKEN_SUBSET_SIZE = 8


def _clustered_child_ids(
    *,
    low: int,
    high: int,
    parent_indices: np.ndarray,
    parent_size: int,
    rng: np.random.Generator,
) -> list[int]:
    n = len(parent_indices)
    span = (high - low) // parent_size
    bases = low + rng.permutation(parent_size).astype(np.int64) * span
    ranks = np.zeros(n, dtype=np.int64)
    counts: dict[int, int] = {}
    for i, parent in enumerate(parent_indices):
        p = int(parent)
        ranks[i] = counts.get(p, 0)
        counts[p] = ranks[i] + 1
    # modulo keeps every id inside its parent's band even for large groups
    offsets = (ranks * CHILD_ID_STRIDE) % span
    return [int(v) for v in bases[parent_indices] + offsets]


def _parent_scoped_tokens(
    *,
    spec: ColumnSpec,
    parent_indices: np.ndarray,
    parent_size: int,
    subsets_cache: dict[tuple[str, int], np.ndarray],
    rng: np.random.Generator,
) -> list[str]:
    prefix = spec.options["prefix"]
    ordinals = int(spec.options.get("ordinals", 200))
    cache_key = (prefix, ordinals)
    if cache_key not in subsets_cache:
        # one subset per parent, shared by every scoped column with the same
        # token space (od origin + destination draw from the same route stations)
        subsets_cache[cache_key] = rng.integers(
            low=1, high=ordinals, size=(parent_size, PARENT_TOKEN_SUBSET_SIZE),
        )
    subsets = subsets_cache[cache_key]
    picks = subsets[parent_indices, rng.integers(0, PARENT_TOKEN_SUBSET_SIZE, size=len(parent_indices))]
    return [f"{prefix}_{int(pick):04d}" for pick in picks]


def _stable_id(value: str, *, low: int, high: int) -> int:
    # deterministic 1:1 mapping (up to negligible hash collisions) so an id
    # column always agrees with its source name column across runs and rows
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest()
    return low + int.from_bytes(digest, "big") % (high - low)


def _draw_synth_values(
    *,
    spec: ColumnSpec,
    n: int,
    rng: np.random.Generator,
    array_profiles: dict[str, dict[str, Any]],
    numeric_profiles: dict[str, dict[str, Any]],
) -> list[Any]:
    source = spec.source
    if source == SOURCE_SYNTH_ID:
        if spec.options.get("derive_from"):
            raise ValueError(
                f"{spec.name}: derive_from requires the column to be entity-pooled"
            )
        low = int(spec.options.get("low", 1))
        high = int(spec.options.get("high", 10_000_000))
        # Snap to distinct-many evenly spaced steps. The calibrated distinct count
        # wins over the declared range: where it is the wider of the two, ids run
        # past `high` rather than collapsing onto fewer values than measured.
        profile = numeric_profiles.get(spec.name)
        distinct = int(profile["distinct_count"]) if profile and profile.get("distinct_count") else 0
        if distinct > 0:
            effective_range = max(distinct, high - low)
            step = effective_range / distinct
            idx = rng.integers(low=0, high=distinct, size=n)
            return [int(round(low + step * int(i))) for i in idx]
        return [int(v) for v in rng.integers(low=low, high=high, size=n)]
    if source == SOURCE_SYNTH_TOKEN:
        prefix = spec.options["prefix"]
        ordinals = int(spec.options.get("ordinals", 200))
        picks = rng.integers(low=1, high=ordinals, size=n)
        return [f"{prefix}_{int(p):04d}" for p in picks]
    if source == SOURCE_SYNTH_ARRAY:
        prefix = spec.options["prefix"]
        min_items = int(spec.options.get("min_items", 0))
        max_items = int(spec.options.get("max_items", 3))
        item_ordinals = int(spec.options.get("item_ordinals", 30))
        # A null row and an empty one are different things: the bundle's null_rate
        # decides which rows are null, so every non-null row gets at least one item.
        null_rate = float(array_profiles[spec.name]["null_rate"])
        null_mask = rng.uniform(size=n) < null_rate
        lengths = rng.integers(low=max(min_items, 1), high=max_items + 1, size=n)
        result: list[Any] = []
        for i, length in enumerate(lengths):
            if null_mask[i]:
                result.append(None)
                continue
            picks = rng.integers(low=1, high=item_ordinals, size=int(length))
            result.append([f"{prefix}_{int(p):04d}" for p in picks])
        return result
    raise ValueError(f"_draw_synth_values does not support source {source!r}")


def _pad_tuples(
    tuples: list[tuple[str, ...]],
    target: int,
    seen: set[tuple[str, ...]],
    fallback: tuple[str, ...],
) -> None:
    """Extend ``tuples`` to ``target`` by suffixing the last column.

    Only once every calibrated combination is used up.
    """
    pad_idx = 0
    while len(tuples) < target:
        base_tup = tuples[pad_idx % len(tuples)] if tuples else fallback
        padded = (*base_tup[:-1], f"{base_tup[-1]}__PAD_{pad_idx:06d}")
        if padded not in seen:
            seen.add(padded)
            tuples.append(padded)
        pad_idx += 1


def _draw_tuple_ladder(
    *,
    fan_out_cols: tuple[str, ...],
    target: int,
    category_samplers: dict[str, CategoricalSampler],
    rng: np.random.Generator,
    allowed_cabins: tuple[str, ...] | None = None,
    family_of_bucket: dict[str, str] | None = None,
    seen: set[tuple[str, ...]] | None = None,
    tuples: list[tuple[str, ...]] | None = None,
    pad: bool = True,
) -> list[tuple[str, ...]]:
    """Draw ``target`` unique fan-out tuples.

    ``allowed_cabins`` limits cabin_name. ``family_of_bucket`` takes family_name
    from the bucket instead of drawing it.
    """
    seen = set() if seen is None else seen
    tuples = [] if tuples is None else tuples
    # 4x oversampling so the dedup pass usually fills the target in one shot.
    # The additive floor matters for small targets, where 4x is too few draws to
    # likely find that many distinct tuples and the shortfall would be padded.
    oversample = max(target * 4, target + 20)
    cols_samples: dict[str, np.ndarray] = {}
    for col in fan_out_cols:
        if col == "family_name" and family_of_bucket is not None:
            continue
        sampler = category_samplers[col]
        if col == "cabin_name" and allowed_cabins is not None:
            mask = np.isin(sampler.names, np.array(allowed_cabins, dtype=object))
            probs = sampler.probabilities[mask]
            cols_samples[col] = rng.choice(
                sampler.names[mask], size=oversample, p=probs / probs.sum()
            )
        else:
            cols_samples[col] = sampler.sample(oversample, rng)

    if family_of_bucket is not None:
        cols_samples["family_name"] = np.array(
            [family_of_bucket[str(b)] for b in cols_samples["bucket_name"]], dtype=object,
        )

    for i in range(oversample):
        tup = tuple(cols_samples[col][i] for col in fan_out_cols)
        if tup not in seen:
            seen.add(tup)
            tuples.append(tup)
            if len(tuples) >= target:
                break

    if pad:
        _pad_tuples(
            tuples, target, seen, tuple(f"{col.upper()}_0001" for col in fan_out_cols)
        )
    return tuples


def _departure_band(*, pool_size: int, departure_days: int, departure_date: date) -> np.ndarray:
    """The slice of a departure-linear pool belonging to one departure date."""
    band_count = max(1, min(departure_days, pool_size))
    band = departure_date.toordinal() % band_count
    return np.array_split(np.arange(pool_size, dtype=np.int64), band_count)[band]


def _build_fanout_tuples(
    *,
    fan_out_cols: tuple[str, ...],
    primary_pool_size: int,
    total_rows: int,
    primary_subset: np.ndarray | None = None,
    category_samplers: dict[str, CategoricalSampler],
    rng: np.random.Generator,
    parent_indices: np.ndarray | None = None,
    family_of_bucket: dict[str, str] | None = None,
    subsets_by_primary: list[tuple[str, ...]] | None = None,
    ladder_depth_hist: dict[str, float] | None = None,
) -> tuple[list[list[tuple[str, ...]]], np.ndarray, np.ndarray]:
    """Draw fan-out tuples per primary entity and assign every row to one.

    Returns the tuple lists plus each row's (primary_idx, sub_idx), in output
    order. One ladder per parent, shared by its children."""
    if total_rows == 0 or primary_pool_size == 0:
        return [], np.zeros(0, dtype=np.int64), np.zeros(0, dtype=np.int64)

    usable = (
        range(primary_pool_size) if primary_subset is None else [int(i) for i in primary_subset]
    )
    if parent_indices is not None:
        children_by_parent: dict[int, list[int]] = {}
        for child_idx in usable:
            children_by_parent.setdefault(int(parent_indices[child_idx]), []).append(child_idx)
        groups = list(children_by_parent.values())
    else:
        groups = [[entity_idx] for entity_idx in usable]

    if ladder_depth_hist:
        # One depth per parent, shared by every child
        depth_values = np.array([int(k) for k in ladder_depth_hist], dtype=np.float64)
        depth_probs = np.array([float(v) for v in ladder_depth_hist.values()], dtype=np.float64)
        depth_probs = depth_probs / depth_probs.sum()
        depths = rng.choice(depth_values, size=len(groups), p=depth_probs)
        n_cabins = np.array(
            [len(subsets_by_primary[g[0]]) if subsets_by_primary else 1 for g in groups],
            dtype=np.float64,
        )
        group_sizes = np.array([len(g) for g in groups], dtype=np.float64)
        weights = depths * n_cabins
        scale = total_rows / float((group_sizes * weights).sum())
        rungs = np.maximum(np.floor(weights * scale), 1.0).astype(np.int64)
        emitted = int((group_sizes * rungs).sum())
        # give a whole extra rung to the parents that lost the most to flooring,
        # as long as it fits
        for gi in np.argsort(-(weights * scale - rungs)):
            cost = int(group_sizes[gi])
            if emitted + cost > total_rows:
                continue
            rungs[gi] += 1
            emitted += cost
            if emitted == total_rows:
                break
        targets = [0] * primary_pool_size
        for gi, children in enumerate(groups):
            for child_idx in children:
                targets[child_idx] = int(rungs[gi])
        # residual < one parent's child count
        flat = [child_idx for children in groups for child_idx in children]
        for offset in range(total_rows - sum(targets)):
            targets[flat[offset % len(flat)]] += 1
    else:
        # ascending child index, so an unbanded pool splits exactly as before
        flat = sorted(child_idx for children in groups for child_idx in children)
        base, remainder = divmod(total_rows, len(flat))
        targets = [0] * primary_pool_size
        for rank, child_idx in enumerate(flat):
            targets[child_idx] = base + (1 if rank < remainder else 0)

    per_entity_tuples: list[list[tuple[str, ...]]] = [[] for _ in range(primary_pool_size)]
    for children in groups:
        ladder_target = max(targets[child_idx] for child_idx in children)
        if ladder_target == 0:
            continue
        if subsets_by_primary is None:
            ladder = _draw_tuple_ladder(
                fan_out_cols=fan_out_cols,
                target=ladder_target,
                category_samplers=category_samplers,
                rng=rng,
                family_of_bucket=family_of_bucket,
            )
            for child_idx in children:
                per_entity_tuples[child_idx] = ladder[: targets[child_idx]]
            continue

        # draw one shared ladder over the union of the children's cabin subsets,
        # then filter each child's view to its own cabins
        union_cabins = tuple(sorted(set().union(*(subsets_by_primary[c] for c in children))))
        cabin_idx = fan_out_cols.index("cabin_name")
        seen: set[tuple[str, ...]] = set()
        ladder: list[tuple[str, ...]] = []
        for round_no in range(8):
            need = 0
            for child_idx in children:
                have = sum(1 for t in ladder if t[cabin_idx] in subsets_by_primary[child_idx])
                need = max(need, targets[child_idx] - have)
            if need <= 0:
                break
            _draw_tuple_ladder(
                fan_out_cols=fan_out_cols,
                target=len(ladder) + max(need * 2, 8),
                category_samplers=category_samplers,
                rng=rng,
                allowed_cabins=union_cabins,
                family_of_bucket=family_of_bucket,
                seen=seen,
                tuples=ladder,
                pad=round_no == 7,
            )
        for child_idx in children:
            subset = subsets_by_primary[child_idx]
            filtered = [t for t in ladder if t[cabin_idx] in subset][: targets[child_idx]]
            _pad_tuples(
                filtered,
                targets[child_idx],
                seen,
                tuple(
                    subset[0] if i == cabin_idx else f"{c.upper()}_0001"
                    for i, c in enumerate(fan_out_cols)
                ),
            )
            per_entity_tuples[child_idx] = filtered

    primary_indices = np.empty(total_rows, dtype=np.int64)
    sub_indices = np.empty(total_rows, dtype=np.int64)
    ptr = 0
    for entity_idx, target in enumerate(targets):
        if target == 0:
            continue
        primary_indices[ptr : ptr + target] = entity_idx
        sub_indices[ptr : ptr + target] = np.arange(target)
        ptr += target

    if ptr != total_rows:
        raise ValueError(f"fan-out emitted {ptr} rows, expected {total_rows}")
    return per_entity_tuples, primary_indices, sub_indices


def _build_slice_columns(
    *,
    table_name: str,
    specs: tuple[ColumnSpec, ...],
    category_samplers: dict[str, CategoricalSampler],
    metric_profiles: dict[str, dict[str, Any]],
    array_profiles: dict[str, dict[str, Any]],
    bool_profiles: dict[str, dict[str, Any]],
    numeric_profiles: dict[str, dict[str, Any]],
    n: int,
    rng: np.random.Generator,
    snapshot: SnapshotSlice,
    entity_pools: dict[str, dict[str, list[Any]]],
    departure_days: int,
    primary_entity: str | None = None,
    primary_indices: np.ndarray | None = None,
    sub_indices: np.ndarray | None = None,
    fan_out_cols: tuple[str, ...] = (),
    fanout_tuples: list[list[tuple[str, ...]]] | None = None,
    per_rung_cols: tuple[str, ...] = (),
) -> tuple[dict[str, list[Any]], np.ndarray]:
    # slice-wide generation lets constraints see cross-chunk groups (e.g. a
    # service whose rows straddle chunk boundaries) -> required for pointers
    use_fanout = (
        fanout_tuples is not None
        and primary_indices is not None
        and sub_indices is not None
    )

    # per-rung columns are drawn once per (parent, ladder position) and shared
    # by every child of that parent
    parent_row: np.ndarray | None = None
    max_rung = 0
    parent_pool_size = 0
    if CONFIG.ablation.per_rung and use_fanout and per_rung_cols and primary_entity is not None:
        parent_entity = ENTITY_HIERARCHY.get(primary_entity)
        if parent_entity:
            parent_of_primary = entity_pools[primary_entity][f"_parent_{parent_entity}_idx"]
            parent_row = np.asarray(parent_of_primary)[primary_indices]
            max_rung = int(sub_indices.max()) + 1
            parent_pool_size = int(entity_pools[parent_entity]["_size"])
    per_rung_set = set(per_rung_cols) if parent_row is not None else set()

    # Only entities this table binds a column to; iterating the pools rather than
    # the set keeps the draw order, and so the output, reproducible.
    active_entities = {
        spec.options.get("entity") for spec in specs if spec.options.get("entity")
    }
    entity_indices: dict[str, np.ndarray] = {}
    for entity in entity_pools:
        if entity not in active_entities:
            continue
        if entity == primary_entity:
            entity_indices[entity] = primary_indices
            continue
        pool_size = int(entity_pools[entity]["_size"])
        if entity in CONFIG.entity_pools.departure_linear:
            band = _departure_band(
                pool_size=pool_size,
                departure_days=departure_days,
                departure_date=snapshot.departure_date,
            )
            entity_indices[entity] = band[rng.integers(low=0, high=len(band), size=n)]
        else:
            entity_indices[entity] = rng.integers(low=0, high=pool_size, size=n)
    # a child's parent is indexed through the child, so the two agree per row
    for child, parent in ENTITY_HIERARCHY.items():
        if child in entity_indices and parent in entity_indices:
            entity_indices[parent] = entity_pools[child][f"_parent_{parent}_idx"][entity_indices[child]]

    fan_out_idx_map = {col: i for i, col in enumerate(fan_out_cols)}

    column_arrays: dict[str, list[Any]] = {}
    row_level_id_derives = [
        spec
        for spec in specs
        if spec.source == SOURCE_SYNTH_ID
        and spec.options.get("derive_from")
        and not spec.options.get("entity")
    ]
    row_level_id_derive_names = {spec.name for spec in row_level_id_derives}
    # sample independent columns
    for spec in specs:
        if spec.source == SOURCE_DERIVED_METRIC:
            continue
        if spec.name in row_level_id_derive_names:
            continue
        if use_fanout and spec.name in fan_out_idx_map:
            field_idx = fan_out_idx_map[spec.name]
            column_arrays[spec.name] = [
                fanout_tuples[int(primary_indices[i])][int(sub_indices[i])][field_idx]
                for i in range(n)
            ]
            continue
        if spec.name in per_rung_set:
            # empty entity context: per-rung columns must be row-level draws
            rung_values = _generate_column_values(
                spec=spec,
                n=parent_pool_size * max_rung,
                rng=rng,
                snapshot=snapshot,
                category_samplers=category_samplers,
                metric_profiles=metric_profiles,
                array_profiles=array_profiles,
                bool_profiles=bool_profiles,
                numeric_profiles=numeric_profiles,
                entity_pools={},
                entity_indices={},
            )
            column_arrays[spec.name] = [
                rung_values[int(parent_row[i]) * max_rung + int(sub_indices[i])]
                for i in range(n)
            ]
            continue
        column_arrays[spec.name] = _generate_column_values(
            spec=spec,
            n=n,
            rng=rng,
            snapshot=snapshot,
            category_samplers=category_samplers,
            metric_profiles=metric_profiles,
            array_profiles=array_profiles,
            bool_profiles=bool_profiles,
            numeric_profiles=numeric_profiles,
            entity_pools=entity_pools,
            entity_indices=entity_indices,
        )

    for spec in row_level_id_derives:
        source_values = column_arrays[spec.options["derive_from"]]
        low = int(spec.options.get("low", 1))
        high = int(spec.options.get("high", 10_000_000))
        id_map = {value: _stable_id(str(value), low=low, high=high) for value in set(source_values)}
        column_arrays[spec.name] = [id_map[value] for value in source_values]

    # derived metrics (anchor x ratio). Iterate to resolve chains
    # where one derived column anchors on another
    derived_specs = [spec for spec in specs if spec.source == SOURCE_DERIVED_METRIC]
    while derived_specs:
        made_progress = False
        deferred: list[ColumnSpec] = []
        for spec in derived_specs:
            entity = spec.options.get("entity")
            if entity and entity in entity_pools and spec.name in entity_pools[entity]:
                pool_values = entity_pools[entity][spec.name]
                idx = entity_indices[entity]
                column_arrays[spec.name] = [pool_values[int(i)] for i in idx]
                made_progress = True
                continue
            anchor_name = spec.options["anchor"]
            if anchor_name not in column_arrays:
                deferred.append(spec)
                continue
            column_arrays[spec.name] = _generate_derived_metric_values(
                spec=spec,
                n=n,
                rng=rng,
                metric_profiles=metric_profiles,
                column_arrays=column_arrays,
            )
            made_progress = True
        if not made_progress:
            missing = [spec.name for spec in deferred]
            raise ValueError(f"Derived metric anchors unresolved: {missing}")
        derived_specs = deferred

    # declarative constraints from constraints.yaml
    if CONFIG.ablation.constraints:
        apply_constraints(
            table_name=table_name,
            column_arrays=column_arrays,
            n=n,
            constraints=CONSTRAINTS,
            rng=rng,
        )

    if table_name == "fact_passenger_event" and CONFIG.ablation.pax_dedupe:
        _assign_unique_pax_row_keys(
            column_arrays=column_arrays,
            booking_pool=entity_pools["booking"],
            n=n,
            rng=rng,
        )

    return column_arrays, _compute_row_order(table_name, column_arrays, n)


# Columns of the passenger-event storage key that vary within a slice; the rest
# of the key is slice-constant. Each source row is a distinct ticket event, so
# the combination has to be unique -> see _pad_tuples for what a repeat costs.
_PAX_ROW_KEY_COLUMNS = (
    "event_type",
    "service_number",
    "od_id",
    "cabin_name",
    "family_name",
    "ticket_key",
)


def _assign_unique_pax_row_keys(
    *,
    column_arrays: dict[str, list[Any]],
    booking_pool: dict[str, Any],
    n: int,
    rng: np.random.Generator,
) -> None:
    """Move colliding rows onto a different booking so the storage key stays unique.

    There are more rows than bookings, so ticket_key repeats on its own.
    """
    prefix_columns = [column_arrays[name] for name in _PAX_ROW_KEY_COLUMNS[:-1]]
    groups: dict[tuple[Any, ...], list[int]] = {}
    for i in range(n):
        groups.setdefault(tuple(column[i] for column in prefix_columns), []).append(i)

    identity_columns = [
        column for column in booking_pool
        if not column.startswith("_") and column in column_arrays
    ]
    pool_size = int(booking_pool["_size"])
    for rows in groups.values():
        if len(rows) == 1:
            continue
        for row, pick in zip(rows, rng.choice(pool_size, size=len(rows), replace=False), strict=True):
            for column in identity_columns:
                column_arrays[column][row] = booking_pool[column][int(pick)]


def _emit_slice_rows(
    specs: tuple[ColumnSpec, ...],
    column_arrays: dict[str, list[Any]],
    row_order: np.ndarray,
    chunk_size: int,
) -> Iterator[list[list[Any]]]:
    # streams to the writer in chunks so we don't hold every formatted string
    # for a whole slice in memory at once
    columns = [(column_arrays[spec.name], _FORMATTER_BY_KIND[spec.kind]) for spec in specs]
    order = row_order.tolist()
    for chunk_start in range(0, len(order), chunk_size):
        yield [
            [format_value(values[index]) for values, format_value in columns]
            for index in order[chunk_start : chunk_start + chunk_size]
        ]


def _compute_row_order(
    table_name: str,
    column_arrays: dict[str, list[Any]],
    n: int,
) -> np.ndarray:
    """Emit rows in the table's storage-key order."""
    key_columns = [c for c in TABLE_SORT_KEYS.get(table_name, ()) if c in column_arrays]
    if not key_columns:
        return np.arange(n)
    # lexsort takes the LAST key as primary, so reverse; strings are factorised to
    # rank codes, which preserves ordering for the ASCII tokens the generator emits
    sort_keys: list[np.ndarray] = []
    for column in reversed(key_columns):
        values = np.asarray(column_arrays[column][:n], dtype=object)
        _, codes = np.unique(values, return_inverse=True)
        sort_keys.append(codes)
    return np.lexsort(sort_keys)


def _generate_column_values(
    *,
    spec: ColumnSpec,
    n: int,
    rng: np.random.Generator,
    snapshot: SnapshotSlice,
    category_samplers: dict[str, CategoricalSampler],
    metric_profiles: dict[str, dict[str, Any]],
    array_profiles: dict[str, dict[str, Any]],
    bool_profiles: dict[str, dict[str, Any]],
    numeric_profiles: dict[str, dict[str, Any]],
    entity_pools: dict[str, dict[str, list[Any]]],
    entity_indices: dict[str, np.ndarray],
) -> list[Any]:
    source = spec.source
    if source == SOURCE_SERVICE_DEPARTURE_DATE:
        return [snapshot.departure_date] * n
    if source == SOURCE_DAY_X:
        return [snapshot.day_x] * n
    if source == SOURCE_DERIVED_IS_LAST:
        # the last snapshot for a service is the one taken after departure
        return [snapshot.day_x > 0] * n
    if source == SOURCE_EVENT_DATETIME:
        if spec.options.get("spread"):
            # per-row timestamps across the sale day, at seconds resolution
            # (sub-second is truncated on output)
            base = datetime.combine(snapshot.sale_date, datetime.min.time())
            secs = rng.integers(low=0, high=22 * 3600 + 59 * 60 + 59, size=n)
            return [base + timedelta(seconds=int(s)) for s in secs]
        return [snapshot.event_datetime] * n
    if source == SOURCE_DERIVED_SERVICE_DEPARTURE_DATETIME:
        return _entity_departure_datetimes(snapshot, "service", entity_pools, entity_indices)
    if source == SOURCE_DERIVED_SERVICE_ARRIVAL_DATETIME:
        return _entity_arrival_datetimes(snapshot, "service", entity_pools, entity_indices)
    if source == SOURCE_DERIVED_LEG_DEPARTURE_DATETIME:
        return _entity_departure_datetimes(snapshot, "leg", entity_pools, entity_indices)
    if source == SOURCE_DERIVED_LEG_ARRIVAL_DATETIME:
        return _entity_arrival_datetimes(
            snapshot, "leg", entity_pools, entity_indices, key="_leg_duration_seconds"
        )
    # entity-bound columns take their value from the shared pool, whatever the
    # source would otherwise draw
    if source in _ENTITY_POOLABLE_SOURCES:
        entity = spec.options.get("entity")
        # an empty pool context forces a row-level draw, which is how per-rung
        # columns are generated
        if entity and entity in entity_pools:
            pool_values = entity_pools[entity][spec.name]
            return [pool_values[int(i)] for i in entity_indices[entity]]

    if source == SOURCE_CATEGORY:
        return list(category_samplers[spec.name].sample(n, rng))
    if source == SOURCE_METRIC:
        return _sample_metric_values(spec, n, rng, metric_profiles)
    if source in (SOURCE_SYNTH_ID, SOURCE_SYNTH_TOKEN, SOURCE_SYNTH_ARRAY):
        return _draw_synth_values(
            spec=spec,
            n=n,
            rng=rng,
            array_profiles=array_profiles,
            numeric_profiles=numeric_profiles,
        )
    if source == SOURCE_FIXED:
        return [spec.options["value"]] * n
    if source == SOURCE_BOOL_RATE:
        rate = (bool_profiles.get(spec.name) or {}).get("true_rate")
        # drawn either way, so the RNG stream does not depend on whether the
        # bundle profiles this column
        draws = rng.uniform(size=n)
        if rate is None:
            # not profiled: a pointer rule in constraints.yaml sets every row
            return [False] * n
        return [bool(v) for v in draws < float(rate)]
    if source == SOURCE_EMPTY:
        return [None] * n
    raise ValueError(f"_generate_column_values does not support source {source!r}")


def _generate_derived_metric_values(
    spec: ColumnSpec,
    n: int,
    rng: np.random.Generator,
    metric_profiles: dict[str, dict[str, Any]],
    column_arrays: dict[str, list[Any]],
) -> list[Any]:
    anchor_name = spec.options["anchor"]
    ratio_name = spec.options["ratio_profile"]
    allow_negative = spec.options.get("allow_negative", False)

    anchor_vals = column_arrays[anchor_name]
    profile = metric_profiles[ratio_name]
    ratio_values, ratio_null = _sample_profile_values(profile, n, rng)

    target_null_rate = profile.get("target_null_rate")
    target_null_mask = (
        rng.uniform(size=n) < float(target_null_rate)
        if target_null_rate is not None
        else None
    )

    # a null anchor arrives as None and converts to nan, which is the null mask
    anchor = np.array(anchor_vals, dtype=np.float64)
    null_mask = np.isnan(anchor)
    if target_null_mask is not None:
        null_mask |= target_null_mask

    signs = np.where(anchor < 0, -1.0, 1.0) if allow_negative else 1.0
    # grouped as (sign * |anchor|) * ratio so the rounding matches value by value
    values = (signs * np.abs(anchor)) * ratio_values
    values[null_mask | (anchor == 0) | ratio_null | (ratio_values == 0)] = 0.0

    out: list[Any] = values.tolist()
    for i in np.flatnonzero(null_mask):
        out[i] = None
    return out


def _sample_profile_values(
    profile: dict[str, Any],
    n: int,
    rng: np.random.Generator,
) -> tuple[np.ndarray, np.ndarray]:
    """Draw one calibrated profile: its values, and which rows are null."""
    family = profile.get("family")
    shape = profile.get("shape") or {}
    bounds = profile["bounds"]
    common = {
        "n": n,
        "non_zero_rate": profile.get("non_zero_rate"),
        "null_rate": profile.get("null_rate"),
        "bounds_lower": bounds.get("lower"),
        "bounds_upper": bounds.get("upper"),
        "rng": rng,
    }
    if family == "lognormal":
        values, null_mask = sample_lognormal_metric(
            log_stddev=shape.get("log_stddev"),
            scale_mean=resolve_scale_mean(scale_decade=profile.get("scale_decade")),
            **common,
        )
    elif family == "bounded":
        values, null_mask = sample_bounded_metric(**common)
    else:
        raise ValueError(f"unsupported metric family {family!r}")
    grid = get_metric_grid(profile, rng) if CONFIG.ablation.metric_grid else None
    return snap_to_grid(values, grid), null_mask


def _sample_metric_values(
    spec: ColumnSpec,
    n: int,
    rng: np.random.Generator,
    metric_profiles: dict[str, dict[str, Any]],
) -> list[Any]:
    metric_name = spec.options["metric"]
    profile = metric_profiles.get(metric_name)
    if profile is None:
        raise ValueError(
            f"{spec.name}: bundle has no profile for metric {metric_name!r}; recalibrate the bundle"
        )
    values, null_mask = _sample_profile_values(profile, n, rng)

    if spec.options.get("allow_negative", False):
        sign_flip = rng.uniform(size=n) < 0.1
        values = np.where(sign_flip, -values, values)

    out: list[Any] = values.tolist()
    for i in np.flatnonzero(null_mask):
        out[i] = None
    return out


def _find_category_distribution(
    bundle: Bundle, table_name: str, column: str
) -> dict[str, Any] | None:
    # prefer the table's own calibrated distribution; fall back to any other
    # table for columns not calibrated everywhere
    own = bundle.table(table_name)["category_distributions"]
    if column in own:
        return own[column]
    for other_name, table_data in bundle.tables.items():
        if other_name == table_name:
            continue
        cds = table_data["category_distributions"]
        if column in cds:
            return cds[column]
    return None


def _entity_departure_datetimes(
    snapshot: SnapshotSlice,
    entity: str,
    entity_pools: dict[str, dict[str, Any]],
    entity_indices: dict[str, np.ndarray],
) -> list[datetime]:
    base = datetime.combine(snapshot.departure_date, datetime.min.time())
    by_member = [
        base + timedelta(seconds=int(s)) for s in entity_pools[entity]["_intra_day_seconds"]
    ]
    return [by_member[int(i)] for i in entity_indices[entity]]


def _entity_arrival_datetimes(
    snapshot: SnapshotSlice,
    entity: str,
    entity_pools: dict[str, dict[str, Any]],
    entity_indices: dict[str, np.ndarray],
    key: str = "_service_duration_seconds",
) -> list[datetime]:
    pool = entity_pools[entity]
    base = datetime.combine(snapshot.departure_date, datetime.min.time())
    by_member = [
        base + timedelta(seconds=int(s) + int(d))
        for s, d in zip(pool["_intra_day_seconds"], pool[key], strict=True)
    ]
    return [by_member[int(i)] for i in entity_indices[entity]]

