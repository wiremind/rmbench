from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import click

from rmbench.clickhouse.engine import ClickHouseEngine
from rmbench.duckdb.engine import DuckDBEngine
from rmbench.generation.bundle import load_bundle
from rmbench.generation.generate import DEFAULT_CHUNK_SIZE, DEFAULT_SEED, generate_public_data
from rmbench.generation.hydrate import (
    UPDATE_MANIFEST_FILENAME,
    change_tokens,
    hydrate_update_data,
    replay_table_counts,
)
from rmbench.generation.spec import SPEC_DIR
from rmbench.io_utils import write_json_file
from rmbench.results.collect import collect_results
from rmbench.results.envelope import RESULTS_DIR
from rmbench.workload.insert import run_insert
from rmbench.workload.query import (
    available_query_families,
    run_query_family,
    run_query_family_concurrent,
)
from rmbench.workload.resources import write_compose_env
from rmbench.workload.storage import BUCKET, upload_prefix
from rmbench.workload.update import run_update

BUNDLE_PATH = SPEC_DIR / "calibration_bundle.json"
DEFAULT_OUTPUT_ROOT = Path("data/synthetic")


@click.group()
def cli() -> None:
    """Generate synthetic benchmark data and time the workload against it."""


def _sf_option(f):
    return click.option("--sf", type=click.IntRange(min=1), required=True, help="Scale factor.")(f)


def _output_root_option(f):
    return click.option(
        "--output-root",
        type=click.Path(file_okay=False, path_type=Path),
        default=DEFAULT_OUTPUT_ROOT,
        show_default=True,
        help="Parent directory; data is read from <output-root>/sf<N>/.",
    )(f)


@cli.command("generate")
@click.option(
    "--sf",
    type=click.IntRange(min=1),
    required=True,
    help="Scale factor. The snapshot window is derived from the bundle.",
)
@click.option(
    "--output-root",
    type=click.Path(file_okay=False, path_type=Path),
    default=DEFAULT_OUTPUT_ROOT,
    show_default=True,
    help="Parent directory; data is written to <output-root>/sf<N>/.",
)
def generate_command(sf: int, output_root: Path) -> None:
    """Generate the three fact tables as multi-member csv.gz files."""
    output_dir = output_root / f"sf{sf}"
    window = load_bundle(BUNDLE_PATH).sf_window(sf)
    snapshots = int(window["sale_days"]) * int(window["departure_days"])

    click.echo(f"Bundle: {BUNDLE_PATH}")
    click.echo(f"Output: {output_dir}")
    click.echo(
        f"Window: sale {window['sale_start']} +{window['sale_days']}d  "
        f"departure {window['departure_start']} +{window['departure_days']}d  "
        f"({snapshots} snapshots, SF={sf})"
    )

    summary = generate_public_data(
        sale_start=date.fromisoformat(window["sale_start"]),
        departure_start=date.fromisoformat(window["departure_start"]),
        sale_days=int(window["sale_days"]),
        departure_days=int(window["departure_days"]),
        bundle_path=BUNDLE_PATH,
        output_dir=output_dir,
        seed=DEFAULT_SEED,
        chunk_size=DEFAULT_CHUNK_SIZE,
    )
    for table_name, info in summary.items():
        click.echo(
            f"  {table_name}: {info['rows_written']:>12,} rows "
            f"({info['rows_per_snapshot']:,}/snapshot × {info['snapshot_count']}) "
            f"-> {info['path']}"
        )


def _scenario(sf: int, window: dict, **parameters) -> dict:
    sale_start = date.fromisoformat(window["sale_start"])
    departure_start = date.fromisoformat(window["departure_start"])
    sale_days = int(window["sale_days"])
    departure_days = int(window["departure_days"])
    return {
        "window": {
            "label": f"sf{sf}",
            "sale_days": sale_days,
            "departure_days": departure_days,
            "sale_start": sale_start.isoformat(),
            "sale_end": (sale_start + timedelta(days=sale_days)).isoformat(),
            "departure_start": departure_start.isoformat(),
            "departure_end": (departure_start + timedelta(days=departure_days)).isoformat(),
        },
        "parameters": {name: value for name, value in parameters.items() if value is not None},
    }


def _engine_option(f):
    return click.option(
        "--engine",
        type=click.Choice(["clickhouse", "duckdb"]),
        default="clickhouse",
        show_default=True,
        help="Which engine to measure.",
    )(f)


def _engine(name: str, sf: int):
    return ClickHouseEngine() if name == "clickhouse" else DuckDBEngine(scale_factor=sf)


@cli.command("upload")
@_sf_option
@_output_root_option
def upload_command(sf: int, output_root: Path) -> None:
    """Upload a generated scale factor into MinIO, ready to insert."""
    input_dir = _require_data_dir(output_root, sf)
    keys = upload_prefix(input_dir=input_dir, prefix=_s3_prefix(sf))
    for key in keys:
        click.echo(f"Uploaded s3://{BUCKET}/{key}")


@cli.command("insert")
@_sf_option
@_engine_option
@_output_root_option
def insert_command(sf: int, engine: str, output_root: Path) -> None:
    """Insert an uploaded scale factor into the chosen engine and record the timings."""
    input_dir = _require_data_dir(output_root, sf)
    prefix = _s3_prefix(sf)
    window = load_bundle(BUNDLE_PATH).sf_window(sf)

    result = run_insert(
        engine=_engine(engine, sf),
        scale_factor=sf,
        input_dir=input_dir,
        s3_prefix=prefix,
        scenario=_scenario(sf, window, source_prefix=prefix, source_dir=str(input_dir)),
    )
    path = write_json_file(RESULTS_DIR / f"insert_sf{sf}_{result['run_id']}.json", result)

    durations = result["result"]["timing"]["durations_ms"]
    click.echo(f"insert   {durations['submit_to_task_done']:>9,} ms  (statement execution)")
    click.echo(f"visible  {durations['submit_to_visible']:>9,} ms")
    click.echo(f"physical {durations['submit_to_physical_done']:>9,} ms")
    click.echo(f"Wrote {path}")


@cli.command("query")
@_sf_option
@_engine_option
@click.option(
    "--family",
    type=click.Choice(available_query_families(ClickHouseEngine().query_dir), case_sensitive=False),
    default="synthetic_od_family",
    show_default=True,
    help="Query family to time.",
)
@click.option(
    "--concurrency",
    type=click.IntRange(1, 256),
    default=None,
    help="Run every worker on the same query at once instead of the cold/warm/hot groups.",
)
def query_command(sf: int, engine: str, family: str, concurrency: int | None) -> None:
    """Time a query family against the chosen engine."""
    window = load_bundle(BUNDLE_PATH).sf_window(sf)
    scenario = _scenario(sf, window, family=family, concurrency=concurrency)

    if concurrency:
        result = run_query_family_concurrent(
            engine=_engine(engine, sf),
            sf=sf, family=family, window=window, scenario=scenario, concurrency=concurrency
        )
        name = f"query_concurrent_{family}_sf{sf}_c{concurrency}_{result['run_id']}.json"
    else:
        result = run_query_family(
            engine=_engine(engine, sf), sf=sf, family=family,
            window=window, scenario=scenario,
        )
        name = f"query_{family}_sf{sf}_{result['run_id']}.json"

    for group in result["result"]["groups"]:
        click.echo(f"[{group['group_name']}]")
        for entry in group["queries"]:
            latency = entry["latency_ms"]
            click.echo(f"  {entry['query_name']:<36}p50 {latency['p50']:>9,.1f} ms   p95 {latency['p95']:>9,.1f} ms")
    click.echo(f"Wrote {write_json_file(RESULTS_DIR / name, result)}")


def _change_options(f):
    for name, help_text in (
        ("field-change-percent", "Percentage of mutable fields to change inside each mutated row."),
        ("row-change-percent", "Percentage of rows to mutate."),
    ):
        f = click.option(f"--{name}", type=click.FloatRange(0.0, 100.0), required=True, help=help_text)(f)
    return f


@cli.command("hydrate-update")
@_sf_option
@_change_options
@_output_root_option
def hydrate_update_command(
    sf: int, row_change_percent: float, field_change_percent: float, output_root: Path
) -> None:
    """Build a replay dataset by mutating a copy of the generated baseline."""
    input_dir = _require_data_dir(output_root, sf)
    output_dir = output_root / _update_dir_name(sf, row_change_percent, field_change_percent)
    manifest_path, cached = hydrate_update_data(
        sf=sf,
        row_change_percent=row_change_percent,
        field_change_percent=field_change_percent,
        input_dir=input_dir,
        output_dir=output_dir,
    )
    click.echo(f"{'Reusing' if cached else 'Hydrated'} {output_dir}")
    tables = json.loads(manifest_path.read_text())["tables"]
    for table_name, stats in tables.items():
        click.echo(
            f"  {table_name:<36}{stats['row_count']:>10,} rows  "
            f"{stats['changed_row_count']:>9,} changed  {stats['changed_field_count']:>10,} fields"
        )


@cli.command("update")
@_sf_option
@_engine_option
@_change_options
@_output_root_option
def update_command(
    sf: int, engine: str, row_change_percent: float, field_change_percent: float, output_root: Path
) -> None:
    """Replay one update batch into the chosen engine and record the timings."""
    name = _update_dir_name(sf, row_change_percent, field_change_percent)
    input_dir = output_root / name
    manifest_path = input_dir / UPDATE_MANIFEST_FILENAME
    if not manifest_path.exists():
        raise click.UsageError(f"No replay data at {input_dir}. Run `rmbench hydrate-update` first.")

    prefix = f"rmbench_{name}"
    upload_prefix(input_dir=input_dir, prefix=prefix)

    window = load_bundle(BUNDLE_PATH).sf_window(sf)
    sale_start = datetime.fromisoformat(window["sale_start"]).replace(tzinfo=UTC)
    sale_window = (sale_start, sale_start + timedelta(days=int(window["sale_days"])))

    result = run_update(
        engine=_engine(engine, sf),
        scale_factor=sf,
        s3_prefix=prefix,
        replay_counts=replay_table_counts(manifest_path),
        sale_window=sale_window,
        scenario=_scenario(
            sf,
            window,
            source_prefix=prefix,
            row_change_percent=row_change_percent,
            field_change_percent=field_change_percent,
        ),
    )
    for phase in result["result"]["phases"]:
        d = phase["timing"]["durations_ms"]
        click.echo(
            f"{phase['phase_name']:<8}exec {d['submit_to_task_done']:>8,} ms   "
            f"visible {d['submit_to_visible']:>8,} ms   physical {d['submit_to_physical_done']:>8,} ms"
        )
    overall = result["result"]["timing"]["durations_ms"]
    click.echo(f"{'overall':<8}{'':>13}   visible {overall['submit_to_visible']:>8,} ms   "
               f"physical {overall['submit_to_physical_done']:>8,} ms")
    click.echo(f"Wrote {write_json_file(RESULTS_DIR / f'update_sf{sf}_{result['run_id']}.json', result)}")


@cli.command("collect")
@click.option("--campaign-id", required=True, help="Campaign identifier to collect.")
@click.option("--sf", "scale_factors", type=int, multiple=True, help="Scale factor filter. Repeatable.")
def collect_command(campaign_id: str, scale_factors: tuple[int, ...]) -> None:
    """Flatten a campaign's result JSONs into CSV tables."""
    summary = collect_results(campaign_id=campaign_id, scale_factors=scale_factors)
    for name, count in summary["row_counts"].items():
        click.echo(f"  {name:<16}{count:>8,} rows")
    if summary["skipped_result_file_count"]:
        click.echo(f"  skipped {summary['skipped_result_file_count']} file(s) not on the current schema")
    click.echo(f"Wrote {summary['output_dir']}")


def _update_dir_name(sf: int, row_change_percent: float, field_change_percent: float) -> str:
    return f"sf{sf}_update_{change_tokens(row_change_percent, field_change_percent)}"


def _s3_prefix(sf: int) -> str:
    return f"rmbench_sf{sf}"


def _require_data_dir(output_root: Path, sf: int) -> Path:
    input_dir = output_root / f"sf{sf}"
    if not input_dir.exists():
        raise click.UsageError(f"No data at {input_dir}. Run `rmbench generate --sf {sf}` first.")
    return input_dir


if __name__ == "__main__":
    cli()


@cli.command("resources")
def resources_command() -> None:
    """Render resources.yaml into the env file docker compose reads."""
    click.echo(f"Wrote {write_compose_env()}")
