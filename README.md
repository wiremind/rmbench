# rmbench

A generator for a wide, denormalised fact schema (three tables, 82 / 48 / 50
columns) plus a harness that times insert, query and update workloads against
ClickHouse and DuckDB.

## Requirements

Python 3.13 or newer, and Docker.

## Setup

```bash
uv venv
source .venv/bin/activate
uv pip install -e .
rmbench resources
docker compose --env-file .compose.env -f clickhouse/compose.yaml up -d
```

`rmbench resources` renders `resources.yaml` into `.compose.env`, where compose
reads the cpu and memory limits from. Re-run it after changing the budget.

`compose.yaml` at the root holds MinIO, which every engine shares; each engine's
own compose file includes it and adds whatever else that engine needs.

Or with the standard library instead of uv:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

The stack is ClickHouse + Keeper + MinIO, and applies the schema migration in
`clickhouse/migrations/` on startup.

## Run

A whole campaign at scale factor 1:

```bash
clickhouse/run_benchmark.sh 1
duckdb/run_benchmark.sh 1
```

`insert`, `query` and `update` take `--engine {clickhouse,duckdb}`; the rest is
engine-neutral. Both engines read the same uploaded objects, and both are held to
the budget in `resources.yaml`.

Or step by step, from the repository root:

```bash
export PATH="$PWD/.venv/bin:$PATH"
export RMBENCH_CAMPAIGN_ID=my-campaign

rmbench generate --sf 1
rmbench upload   --sf 1
rmbench insert   --sf 1
rmbench query    --sf 1
rmbench query    --sf 1 --family bi_cache_family --concurrency 16
rmbench hydrate-update --sf 1 --row-change-percent 10 --field-change-percent 20
rmbench update         --sf 1 --row-change-percent 10 --field-change-percent 20
rmbench collect --campaign-id my-campaign --sf 1
```

Results are written as JSON to `data/benchmark_results/`, and `collect` flattens
a campaign into CSVs under `data/collected/`.

Notes: `insert` requires empty tables, `query` restarts the engine between
measurements so it takes minutes, and `update` requires the baseline already
loaded.

To start over from empty tables:

```bash
docker compose --env-file .compose.env -f clickhouse/compose.yaml down -v   # clickhouse
rm -f data/duckdb/sf1.duckdb data/duckdb/sf1.duckdb.wal   # duckdb
```

## Layout

```
compose.yaml         MinIO, shared by every engine.
resources.yaml       Cpu and memory budget for every component.
clickhouse/          ClickHouse stack, migrations and campaign runner.
duckdb/              DuckDB migrations and campaign runner (no engine container).
src/rmbench/
  generation/        The generator, and the calibration bundle it reads.
  workload/          The insert, query and update measurements, engine-neutral.
  clickhouse/        ClickHouse adapter, SQL statements and query families.
  duckdb/            DuckDB adapter, Alembic runner and query families.
  results/           Timing primitives, result envelope, CSV collection.
```
