#!/usr/bin/env bash
#
# One ClickHouse campaign end to end: fresh stack, generate, insert, query,
# update, collect. Every run is tagged with a campaign id so `rmbench
# collect` can flatten it into CSVs.
#
#   clickhouse/run_benchmark.sh <sf>

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="clickhouse/compose.yaml"
COMPOSE=(docker compose --env-file .compose.env -f "$COMPOSE_FILE")

SF="${1:-}"
CAMPAIGN_ID="${RMBENCH_CAMPAIGN_ID:-campaign-clickhouse-$(date -u +"%Y%m%dT%H%M%SZ")-$RANDOM}"
QUERY_FAMILIES=(synthetic_od_family bi_cache_family)
CONCURRENCY_LEVELS=(4 8 16)
UPDATE_ROW_CHANGE_PERCENT="25"
UPDATE_FIELD_CHANGE_PERCENT="25"

if [[ -z "$SF" ]]; then
    printf 'Usage: %s <sf>\n' "$(basename "$0")" >&2
    exit 1
fi

if [[ ! "$SF" =~ ^[0-9]+$ ]] || (( SF < 1 )); then
    printf 'Scale factor must be a positive integer: %s\n' "$SF" >&2
    exit 1
fi

cd "$ROOT_DIR"

if [[ -f "$ROOT_DIR/.venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "$ROOT_DIR/.venv/bin/activate"
fi

if ! command -v rmbench >/dev/null 2>&1; then
    printf 'rmbench is not on PATH. Install it with: pip install -e .\n' >&2
    exit 1
fi

rmbench resources >/dev/null

cleanup() {
    "${COMPOSE[@]}" down -v --remove-orphans
}

on_exit() {
    if [[ $? -eq 0 ]]; then
        cleanup
    else
        printf 'Run failed: leaving the stack and volumes up for diagnosis\n' >&2
    fi
}

trap on_exit EXIT

# start from an empty database so the insert measurement is a cold load
cleanup
printf 'Starting the ClickHouse stack\n'
"${COMPOSE[@]}" up -d --build --wait

export RMBENCH_CAMPAIGN_ID="$CAMPAIGN_ID"
printf 'Campaign %s\n' "$RMBENCH_CAMPAIGN_ID"

printf 'Generating SF %s\n' "$SF"
rmbench generate --sf "$SF"

printf 'Uploading SF %s\n' "$SF"
rmbench upload --sf "$SF"

printf 'Inserting SF %s\n' "$SF"
rmbench insert --sf "$SF"

for family in "${QUERY_FAMILIES[@]}"; do
    printf 'Running query family %s for SF %s\n' "$family" "$SF"
    rmbench query --sf "$SF" --family "$family"
done

for concurrency in "${CONCURRENCY_LEVELS[@]}"; do
    printf 'Running synthetic_od_family for SF %s at c=%s\n' "$SF" "$concurrency"
    rmbench query --sf "$SF" --family synthetic_od_family --concurrency "$concurrency"
done

printf 'Preparing the update replay for SF %s (%s%% rows, %s%% fields)\n' \
    "$SF" "$UPDATE_ROW_CHANGE_PERCENT" "$UPDATE_FIELD_CHANGE_PERCENT"
rmbench hydrate-update \
    --sf "$SF" \
    --row-change-percent "$UPDATE_ROW_CHANGE_PERCENT" \
    --field-change-percent "$UPDATE_FIELD_CHANGE_PERCENT"

printf 'Replaying the update workload for SF %s\n' "$SF"
rmbench update \
    --sf "$SF" \
    --row-change-percent "$UPDATE_ROW_CHANGE_PERCENT" \
    --field-change-percent "$UPDATE_FIELD_CHANGE_PERCENT"

rmbench collect --campaign-id "$RMBENCH_CAMPAIGN_ID" --sf "$SF"
