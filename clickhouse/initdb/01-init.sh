#!/bin/sh
set -eu

clickhouse-client --user benchmark --password benchmark --database default -n <<'EOSQL'
CREATE DATABASE IF NOT EXISTS rmbench;
EOSQL
