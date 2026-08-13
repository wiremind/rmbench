import logging

from alembic import context
from sqlalchemy import engine_from_config, pool

# inspired by https://github.com/xzkostyan/clickhouse-sqlalchemy-alembic-example/blob/main/simple/migrations/env.py

config = context.config
logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)
for key in list(logging.root.manager.loggerDict.keys()):
    if "alembic" in key:
        logging.getLogger(key).disabled = False

# migrations are written as explicit raw SQL, no model metadata to autogenerate from
target_metadata = None

# Currently there is an issue in clickhouse-sqlalchemy https://github.com/xzkostyan/clickhouse-sqlalchemy/pull/369
# makes Alembic mimic what clickhouse_sqlalchemy.alembic is expecting (designed for Alembic 1.5.8)
# https://github.com/sqlalchemy/alembic/blob/rel_1_5_8/alembic/util/sqla_compat.py#L180
from alembic.util import sqla_compat  # noqa: E402

sqla_compat._reflect_table = lambda inspector, table, include_cols: inspector.reflect_table(table, None)
# patch before next import
from clickhouse_sqlalchemy.alembic.dialect import include_object, patch_alembic_version  # noqa: E402

REPLICATION = True


def _replication_kwargs():
    if not REPLICATION:
        return {}
    return {
        "cluster": "bench",
        "table_path": "/clickhouse/tables/default/default/alembic_version",
        "replica_name": "{replica}",
    }


def run_migrations_offline():
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        patch_alembic_version(context, **_replication_kwargs())
        context.run_migrations()


def run_migrations_online():
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    # Wait up to 300s for ON CLUSTER DDL tasks to complete on all replicas
    # (default is 180s) so large migrations don't time out. The clickhouse-native
    # driver reads per-statement settings from execution_options, not connect_args.
    with connectable.connect().execution_options(settings={"distributed_ddl_task_timeout": 300}) as connection:
        context.configure(connection=connection, target_metadata=target_metadata, include_object=include_object)

        with context.begin_transaction():
            patch_alembic_version(context, **_replication_kwargs())
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
