from alembic import context
from alembic.ddl.impl import DefaultImpl
from sqlalchemy import engine_from_config, pool


class DuckDBImpl(DefaultImpl):
    # duckdb-engine has no Alembic dialect; defining this registers one
    __dialect__ = "duckdb"


config = context.config

# migrations are written as explicit raw SQL, no model metadata to autogenerate from
target_metadata = None


def run_migrations_offline():
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()
    connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
