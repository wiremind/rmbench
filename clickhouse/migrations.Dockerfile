FROM python:3.13-slim

WORKDIR /app

COPY migrations.requirements.txt /app/migrations.requirements.txt
RUN pip install --no-cache-dir -r /app/migrations.requirements.txt

COPY alembic.ini /app/alembic.ini
COPY migrations /app/migrations

ENTRYPOINT ["alembic", "-c", "/app/alembic.ini", "-n", "clickhouse", "upgrade", "head"]
