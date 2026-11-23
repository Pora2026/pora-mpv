"""Idempotent migration for PostgreSQL: add nullable daily_bna to business_days.

Run with the same DATABASE_URL as the production service, after backup.
This script does not backfill sales or alter existing rows.
"""
import os
from sqlalchemy import create_engine, inspect, text

url = os.getenv('DATABASE_URL', '')
if not url:
    raise SystemExit('DATABASE_URL no configurada. No se modificó nada.')
url = url.replace('postgres://', 'postgresql://', 1)
engine = create_engine(url)
if engine.dialect.name != 'postgresql':
    raise SystemExit('Esta migración es exclusiva para PostgreSQL.')
with engine.begin() as connection:
    if not inspect(connection).has_table('business_days'):
        raise SystemExit('No existe business_days. No se modificó nada.')
    connection.execute(text('ALTER TABLE business_days ADD COLUMN IF NOT EXISTS daily_bna DOUBLE PRECISION'))
print('OK: business_days.daily_bna disponible; sin cambios en datos históricos.')
