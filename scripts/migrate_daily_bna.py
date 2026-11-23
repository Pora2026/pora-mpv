"""Migración SQLite idempotente para PORÁ: añade business_days.daily_bna.

Ejecutar con PORA_SQLITE_PATH apuntando al archivo real de base de datos.
Hacer copia de seguridad antes. No usar con bases PostgreSQL.
"""
import os
import sqlite3
from pathlib import Path

path = os.environ.get('PORA_SQLITE_PATH')
if not path:
    raise SystemExit('Falta PORA_SQLITE_PATH. No se modificó ninguna base de datos.')
db_path = Path(path).resolve()
if not db_path.is_file():
    raise SystemExit(f'No existe el archivo: {db_path}')
with sqlite3.connect(db_path) as conn:
    columns = {r[1] for r in conn.execute('PRAGMA table_info(business_days)')}
    if not columns:
        raise SystemExit('No existe la tabla business_days. No se modificó la base.')
    if 'daily_bna' in columns:
        print('La columna daily_bna ya existe; no se realizaron cambios.')
    else:
        conn.execute('ALTER TABLE business_days ADD COLUMN daily_bna FLOAT')
        conn.commit()
        print('Columna daily_bna incorporada correctamente.')
