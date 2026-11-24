"""Migración mínima e idempotente de ventas BNA al iniciar PORÁ.

No modifica filas existentes ni genera acreditaciones ficticias.
"""
from sqlalchemy import inspect, text


def ensure_daily_bna_column(engine):
    with engine.begin() as connection:
        if not inspect(connection).has_table("business_days"):
            raise RuntimeError("No existe business_days; se cancela la migración BNA")
        columns = {column["name"] for column in inspect(connection).get_columns("business_days")}
        if "daily_bna" in columns:
            return False
        dialect = connection.dialect.name
        if dialect == "postgresql":
            connection.execute(text(
                "ALTER TABLE business_days ADD COLUMN IF NOT EXISTS daily_bna DOUBLE PRECISION"
            ))
        elif dialect == "sqlite":
            connection.execute(text("ALTER TABLE business_days ADD COLUMN daily_bna REAL"))
        else:
            raise RuntimeError(f"Motor de base de datos no soportado: {dialect}")
    return True


def ensure_bna_accreditation_column(engine):
    """Agrega el importe diario confirmado sin alterar registros históricos."""
    with engine.begin() as connection:
        if not inspect(connection).has_table("business_days"):
            raise RuntimeError("No existe business_days")
        names = {c["name"] for c in inspect(connection).get_columns("business_days")}
        if "daily_bna_accredited" in names:
            return False
        if connection.dialect.name == "postgresql":
            sql = "ALTER TABLE business_days ADD COLUMN IF NOT EXISTS daily_bna_accredited DOUBLE PRECISION"
        elif connection.dialect.name == "sqlite":
            sql = "ALTER TABLE business_days ADD COLUMN daily_bna_accredited REAL"
        else:
            raise RuntimeError("Motor no soportado")
        connection.execute(text(sql))
    return True
