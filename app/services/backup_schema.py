"""Definición estable del formato de backup de PORÁ Caja."""

from collections import OrderedDict

from app.extensions import db
from app.models import BusinessDay, ExpenseCategory, ExpenseEntry, ShiftRecord, User


BACKUP_FORMAT = "pora-caja-backup"
CURRENT_FORMAT_VERSION = 1

# Registro explícito de las tablas que forman parte del backup.
# El orden también será útil para una futura restauración.
BACKUP_MODELS = OrderedDict(
    [
        ("users", User),
        ("expense_categories", ExpenseCategory),
        ("business_days", BusinessDay),
        ("shift_records", ShiftRecord),
        ("expense_entries", ExpenseEntry),
    ]
)


class BackupSchemaError(RuntimeError):
    """El esquema SQLAlchemy no coincide con el registro de backup."""


def validate_backup_registry() -> None:
    """Evita generar silenciosamente un backup incompleto.

    Si en el futuro se agrega un modelo nuevo a ``db.metadata`` y no se lo
    incorpora a ``BACKUP_MODELS``, la exportación se detendrá con un error.
    """

    registered_names = set(BACKUP_MODELS.keys())
    model_table_names = {model.__table__.name for model in BACKUP_MODELS.values()}
    metadata_table_names = {table.name for table in db.metadata.sorted_tables}

    if registered_names != model_table_names:
        raise BackupSchemaError(
            "El nombre registrado de una o más tablas no coincide con su modelo SQLAlchemy."
        )

    missing_tables = metadata_table_names - registered_names
    unknown_tables = registered_names - metadata_table_names

    if missing_tables or unknown_tables:
        details = []
        if missing_tables:
            details.append(
                "tablas sin registrar: " + ", ".join(sorted(missing_tables))
            )
        if unknown_tables:
            details.append(
                "tablas registradas inexistentes: " + ", ".join(sorted(unknown_tables))
            )
        raise BackupSchemaError("Registro de backup incompleto: " + "; ".join(details))
