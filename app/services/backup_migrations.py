"""Migraciones del formato JSON de backup de PORÁ Caja.

Este módulo no modifica la base de datos. Convierte backups antiguos en memoria
hasta la versión vigente antes de validarlos o restaurarlos.
"""

from copy import deepcopy
from typing import Any, Callable

from app.services.backup_schema import CURRENT_FORMAT_VERSION


class BackupMigrationError(RuntimeError):
    """El backup no puede convertirse a la versión vigente."""


MigrationFunction = Callable[[dict[str, Any]], dict[str, Any]]

# Ejemplo futuro:
# def _migrate_v1_to_v2(payload): ...
# MIGRATIONS = {1: _migrate_v1_to_v2}
MIGRATIONS: dict[int, MigrationFunction] = {}


def migrate_payload_to_current(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    """Devuelve una copia migrada del payload y la lista de migraciones aplicadas."""

    version = payload.get("format_version")
    if isinstance(version, bool) or not isinstance(version, int):
        raise BackupMigrationError("El campo format_version debe ser un número entero.")

    if version < 1:
        raise BackupMigrationError("La versión del backup no es válida.")

    if version > CURRENT_FORMAT_VERSION:
        raise BackupMigrationError(
            "El backup fue generado por una versión más nueva de PORÁ Caja "
            f"(versión {version}; versión soportada: {CURRENT_FORMAT_VERSION})."
        )

    migrated = deepcopy(payload)
    applied: list[str] = []

    while version < CURRENT_FORMAT_VERSION:
        migration = MIGRATIONS.get(version)
        if migration is None:
            raise BackupMigrationError(
                f"No existe una migración disponible desde la versión {version}."
            )

        next_version = version + 1
        migrated = migration(migrated)
        migrated["format_version"] = next_version
        applied.append(f"v{version} → v{next_version}")
        version = next_version

    return migrated, applied
