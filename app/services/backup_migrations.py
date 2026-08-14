"""Migraciones del formato JSON de backup de PORÁ Caja.

Este módulo no modifica la base de datos. Convierte backups antiguos en memoria
hasta la versión vigente antes de validarlos o restaurarlos.
"""

from copy import deepcopy
import hashlib
import json
from typing import Any, Callable

from app.services.backup_schema import CURRENT_FORMAT_VERSION


class BackupMigrationError(RuntimeError):
    """El backup no puede convertirse a la versión vigente."""


MigrationFunction = Callable[[dict[str, Any]], dict[str, Any]]


def _recompute_content_checksum(payload: dict[str, Any]) -> None:
    """Actualiza el checksum luego de una migración que modifica ``tables``."""

    tables = payload.get("tables")
    if not isinstance(tables, dict):
        raise BackupMigrationError("El backup no contiene un bloque tables válido.")

    canonical = json.dumps(
        tables,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")

    manifest = payload.setdefault("manifest", {})
    if not isinstance(manifest, dict):
        raise BackupMigrationError("El backup no contiene un manifest válido.")

    manifest["content_sha256"] = hashlib.sha256(canonical).hexdigest()


def _migrate_v1_to_v2(payload: dict[str, Any]) -> dict[str, Any]:
    """Agrega el nuevo estado persistente de fondos reservados.

    ``safe_box_transfer`` se conserva intacto como dato legacy. No se convierte
    automáticamente a saldo reservado porque históricamente tuvo una semántica
    distinta. Los nuevos campos nacen en ``null`` y podrán establecerse
    explícitamente después de restaurar un backup v1.
    """

    tables = payload.get("tables")
    manifest = payload.get("manifest")
    if not isinstance(tables, dict) or not isinstance(manifest, dict):
        raise BackupMigrationError("El backup v1 no tiene la estructura esperada.")

    business_days = tables.get("business_days")
    manifest_tables = manifest.get("tables")
    if not isinstance(business_days, list) or not isinstance(manifest_tables, dict):
        raise BackupMigrationError("El backup v1 no contiene business_days válidos.")

    business_meta = manifest_tables.get("business_days")
    if not isinstance(business_meta, dict):
        raise BackupMigrationError("Falta business_days en manifest.tables.")

    for row in business_days:
        if not isinstance(row, dict):
            raise BackupMigrationError("business_days contiene un registro inválido.")
        row.setdefault("reserved_funds_balance", None)
        row.setdefault("reserved_funds_changed_at", None)

    columns = business_meta.get("columns")
    if not isinstance(columns, list) or not all(isinstance(item, str) for item in columns):
        raise BackupMigrationError("Las columnas declaradas de business_days no son válidas.")

    for field in ("reserved_funds_balance", "reserved_funds_changed_at"):
        if field in columns:
            columns.remove(field)

    try:
        insert_at = columns.index("safe_box_transfer") + 1
    except ValueError:
        insert_at = len(columns)

    columns[insert_at:insert_at] = [
        "reserved_funds_balance",
        "reserved_funds_changed_at",
    ]

    _recompute_content_checksum(payload)
    return payload


def _migrate_v2_to_v3(payload: dict[str, Any]) -> dict[str, Any]:
    """Agrega el saldo diario de caja operativa usado en la conciliación."""

    tables = payload.get("tables")
    manifest = payload.get("manifest")
    if not isinstance(tables, dict) or not isinstance(manifest, dict):
        raise BackupMigrationError("El backup v2 no tiene la estructura esperada.")

    business_days = tables.get("business_days")
    manifest_tables = manifest.get("tables")
    if not isinstance(business_days, list) or not isinstance(manifest_tables, dict):
        raise BackupMigrationError("El backup v2 no contiene business_days válidos.")

    business_meta = manifest_tables.get("business_days")
    if not isinstance(business_meta, dict):
        raise BackupMigrationError("Falta business_days en manifest.tables.")

    for row in business_days:
        if not isinstance(row, dict):
            raise BackupMigrationError("business_days contiene un registro inválido.")
        row.setdefault("operating_cash_balance", None)

    columns = business_meta.get("columns")
    if not isinstance(columns, list) or not all(isinstance(item, str) for item in columns):
        raise BackupMigrationError("Las columnas declaradas de business_days no son válidas.")

    if "operating_cash_balance" in columns:
        columns.remove("operating_cash_balance")

    try:
        insert_at = columns.index("reserved_funds_changed_at") + 1
    except ValueError:
        insert_at = len(columns)

    columns.insert(insert_at, "operating_cash_balance")

    _recompute_content_checksum(payload)
    return payload


MIGRATIONS: dict[int, MigrationFunction] = {
    1: _migrate_v1_to_v2,
    2: _migrate_v2_to_v3,
}


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
