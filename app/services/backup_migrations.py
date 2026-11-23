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


def _migrate_v3_to_v4(payload: dict[str, Any]) -> dict[str, Any]:
    """Agrega las tablas iniciales de Arandú vacías a backups anteriores."""

    tables = payload.get("tables")
    manifest = payload.get("manifest")
    if not isinstance(tables, dict) or not isinstance(manifest, dict):
        raise BackupMigrationError("El backup v3 no tiene la estructura esperada.")

    manifest_tables = manifest.get("tables")
    if not isinstance(manifest_tables, dict):
        raise BackupMigrationError("El backup v3 no contiene manifest.tables válido.")

    new_tables = {
        "arandu_conversations": [
            "id", "title", "created_by_user_id", "status", "created_at", "updated_at"
        ],
        "arandu_messages": [
            "id", "conversation_id", "user_id", "role", "content", "created_at"
        ],
        "arandu_rules": [
            "id", "rule_key", "title", "content", "status", "version",
            "proposed_by_user_id", "approved_by_user_id", "created_at", "approved_at"
        ],
        "arandu_audit_events": [
            "id", "conversation_id", "user_id", "event_type", "payload_json", "created_at"
        ],
    }
    for table_name, columns in new_tables.items():
        tables.setdefault(table_name, [])
        manifest_tables.setdefault(
            table_name, {"row_count": 0, "columns": columns}
        )

    manifest["total_rows"] = sum(
        item.get("row_count", 0)
        for item in manifest_tables.values()
        if isinstance(item, dict)
    )
    _recompute_content_checksum(payload)
    return payload


def _migrate_v4_to_v5(payload: dict[str, Any]) -> dict[str, Any]:
    """Agrega el registro de documentos de Arandú a backups anteriores."""

    tables = payload.get("tables")
    manifest = payload.get("manifest")
    if not isinstance(tables, dict) or not isinstance(manifest, dict):
        raise BackupMigrationError("El backup v4 no tiene la estructura esperada.")
    manifest_tables = manifest.get("tables")
    if not isinstance(manifest_tables, dict):
        raise BackupMigrationError("El backup v4 no contiene manifest.tables válido.")

    table_name = "arandu_documents"
    columns = [
        "id", "original_name", "stored_name", "mime_type", "size_bytes",
        "sha256", "uploaded_by_user_id", "status", "extracted_text", "file_content",
        "created_at",
    ]
    tables.setdefault(table_name, [])
    manifest_tables.setdefault(table_name, {"row_count": 0, "columns": columns})
    manifest["total_rows"] = sum(
        item.get("row_count", 0)
        for item in manifest_tables.values()
        if isinstance(item, dict)
    )
    _recompute_content_checksum(payload)
    return payload


def _migrate_v5_to_v6(payload: dict[str, Any]) -> dict[str, Any]:
    """Incorpora daily_bna sin alterar ventas ni datos historicos."""
    tables = payload.get("tables")
    manifest = payload.get("manifest")
    if not isinstance(tables, dict) or not isinstance(manifest, dict):
        raise BackupMigrationError("El backup v5 no tiene la estructura esperada.")
    rows = tables.get("business_days")
    manifest_tables = manifest.get("tables")
    if not isinstance(rows, list) or not isinstance(manifest_tables, dict):
        raise BackupMigrationError("Falta business_days en el backup.")
    meta = manifest_tables.get("business_days")
    if not isinstance(meta, dict):
        raise BackupMigrationError("Falta el manifiesto de business_days.")
    columns = meta.get("columns")
    if not isinstance(columns, list) or not all(isinstance(c, str) for c in columns):
        raise BackupMigrationError("Columnas de business_days invalidas.")
    for row in rows:
        if not isinstance(row, dict):
            raise BackupMigrationError("Registro business_days invalido.")
        row.setdefault("daily_bna", None)
    # El validador exige el mismo orden que el modelo SQLAlchemy.
    # daily_bna se declara inmediatamente despues de daily_mercadopago.
    if "daily_bna" in columns:
        columns.remove("daily_bna")
    if "daily_mercadopago" not in columns:
        raise BackupMigrationError("Falta daily_mercadopago en el manifiesto de business_days.")
    columns.insert(columns.index("daily_mercadopago") + 1, "daily_bna")
    _recompute_content_checksum(payload)
    return payload


MIGRATIONS: dict[int, MigrationFunction] = {
    1: _migrate_v1_to_v2,
    2: _migrate_v2_to_v3,
    3: _migrate_v3_to_v4,
    4: _migrate_v4_to_v5,
    5: _migrate_v5_to_v6,
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
