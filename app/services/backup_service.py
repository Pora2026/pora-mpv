"""Generación y validación de backups completos y versionados."""

import base64
import hashlib
import json
import math
import os
import re
import secrets
import tempfile
import time as time_module
from datetime import date, datetime, time, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    LargeBinary,
    Numeric,
    String,
    Text,
    delete,
    func,
    insert,
    select,
    text,
)
from sqlalchemy.inspection import inspect as sa_inspect

from flask import current_app

from app.extensions import db
from app.services.backup_migrations import BackupMigrationError, migrate_payload_to_current
from app.services.backup_schema import (
    BACKUP_FORMAT,
    BACKUP_MODELS,
    CURRENT_FORMAT_VERSION,
    validate_backup_registry,
)


MAX_BACKUP_BYTES = 10 * 1024 * 1024
MAX_VALIDATION_ERRORS = 50
PENDING_RESTORE_TTL_SECONDS = 24 * 60 * 60
_RESTORE_TOKEN_PATTERN = re.compile(r"^[a-f0-9]{64}$")


class BackupGenerationError(RuntimeError):
    """No fue posible generar un backup íntegro."""


class BackupValidationError(RuntimeError):
    """No fue posible leer o validar el archivo de backup."""


class BackupRestoreError(RuntimeError):
    """No fue posible restaurar el backup de forma segura."""


def _serialize_value(value: Any) -> Any:
    """Convierte valores SQLAlchemy a tipos portables en JSON."""

    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, (date, time)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, Enum):
        return _serialize_value(value.value)

    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        return {
            "__type__": "bytes",
            "encoding": "base64",
            "value": base64.b64encode(raw).decode("ascii"),
        }

    raise BackupGenerationError(
        f"Tipo de dato no soportado en backup: {type(value).__name__}"
    )


def _serialize_record(record: Any) -> dict[str, Any]:
    mapper = sa_inspect(record).mapper
    return {
        column.key: _serialize_value(getattr(record, column.key))
        for column in mapper.columns
    }


def _ordered_records(model: Any) -> list[Any]:
    mapper = sa_inspect(model)
    primary_key_columns = list(mapper.primary_key)

    statement = select(model)
    if primary_key_columns:
        statement = statement.order_by(*primary_key_columns)

    return list(db.session.execute(statement).scalars().all())


def _content_checksum(tables: dict[str, list[dict[str, Any]]]) -> str:
    canonical = json.dumps(
        tables,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def get_backup_summary() -> dict[str, int]:
    """Devuelve cantidades sin modificar ningún registro."""

    validate_backup_registry()

    summary: dict[str, int] = {}
    for table_name, model in BACKUP_MODELS.items():
        statement = select(func.count()).select_from(model)
        summary[table_name] = int(db.session.execute(statement).scalar_one())
    return summary


def build_backup_payload() -> dict[str, Any]:
    """Construye en memoria un backup íntegro de todas las tablas registradas."""

    validate_backup_registry()

    tables: dict[str, list[dict[str, Any]]] = {}
    manifest_tables: dict[str, dict[str, Any]] = {}

    for table_name, model in BACKUP_MODELS.items():
        rows = [_serialize_record(record) for record in _ordered_records(model)]
        columns = [column.key for column in sa_inspect(model).columns]

        tables[table_name] = rows
        manifest_tables[table_name] = {
            "row_count": len(rows),
            "columns": columns,
        }

    total_rows = sum(item["row_count"] for item in manifest_tables.values())

    return {
        "format": BACKUP_FORMAT,
        "format_version": CURRENT_FORMAT_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "database_engine": db.engine.dialect.name,
        },
        "manifest": {
            "tables": manifest_tables,
            "total_rows": total_rows,
            "content_sha256": _content_checksum(tables),
        },
        "tables": tables,
    }


def build_backup_bytes() -> bytes:
    payload = build_backup_payload()
    return json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
        sort_keys=False,
        allow_nan=False,
    ).encode("utf-8")


def _new_result(kind: str) -> dict[str, Any]:
    return {
        "kind": kind,
        "recognized": kind in {"backup", "legacy"},
        "valid": False,
        "restorable": False,
        "format": None,
        "format_version": None,
        "generated_at": None,
        "source_engine": None,
        "checksum_valid": None,
        "table_counts": {},
        "total_rows": 0,
        "migrations_applied": [],
        "warnings": [],
        "errors": [],
    }


def _append_error(result: dict[str, Any], message: str) -> None:
    errors = result["errors"]
    if len(errors) < MAX_VALIDATION_ERRORS:
        errors.append(message)
    elif len(errors) == MAX_VALIDATION_ERRORS:
        errors.append("Se omitieron errores adicionales para mantener el informe legible.")


def _parse_json(content: bytes) -> Any:
    if not content:
        raise BackupValidationError("El archivo está vacío.")

    if len(content) > MAX_BACKUP_BYTES:
        raise BackupValidationError(
            f"El archivo supera el máximo permitido de {MAX_BACKUP_BYTES // (1024 * 1024)} MB."
        )

    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise BackupValidationError("El archivo no está codificado en UTF-8.") from exc

    def _reject_constant(value: str):
        raise ValueError(f"valor numérico no válido: {value}")

    try:
        return json.loads(text, parse_constant=_reject_constant)
    except json.JSONDecodeError as exc:
        raise BackupValidationError(
            f"JSON inválido en línea {exc.lineno}, columna {exc.colno}: {exc.msg}."
        ) from exc
    except ValueError as exc:
        raise BackupValidationError(f"JSON inválido: {exc}.") from exc


def _looks_like_legacy(payload: dict[str, Any]) -> bool:
    required = {"days", "shifts", "expenses", "categories"}
    return required.issubset(payload.keys()) and "tables" not in payload


def _inspect_legacy(payload: dict[str, Any]) -> dict[str, Any]:
    result = _new_result("legacy")
    result.update(
        {
            "recognized": True,
            "valid": True,
            "restorable": False,
            "format": "export-legacy-parcial",
            "generated_at": payload.get("generated_at"),
        }
    )

    mapping = {
        "days": "business_days",
        "shifts": "shift_records",
        "categories": "expense_categories",
        "expenses": "expense_entries",
    }

    for legacy_name, table_name in mapping.items():
        rows = payload.get(legacy_name)
        if not isinstance(rows, list):
            result["valid"] = False
            _append_error(result, f"El bloque '{legacy_name}' debe ser una lista.")
            continue
        result["table_counts"][table_name] = len(rows)

    result["total_rows"] = sum(result["table_counts"].values())
    result["warnings"].extend(
        [
            "Es una exportación legacy parcial, no un backup completo.",
            "No conserva todos los campos financieros, IDs ni usuarios.",
            "No podrá utilizarse para una restauración completa.",
        ]
    )
    return result


def _expected_schema() -> dict[str, dict[str, Any]]:
    validate_backup_registry()
    schema: dict[str, dict[str, Any]] = {}
    for table_name, model in BACKUP_MODELS.items():
        mapper = sa_inspect(model)
        schema[table_name] = {
            "model": model,
            "columns": [column.key for column in mapper.columns],
            "column_objects": {column.key: column for column in mapper.columns},
            "primary_keys": [column.key for column in mapper.primary_key],
        }
    return schema


def _valid_iso_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
        return True
    except (TypeError, ValueError):
        return False


def _valid_iso_datetime(value: str) -> bool:
    try:
        datetime.fromisoformat(value)
        return True
    except (TypeError, ValueError):
        return False


def _validate_column_value(column: Any, value: Any) -> str | None:
    if value is None:
        if not column.nullable and not column.primary_key:
            return "no admite null"
        return None

    column_type = column.type

    if isinstance(column_type, Boolean):
        if not isinstance(value, bool):
            return "debe ser booleano"
        return None

    if isinstance(column_type, Integer):
        if isinstance(value, bool) or not isinstance(value, int):
            return "debe ser entero"
        return None

    if isinstance(column_type, (Float, Numeric)):
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            return "debe ser numérico"
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return "debe ser numérico"
        if not math.isfinite(numeric):
            return "debe ser un número finito"
        return None

    if isinstance(column_type, DateTime):
        if not isinstance(value, str) or not _valid_iso_datetime(value):
            return "debe ser una fecha y hora ISO válida"
        return None

    if isinstance(column_type, Date):
        if not isinstance(value, str) or not _valid_iso_date(value):
            return "debe ser una fecha ISO válida"
        return None

    if isinstance(column_type, (String, Text)):
        if not isinstance(value, str):
            return "debe ser texto"
        max_length = getattr(column_type, "length", None)
        if max_length is not None and len(value) > max_length:
            return f"supera el máximo de {max_length} caracteres"
        return None

    return None


def _validate_current_backup(payload: dict[str, Any]) -> dict[str, Any]:
    result = _new_result("backup")
    result.update(
        {
            "recognized": True,
            "format": payload.get("format"),
            "format_version": payload.get("format_version"),
            "generated_at": payload.get("generated_at"),
        }
    )

    try:
        migrated, applied = migrate_payload_to_current(payload)
        result["migrations_applied"] = applied
    except BackupMigrationError as exc:
        _append_error(result, str(exc))
        return result

    payload = migrated
    result["format_version"] = payload.get("format_version")

    if payload.get("format") != BACKUP_FORMAT:
        _append_error(result, "El identificador de formato no corresponde a PORÁ Caja.")

    generated_at = payload.get("generated_at")
    if not isinstance(generated_at, str) or not _valid_iso_datetime(generated_at):
        _append_error(result, "generated_at debe ser una fecha y hora ISO válida.")

    source = payload.get("source")
    if not isinstance(source, dict):
        _append_error(result, "El bloque source debe ser un objeto.")
        source = {}
    source_engine = source.get("database_engine")
    if source_engine is not None and not isinstance(source_engine, str):
        _append_error(result, "source.database_engine debe ser texto.")
    result["source_engine"] = source_engine

    manifest = payload.get("manifest")
    if not isinstance(manifest, dict):
        _append_error(result, "El bloque manifest debe ser un objeto.")
        manifest = {}

    manifest_tables = manifest.get("tables")
    if not isinstance(manifest_tables, dict):
        _append_error(result, "manifest.tables debe ser un objeto.")
        manifest_tables = {}

    tables = payload.get("tables")
    if not isinstance(tables, dict):
        _append_error(result, "El bloque tables debe ser un objeto.")
        tables = {}

    expected = _expected_schema()
    expected_names = set(expected)
    actual_names = set(tables)
    manifest_names = set(manifest_tables)

    for name in sorted(expected_names - actual_names):
        _append_error(result, f"Falta la tabla obligatoria '{name}'.")
    for name in sorted(actual_names - expected_names):
        _append_error(result, f"El backup contiene una tabla desconocida: '{name}'.")
    for name in sorted(expected_names - manifest_names):
        _append_error(result, f"Falta '{name}' en manifest.tables.")
    for name in sorted(manifest_names - expected_names):
        _append_error(result, f"manifest.tables contiene una tabla desconocida: '{name}'.")

    table_ids: dict[str, set[Any]] = {}
    rows_by_table: dict[str, list[dict[str, Any]]] = {}

    for table_name, table_schema in expected.items():
        rows = tables.get(table_name)
        meta = manifest_tables.get(table_name)

        if not isinstance(rows, list):
            if table_name in tables:
                _append_error(result, f"tables.{table_name} debe ser una lista.")
            continue

        rows_by_table[table_name] = rows
        result["table_counts"][table_name] = len(rows)

        if not isinstance(meta, dict):
            if table_name in manifest_tables:
                _append_error(result, f"manifest.tables.{table_name} debe ser un objeto.")
            meta = {}

        declared_count = meta.get("row_count")
        if isinstance(declared_count, bool) or not isinstance(declared_count, int):
            _append_error(result, f"El row_count de '{table_name}' debe ser entero.")
        elif declared_count != len(rows):
            _append_error(
                result,
                f"La tabla '{table_name}' declara {declared_count} registros pero contiene {len(rows)}.",
            )

        declared_columns = meta.get("columns")
        expected_columns = table_schema["columns"]
        if not isinstance(declared_columns, list) or not all(
            isinstance(item, str) for item in declared_columns
        ):
            _append_error(result, f"La lista de columnas de '{table_name}' no es válida.")
        elif declared_columns != expected_columns:
            missing = sorted(set(expected_columns) - set(declared_columns))
            extra = sorted(set(declared_columns) - set(expected_columns))
            details = []
            if missing:
                details.append("faltan: " + ", ".join(missing))
            if extra:
                details.append("sobran: " + ", ".join(extra))
            if not details:
                details.append("el orden no coincide")
            _append_error(
                result,
                f"Las columnas declaradas de '{table_name}' no coinciden ({'; '.join(details)}).",
            )

        expected_key_set = set(expected_columns)
        primary_keys = table_schema["primary_keys"]
        primary_values: set[Any] = set()

        for index, row in enumerate(rows, start=1):
            if not isinstance(row, dict):
                _append_error(result, f"{table_name}[{index}] debe ser un objeto.")
                continue

            row_keys = set(row)
            missing_keys = sorted(expected_key_set - row_keys)
            extra_keys = sorted(row_keys - expected_key_set)
            if missing_keys:
                _append_error(
                    result,
                    f"{table_name}[{index}] no contiene: {', '.join(missing_keys)}.",
                )
            if extra_keys:
                _append_error(
                    result,
                    f"{table_name}[{index}] contiene campos desconocidos: {', '.join(extra_keys)}.",
                )

            for column_name, column in table_schema["column_objects"].items():
                if column_name not in row:
                    continue
                error = _validate_column_value(column, row[column_name])
                if error:
                    _append_error(
                        result,
                        f"{table_name}[{index}].{column_name} {error}.",
                    )

            if primary_keys and all(key in row for key in primary_keys):
                pk_value = tuple(row[key] for key in primary_keys)
                if any(value is None for value in pk_value):
                    _append_error(result, f"{table_name}[{index}] tiene una clave primaria nula.")
                elif pk_value in primary_values:
                    _append_error(result, f"La tabla '{table_name}' contiene una clave primaria duplicada: {pk_value}.")
                else:
                    primary_values.add(pk_value)

        if len(primary_keys) == 1:
            table_ids[table_name] = {item[0] for item in primary_values}

    result["total_rows"] = sum(result["table_counts"].values())

    declared_total = manifest.get("total_rows")
    if isinstance(declared_total, bool) or not isinstance(declared_total, int):
        _append_error(result, "manifest.total_rows debe ser entero.")
    elif declared_total != result["total_rows"]:
        _append_error(
            result,
            f"manifest.total_rows declara {declared_total} registros pero se detectaron {result['total_rows']}.",
        )

    declared_checksum = manifest.get("content_sha256")
    if not isinstance(declared_checksum, str) or len(declared_checksum) != 64:
        _append_error(result, "manifest.content_sha256 no es un SHA-256 válido.")
        result["checksum_valid"] = False
    elif isinstance(tables, dict):
        try:
            calculated_checksum = _content_checksum(tables)
        except (TypeError, ValueError):
            calculated_checksum = None
        result["checksum_valid"] = calculated_checksum == declared_checksum
        if result["checksum_valid"] is False:
            _append_error(result, "El checksum no coincide: el contenido fue modificado o está corrupto.")

    business_day_ids = table_ids.get("business_days", set())
    category_ids = table_ids.get("expense_categories", set())

    for index, row in enumerate(rows_by_table.get("shift_records", []), start=1):
        if isinstance(row, dict) and row.get("business_day_id") not in business_day_ids:
            _append_error(
                result,
                f"shift_records[{index}] referencia un business_day_id inexistente.",
            )

    for index, row in enumerate(rows_by_table.get("expense_entries", []), start=1):
        if not isinstance(row, dict):
            continue
        if row.get("business_day_id") not in business_day_ids:
            _append_error(
                result,
                f"expense_entries[{index}] referencia un business_day_id inexistente.",
            )
        if row.get("category_id") not in category_ids:
            _append_error(
                result,
                f"expense_entries[{index}] referencia un category_id inexistente.",
            )

    unique_checks = [
        ("users", ("username",)),
        ("business_days", ("day",)),
        ("expense_categories", ("kind", "name")),
        ("shift_records", ("business_day_id", "shift")),
    ]
    for table_name, fields in unique_checks:
        seen: set[tuple[Any, ...]] = set()
        for index, row in enumerate(rows_by_table.get(table_name, []), start=1):
            if not isinstance(row, dict) or not all(field in row for field in fields):
                continue
            key = tuple(row[field] for field in fields)
            if key in seen:
                _append_error(
                    result,
                    f"{table_name}[{index}] viola la unicidad de {', '.join(fields)}: {key}.",
                )
            seen.add(key)

    result["valid"] = len(result["errors"]) == 0
    result["restorable"] = result["valid"]
    if result["migrations_applied"]:
        result["warnings"].append(
            "El archivo fue migrado en memoria antes de validarse: "
            + ", ".join(result["migrations_applied"])
            + "."
        )
    return result


def inspect_backup_bytes(content: bytes) -> dict[str, Any]:
    """Clasifica y valida un JSON sin modificar la base de datos."""

    try:
        payload = _parse_json(content)
    except BackupValidationError as exc:
        result = _new_result("unknown")
        _append_error(result, str(exc))
        return result

    if not isinstance(payload, dict):
        result = _new_result("unknown")
        _append_error(result, "La raíz del JSON debe ser un objeto.")
        return result

    if payload.get("format") == BACKUP_FORMAT:
        return _validate_current_backup(payload)

    if _looks_like_legacy(payload):
        return _inspect_legacy(payload)

    result = _new_result("unknown")
    _append_error(
        result,
        "El archivo no corresponde a un backup de PORÁ Caja ni a una exportación legacy reconocida.",
    )
    return result

# -----------------------------------------------------------------------------
# Staging seguro para la confirmación de restore
# -----------------------------------------------------------------------------

def _atomic_write_bytes(destination: Path, content: bytes) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=".tmp_",
        suffix=destination.suffix or ".json",
        dir=str(destination.parent),
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.chmod(temporary_path, 0o600)
        except OSError:
            pass
        os.replace(temporary_path, destination)
    except Exception:
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _restore_staging_dir() -> Path:
    return Path(current_app.instance_path) / "restore_staging"


def cleanup_staged_restore_files() -> None:
    directory = _restore_staging_dir()
    if not directory.exists():
        return

    cutoff = time_module.time() - PENDING_RESTORE_TTL_SECONDS
    for path in directory.glob("*.json"):
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink(missing_ok=True)
        except OSError:
            continue


def _validated_restore_token(token: str) -> str:
    if not isinstance(token, str) or not _RESTORE_TOKEN_PATTERN.fullmatch(token):
        raise BackupRestoreError("El identificador temporal de restauración no es válido.")
    return token


def stage_restore_bytes(content: bytes) -> str:
    """Guarda temporalmente un backup ya validado para su confirmación posterior."""

    _load_restorable_payload(content)
    cleanup_staged_restore_files()

    token = secrets.token_hex(32)
    destination = _restore_staging_dir() / f"{token}.json"
    _atomic_write_bytes(destination, content)
    return token


def read_staged_restore_bytes(token: str) -> bytes:
    token = _validated_restore_token(token)
    path = _restore_staging_dir() / f"{token}.json"
    if not path.is_file():
        raise BackupRestoreError(
            "El archivo temporal ya no está disponible. Volvé a validarlo antes de restaurar."
        )

    content = path.read_bytes()
    if len(content) > MAX_BACKUP_BYTES:
        raise BackupRestoreError("El archivo temporal supera el tamaño permitido.")
    return content


def delete_staged_restore(token: str | None) -> None:
    if not token:
        return
    try:
        token = _validated_restore_token(token)
    except BackupRestoreError:
        return
    try:
        (_restore_staging_dir() / f"{token}.json").unlink(missing_ok=True)
    except OSError:
        pass


# -----------------------------------------------------------------------------
# Restore transaccional
# -----------------------------------------------------------------------------

def _load_restorable_payload(content: bytes) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        payload = _parse_json(content)
    except BackupValidationError as exc:
        raise BackupRestoreError(str(exc)) from exc

    if not isinstance(payload, dict) or payload.get("format") != BACKUP_FORMAT:
        raise BackupRestoreError("El archivo no es un backup completo de PORÁ Caja.")

    result = _validate_current_backup(payload)
    if not result.get("restorable"):
        errors = result.get("errors") or ["El backup no superó las validaciones."]
        raise BackupRestoreError(" ".join(errors[:5]))

    try:
        migrated, _ = migrate_payload_to_current(payload)
    except BackupMigrationError as exc:
        raise BackupRestoreError(str(exc)) from exc

    return migrated, result


def _deserialize_value(column: Any, value: Any) -> Any:
    if value is None:
        return None

    column_type = column.type

    if isinstance(column_type, DateTime):
        return datetime.fromisoformat(value)
    if isinstance(column_type, Date):
        return date.fromisoformat(value)
    if isinstance(column_type, Numeric):
        return Decimal(str(value))
    if isinstance(column_type, Float):
        return float(value)
    if isinstance(column_type, Integer):
        return int(value)
    if isinstance(column_type, Boolean):
        return bool(value)
    if isinstance(column_type, LargeBinary):
        if not isinstance(value, dict) or value.get("__type__") != "bytes":
            raise BackupRestoreError(
                f"El campo binario '{column.key}' no tiene un formato válido."
            )
        try:
            return base64.b64decode(value["value"], validate=True)
        except Exception as exc:
            raise BackupRestoreError(
                f"El campo binario '{column.key}' no pudo decodificarse."
            ) from exc

    return value


def _deserialize_row(model: Any, row: dict[str, Any]) -> dict[str, Any]:
    mapper = sa_inspect(model)
    return {
        column.key: _deserialize_value(column, row[column.key])
        for column in mapper.columns
    }


def _create_pre_restore_backup() -> Path:
    """Genera y persiste el backup preventivo antes de modificar la base."""

    try:
        content = build_backup_bytes()
    finally:
        # Libera la conexión de lectura antes de abrir la transacción destructiva.
        db.session.remove()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    destination = (
        Path(current_app.instance_path)
        / "backups"
        / f"pre_restore_{timestamp}.json"
    )
    try:
        _atomic_write_bytes(destination, content)
    except Exception as exc:
        raise BackupRestoreError(
            "No se pudo guardar el backup preventivo. La restauración fue cancelada."
        ) from exc
    return destination


def _reset_postgresql_sequences(connection: Any, payload: dict[str, Any]) -> None:
    if connection.dialect.name != "postgresql":
        return

    for table_name, model in BACKUP_MODELS.items():
        mapper = sa_inspect(model)
        primary_keys = list(mapper.primary_key)
        if len(primary_keys) != 1 or not isinstance(primary_keys[0].type, Integer):
            continue

        primary_key = primary_keys[0]
        sequence_name = connection.execute(
            text(
                "SELECT pg_get_serial_sequence(:table_name, :column_name)"
            ),
            {
                "table_name": model.__table__.fullname,
                "column_name": primary_key.name,
            },
        ).scalar_one_or_none()

        if not sequence_name:
            continue

        values = [
            row.get(primary_key.key)
            for row in payload["tables"].get(table_name, [])
            if row.get(primary_key.key) is not None
        ]
        if values:
            value = max(values)
            is_called = True
        else:
            value = 1
            is_called = False

        connection.execute(
            text(
                "SELECT setval(CAST(:sequence_name AS regclass), :value, :is_called)"
            ),
            {
                "sequence_name": sequence_name,
                "value": value,
                "is_called": is_called,
            },
        )


def _verify_restored_database(connection: Any, payload: dict[str, Any]) -> dict[str, int]:
    restored_counts: dict[str, int] = {}

    for table_name, model in BACKUP_MODELS.items():
        actual = int(
            connection.execute(
                select(func.count()).select_from(model.__table__)
            ).scalar_one()
        )
        expected = len(payload["tables"][table_name])
        if actual != expected:
            raise BackupRestoreError(
                f"La verificación de '{table_name}' falló: "
                f"se esperaban {expected} registros y se encontraron {actual}."
            )
        restored_counts[table_name] = actual

    if connection.dialect.name == "sqlite":
        violations = connection.exec_driver_sql("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise BackupRestoreError(
                "La verificación de claves foráneas de SQLite detectó inconsistencias."
            )

    return restored_counts


def restore_backup_bytes(content: bytes) -> dict[str, Any]:
    """Reemplaza toda la base usando un backup validado y una única transacción."""

    payload, validation = _load_restorable_payload(content)

    # Debe completarse antes de abrir la transacción destructiva.
    preventive_backup = _create_pre_restore_backup()

    try:
        # Flask-Login pudo abrir una transacción de lectura para cargar current_user.
        db.session.remove()

        with db.engine.begin() as connection:
            if connection.dialect.name == "postgresql":
                # Evita dos restores simultáneos entre workers de la aplicación.
                connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext('pora-caja-restore'))")
                )

            for _, model in reversed(BACKUP_MODELS.items()):
                connection.execute(delete(model.__table__))

            for table_name, model in BACKUP_MODELS.items():
                rows = payload["tables"][table_name]
                if not rows:
                    continue
                values = [_deserialize_row(model, row) for row in rows]
                connection.execute(insert(model.__table__), values)

            _reset_postgresql_sequences(connection, payload)
            restored_counts = _verify_restored_database(connection, payload)

    except Exception as exc:
        db.session.remove()
        if isinstance(exc, BackupRestoreError):
            message = str(exc)
        else:
            message = f"Error durante la restauración: {exc}"
        raise BackupRestoreError(
            message
            + " La transacción fue revertida. "
            + f"Backup preventivo: {preventive_backup.name}."
        ) from exc
    finally:
        db.session.remove()

    return {
        "preventive_backup_name": preventive_backup.name,
        "preventive_backup_path": str(preventive_backup),
        "restored_counts": restored_counts,
        "total_rows": sum(restored_counts.values()),
        "source_engine": validation.get("source_engine"),
        "target_engine": db.engine.dialect.name,
        "format_version": payload.get("format_version"),
    }



def read_preventive_backup_bytes(filename: str) -> bytes:
    """Lee un backup preventivo por su nombre interno seguro."""

    if not isinstance(filename, str):
        raise BackupRestoreError("El backup preventivo solicitado no es válido.")

    safe_name = Path(filename).name
    if (
        safe_name != filename
        or not safe_name.startswith("pre_restore_")
        or not safe_name.endswith(".json")
    ):
        raise BackupRestoreError("El backup preventivo solicitado no es válido.")

    path = Path(current_app.instance_path) / "backups" / safe_name
    if not path.is_file():
        raise BackupRestoreError("El backup preventivo ya no está disponible.")

    content = path.read_bytes()
    if not content or len(content) > MAX_BACKUP_BYTES:
        raise BackupRestoreError("El backup preventivo no tiene un tamaño válido.")
    return content
