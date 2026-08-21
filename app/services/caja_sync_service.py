from __future__ import annotations

from datetime import date
from math import isfinite

from app.extensions import db
from app.models import BusinessDay, ShiftRecord


CONTRACT_VERSION = 1
SOURCE_NAME = "caja"
SHIFT_MAP = {
    "MORNING": "Mañana",
    "AFTERNOON": "Tarde",
}


class CajaSyncValidationError(ValueError):
    """Payload inválido recibido desde CAJA."""


def _number(value, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CajaSyncValidationError(f"{field_name} debe ser numérico.")

    result = float(value)
    if not isfinite(result):
        raise CajaSyncValidationError(f"{field_name} debe ser finito.")
    return result


def _parse_start_date(value) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise CajaSyncValidationError("CAJA_SYNC_START_DATE inválida.") from exc


def validate_day_snapshot(path_day: str, payload: dict, *, start_date="2026-08-13") -> tuple[date, dict]:
    if not isinstance(payload, dict):
        raise CajaSyncValidationError("El cuerpo JSON debe ser un objeto.")

    if payload.get("contract_version") != CONTRACT_VERSION:
        raise CajaSyncValidationError("contract_version no soportado.")

    if payload.get("source") != SOURCE_NAME:
        raise CajaSyncValidationError("source inválido.")

    payload_day = payload.get("day")
    if payload_day != path_day:
        raise CajaSyncValidationError("La fecha del payload no coincide con la URL.")

    try:
        parsed_day = date.fromisoformat(path_day)
    except (TypeError, ValueError) as exc:
        raise CajaSyncValidationError("Fecha inválida; se espera YYYY-MM-DD.") from exc

    minimum_day = _parse_start_date(start_date)
    if parsed_day < minimum_day:
        raise CajaSyncValidationError(
            f"La sincronización CAJA -> PORÁ comienza el {minimum_day.isoformat()}."
        )

    if parsed_day.weekday() == 6:
        raise CajaSyncValidationError("PORÁ no admite días domingo.")

    shifts = payload.get("shifts")
    if not isinstance(shifts, dict):
        raise CajaSyncValidationError("shifts es obligatorio.")

    normalized_shifts = {}
    for code in SHIFT_MAP:
        shift_payload = shifts.get(code)
        if not isinstance(shift_payload, dict):
            raise CajaSyncValidationError(f"Falta el turno {code}.")
        normalized_shifts[code] = {
            "income": _number(shift_payload.get("income"), f"shifts.{code}.income")
        }

    normalized = {
        "shifts": normalized_shifts,
        "real_apps_pending": _number(
            payload.get("real_apps_pending"), "real_apps_pending"
        ),
        "daily_mercadopago": _number(
            payload.get("daily_mercadopago"), "daily_mercadopago"
        ),
        "daily_cash_withdrawn": _number(
            payload.get("daily_cash_withdrawn"), "daily_cash_withdrawn"
        ),
        "operating_cash_balance": _number(
            payload.get("operating_cash_balance"), "operating_cash_balance"
        ),
    }

    return parsed_day, normalized


def apply_day_snapshot(path_day: str, payload: dict, *, start_date="2026-08-13") -> dict:
    """Aplica el snapshot diario de CAJA sin tocar campos manuales de PORÁ.

    Campos propiedad de la integración CAJA -> PORÁ:
      - ingreso Mañana / Tarde;
      - apps pendientes;
      - Mercado Pago diario;
      - efectivo retirado;
      - caja operativa al cierre.

    El resto del BusinessDay, sus notas y sus gastos se conservan intactos.
    """

    parsed_day, data = validate_day_snapshot(
        path_day,
        payload,
        start_date=start_date,
    )

    bday = BusinessDay.query.filter_by(day=parsed_day).first()
    created = bday is None
    if created:
        bday = BusinessDay(day=parsed_day, note="", status="draft")
        db.session.add(bday)
        db.session.flush()

    existing_shifts = {s.shift: s for s in bday.shifts}
    for source_code, pora_name in SHIFT_MAP.items():
        shift = existing_shifts.get(pora_name)
        if shift is None:
            shift = ShiftRecord(business_day=bday, shift=pora_name)
            db.session.add(shift)
            existing_shifts[pora_name] = shift

        shift.income = data["shifts"][source_code]["income"]
        # El endpoint sólo recibe días consolidados con ambos cierres de CAJA.
        shift.is_closed = True

    bday.real_apps_pending = data["real_apps_pending"]
    bday.daily_mercadopago = data["daily_mercadopago"]
    bday.daily_cash_withdrawn = data["daily_cash_withdrawn"]
    bday.operating_cash_balance = data["operating_cash_balance"]
    bday.status = "complete"

    return {
        "created": created,
        "day": parsed_day.isoformat(),
    }
