from __future__ import annotations

import hmac

from flask import Blueprint, current_app, jsonify, request

from app.extensions import db
from app.services.caja_sync_service import (
    CONTRACT_VERSION,
    CajaSyncValidationError,
    apply_day_snapshot,
)


caja_api_bp = Blueprint("caja_api_bp", __name__)


def _configured_token() -> str:
    return str(current_app.config.get("CAJA_SYNC_TOKEN") or "").strip()


def _is_authorized() -> bool:
    expected = _configured_token()
    if not expected:
        return False

    header = (request.headers.get("Authorization") or "").strip()
    prefix = "Bearer "
    if not header.startswith(prefix):
        return False

    supplied = header[len(prefix):].strip()
    return bool(supplied) and hmac.compare_digest(supplied, expected)


def _auth_error():
    if not _configured_token():
        return jsonify(ok=False, error="receiver_not_configured"), 503
    return jsonify(ok=False, error="unauthorized"), 401


@caja_api_bp.get("/api/v1/caja/health")
def caja_api_health():
    if not _is_authorized():
        return _auth_error()
    return jsonify(
        ok=True,
        receiver="pora",
        contract_version=CONTRACT_VERSION,
        sync_start_date=current_app.config.get("CAJA_SYNC_START_DATE"),
    )


@caja_api_bp.put("/api/v1/caja/days/<day>")
def caja_api_upsert_day(day):
    if not _is_authorized():
        return _auth_error()

    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify(ok=False, error="invalid_json"), 400

    try:
        result = apply_day_snapshot(
            day,
            payload,
            start_date=current_app.config.get("CAJA_SYNC_START_DATE", "2026-08-13"),
        )
        db.session.commit()
    except CajaSyncValidationError as exc:
        db.session.rollback()
        return jsonify(ok=False, error="invalid_payload", detail=str(exc)), 400
    except Exception:
        db.session.rollback()
        current_app.logger.exception("Error aplicando sincronización CAJA -> PORÁ")
        return jsonify(ok=False, error="internal_error"), 500

    action = "created" if result["created"] else "updated"
    status_code = 201 if result["created"] else 200
    return jsonify(ok=True, action=action, day=result["day"]), status_code
