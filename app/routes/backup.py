"""Rutas del sistema de backup y restore completo de PORÁ Caja."""

from datetime import datetime
from io import BytesIO
from pathlib import Path

from flask import Blueprint, flash, redirect, request, send_file, session, url_for
from flask_login import login_required
from markupsafe import escape

from app.services.backup_schema import CURRENT_FORMAT_VERSION
from app.services.backup_service import (
    MAX_BACKUP_BYTES,
    BackupRestoreError,
    build_backup_bytes,
    cleanup_staged_restore_files,
    delete_staged_restore,
    get_backup_summary,
    inspect_backup_bytes,
    read_preventive_backup_bytes,
    read_staged_restore_bytes,
    restore_backup_bytes,
    stage_restore_bytes,
)


backup_bp = Blueprint("backup_bp", __name__)


_TABLE_LABELS = {
    "users": "Usuarios",
    "expense_categories": "Categorías de gastos",
    "business_days": "Días de trabajo",
    "shift_records": "Turnos",
    "expense_entries": "Gastos",
}

_PENDING_TOKEN_KEY = "pending_restore_token"
_PENDING_FILENAME_KEY = "pending_restore_filename"
_LAST_PREVENTIVE_BACKUP_KEY = "last_preventive_backup"


def _render_page(body_html: str, **context):
    from app_owners import render_page

    return render_page(body_html, **context)


def _table_rows(summary: dict[str, int]) -> str:
    return "".join(
        f"<tr><td>{escape(_TABLE_LABELS.get(table_name, table_name))}</td>"
        f"<td class='num'>{count}</td></tr>"
        for table_name, count in summary.items()
    )


def _message_list(title: str, messages: list[str], css_class: str) -> str:
    if not messages:
        return ""
    items = "".join(f"<li>{escape(message)}</li>" for message in messages)
    return f"""
    <div class="card {css_class}">
      <h3>{escape(title)}</h3>
      <ul>{items}</ul>
    </div>
    """


def _clear_pending_restore() -> None:
    token = session.pop(_PENDING_TOKEN_KEY, None)
    session.pop(_PENDING_FILENAME_KEY, None)
    delete_staged_restore(token)


def _restore_confirmation(token: str | None) -> str:
    if not token:
        return ""

    return f"""
    <div class="card flash-error">
      <h2>Restaurar este backup</h2>
      <p><strong>Esta operación reemplazará todos los datos actuales.</strong></p>
      <p>
        Antes de modificar la base, el sistema guardará automáticamente un backup
        preventivo completo. Si ocurre un error, la transacción se revertirá.
      </p>
      <form method="post" action="{url_for('backup_bp.backup_restore')}">
        <input type="hidden" name="restore_token" value="{escape(token)}">

        <label style="display:flex; gap:10px; align-items:flex-start; margin:12px 0;">
          <input
            type="checkbox"
            name="confirm_replace"
            value="yes"
            required
            style="width:auto; margin-top:3px;"
          >
          <span>Entiendo que la base actual será reemplazada por el contenido del backup.</span>
        </label>

        <div class="field" style="max-width:360px; margin-bottom:12px;">
          <label for="confirmation"><strong>Escribí RESTAURAR para confirmar</strong></label>
          <input
            id="confirmation"
            name="confirmation"
            type="text"
            autocomplete="off"
            required
          >
        </div>

        <button
          class="btn"
          type="submit"
          style="background:#b91c1c; color:#fff; border-color:#b91c1c;"
        >
          Restaurar base completa
        </button>
      </form>
    </div>
    """


def _preview_body(filename: str, result: dict, restore_token: str | None = None) -> str:
    if result["kind"] == "backup" and result["valid"]:
        status_text = "Backup válido"
        status_class = "pill ok"
    elif result["kind"] == "legacy" and result["valid"]:
        status_text = "Exportación legacy parcial"
        status_class = "pill warn"
    else:
        status_text = "Archivo inválido"
        status_class = "pill bad"

    checksum = result.get("checksum_valid")
    if checksum is True:
        checksum_text = "Correcto"
    elif checksum is False:
        checksum_text = "Incorrecto"
    else:
        checksum_text = "No disponible"

    counts = result.get("table_counts", {})
    rows = _table_rows(counts)
    if rows:
        rows += (
            "<tr><td><strong>Total</strong></td>"
            f"<td class='num'><strong>{result.get('total_rows', 0)}</strong></td></tr>"
        )
    else:
        rows = "<tr><td colspan='2' class='muted'>No se pudieron leer tablas.</td></tr>"

    details = [
        ("Archivo", filename),
        ("Formato", result.get("format") or "No reconocido"),
        (
            "Versión",
            result.get("format_version")
            if result.get("format_version") is not None
            else "—",
        ),
        ("Generado", result.get("generated_at") or "—"),
        ("Motor de origen", result.get("source_engine") or "—"),
        ("Checksum", checksum_text),
    ]
    detail_rows = "".join(
        f"<tr><td>{escape(str(label))}</td><td>{escape(str(value))}</td></tr>"
        for label, value in details
    )

    errors_html = _message_list(
        "Errores detectados", result.get("errors", []), "flash-error"
    )
    warnings_html = _message_list("Advertencias", result.get("warnings", []), "")

    safe_note = ""
    if result.get("restorable"):
        safe_note = (
            "<p class='muted'>El archivo cumple las validaciones necesarias. "
            "La base todavía no fue modificada.</p>"
        )

    confirmation_html = _restore_confirmation(restore_token)

    return f"""
    <h1>Vista previa del backup</h1>

    <div class="card">
      <div class="inline" style="align-items:center;">
        <h2 style="margin:0;">Resultado</h2>
        <span class="{status_class}">{escape(status_text)}</span>
      </div>
      {safe_note}
      <table><tbody>{detail_rows}</tbody></table>
    </div>

    <div class="card">
      <h2>Contenido detectado</h2>
      <table>
        <thead><tr><th>Tabla</th><th class="num">Registros</th></tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>

    {errors_html}
    {warnings_html}
    {confirmation_html}

    <a class="btn" href="{url_for('backup_bp.backup_home')}">Volver a Backup</a>
    """


def _restore_success_body(report: dict) -> str:
    rows = _table_rows(report["restored_counts"])
    return f"""
    <h1>Restauración completada</h1>

    <div class="card flash-ok">
      <h2>Base restaurada correctamente</h2>
      <p>
        Se restauraron <strong>{report['total_rows']}</strong> registros mediante
        una única transacción.
      </p>
      <p>
        Backup preventivo generado:
        <strong>{escape(report['preventive_backup_name'])}</strong>
      </p>
      <a class="btn" href="{url_for('backup_bp.backup_preventive_download')}">
        Descargar backup preventivo
      </a>
    </div>

    <div class="card">
      <h2>Resultado</h2>
      <table>
        <thead><tr><th>Tabla</th><th class="num">Registros</th></tr></thead>
        <tbody>
          {rows}
          <tr>
            <td><strong>Total</strong></td>
            <td class="num"><strong>{report['total_rows']}</strong></td>
          </tr>
        </tbody>
      </table>
      <p class="muted">
        Motor de origen: {escape(str(report.get('source_engine') or '—'))} ·
        Motor restaurado: {escape(str(report.get('target_engine') or '—'))}
      </p>
    </div>

    <a class="btn primary" href="{url_for('dashboard_bp.dashboard_finanzas')}">
      Ir al Panel Central
    </a>
    <a class="btn" href="{url_for('backup_bp.backup_home')}">Volver a Backup</a>
    """


@backup_bp.get("/backup")
@login_required
def backup_home():
    cleanup_staged_restore_files()

    try:
        summary = get_backup_summary()
    except Exception as exc:
        flash(f"No se pudo preparar el backup: {exc}", "error")
        return redirect(url_for("home_bp.home"))

    rows = _table_rows(summary)
    total_rows = sum(summary.values())
    max_mb = MAX_BACKUP_BYTES // (1024 * 1024)

    body = f"""
    <h1>Backup completo</h1>

    <div class="card">
      <h2>Contenido actual</h2>
      <table>
        <thead>
          <tr><th>Tabla</th><th class="num">Registros</th></tr>
        </thead>
        <tbody>
          {rows}
          <tr>
            <td><strong>Total</strong></td>
            <td class="num"><strong>{total_rows}</strong></td>
          </tr>
        </tbody>
      </table>
    </div>

    <div class="card">
      <h2>Generar backup</h2>
      <p>
        Genera un JSON completo y versionado. Incluye todas las columnas,
        identificadores y relaciones actuales.
      </p>
      <p class="muted">Versión del formato: {CURRENT_FORMAT_VERSION}. Guardá este archivo en un lugar seguro.</p>
      <a class="btn primary" href="{url_for('backup_bp.backup_download')}">
        Descargar backup completo
      </a>
    </div>

    <div class="card">
      <h2>Validar y restaurar un backup</h2>
      <p>
        Primero se analiza el archivo y se muestra una vista previa. La restauración
        solo se habilita si todas las validaciones son correctas.
      </p>
      <form method="post" action="{url_for('backup_bp.backup_preview')}" enctype="multipart/form-data">
        <div class="field">
          <label for="backup_file"><strong>Archivo JSON</strong></label>
          <input id="backup_file" name="backup_file" type="file" accept=".json,application/json" required>
        </div>
        <p class="muted">Tamaño máximo: {max_mb} MB.</p>
        <button class="btn primary" type="submit">Validar y ver contenido</button>
      </form>
    </div>
    """
    return _render_page(body, show_nav=True)


@backup_bp.get("/backup/download")
@login_required
def backup_download():
    try:
        content = build_backup_bytes()
    except Exception as exc:
        flash(f"No se pudo generar el backup: {exc}", "error")
        return redirect(url_for("backup_bp.backup_home"))

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"pora_caja_backup_{timestamp}.json"

    response = send_file(
        BytesIO(content),
        mimetype="application/json",
        as_attachment=True,
        download_name=filename,
        max_age=0,
    )
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return response


@backup_bp.post("/backup/preview")
@login_required
def backup_preview():
    uploaded = request.files.get("backup_file")
    if uploaded is None or not uploaded.filename:
        flash("Seleccioná un archivo JSON.", "error")
        return redirect(url_for("backup_bp.backup_home"))

    original_name = Path(uploaded.filename).name
    if Path(original_name).suffix.lower() != ".json":
        flash("El archivo debe tener extensión .json.", "error")
        return redirect(url_for("backup_bp.backup_home"))

    content = uploaded.stream.read(MAX_BACKUP_BYTES + 1)
    result = inspect_backup_bytes(content)

    _clear_pending_restore()
    restore_token = None

    if result.get("restorable"):
        try:
            restore_token = stage_restore_bytes(content)
            session[_PENDING_TOKEN_KEY] = restore_token
            session[_PENDING_FILENAME_KEY] = original_name
        except Exception as exc:
            result["valid"] = False
            result["restorable"] = False
            result.setdefault("errors", []).append(
                f"No se pudo preparar el archivo para restauración: {exc}"
            )

    return _render_page(
        _preview_body(original_name, result, restore_token),
        show_nav=True,
    )


@backup_bp.post("/backup/restore")
@login_required
def backup_restore():
    submitted_token = request.form.get("restore_token", "")
    pending_token = session.get(_PENDING_TOKEN_KEY)

    if not pending_token or submitted_token != pending_token:
        flash(
            "La confirmación de restauración venció o no es válida. Volvé a cargar el backup.",
            "error",
        )
        return redirect(url_for("backup_bp.backup_home"))

    if request.form.get("confirm_replace") != "yes":
        flash("Debés confirmar que la base actual será reemplazada.", "error")
        return redirect(url_for("backup_bp.backup_home"))

    if request.form.get("confirmation", "").strip() != "RESTAURAR":
        flash("La palabra de confirmación debe ser exactamente RESTAURAR.", "error")
        return redirect(url_for("backup_bp.backup_home"))

    try:
        content = read_staged_restore_bytes(pending_token)
        report = restore_backup_bytes(content)
    except BackupRestoreError as exc:
        flash(str(exc), "error")
        return redirect(url_for("backup_bp.backup_home"))
    except Exception as exc:
        flash(f"No se pudo completar la restauración: {exc}", "error")
        return redirect(url_for("backup_bp.backup_home"))

    _clear_pending_restore()
    session[_LAST_PREVENTIVE_BACKUP_KEY] = report["preventive_backup_name"]
    return _render_page(_restore_success_body(report), show_nav=True)


@backup_bp.get("/backup/preventive-download")
@login_required
def backup_preventive_download():
    filename = session.get(_LAST_PREVENTIVE_BACKUP_KEY)
    if not filename:
        flash("No hay un backup preventivo disponible en esta sesión.", "error")
        return redirect(url_for("backup_bp.backup_home"))

    try:
        content = read_preventive_backup_bytes(filename)
    except BackupRestoreError as exc:
        flash(str(exc), "error")
        return redirect(url_for("backup_bp.backup_home"))

    response = send_file(
        BytesIO(content),
        mimetype="application/json",
        as_attachment=True,
        download_name=filename,
        max_age=0,
    )
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return response
