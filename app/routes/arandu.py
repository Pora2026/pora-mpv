"""Interfaz, API cerrada y controles del MVP de Arandú."""

import html
import json
import re
import secrets
from datetime import date, datetime, timezone
from io import BytesIO

from flask import (
    Blueprint, abort, current_app, flash, jsonify, redirect, request,
    send_file, session, url_for,
)
from flask_login import current_user, login_required

from app.extensions import db
from app.models import User
from app.models.arandu import AranduAuditEvent, AranduDocument, AranduMessage, AranduRule
from app.services.arandu_access_service import user_can_access_arandu
from app.services.arandu_alert_service import get_period_alerts
from app.services.arandu_document_service import DocumentValidationError, save_document
from app.services.arandu_finance_service import (
    compare_financial_periods, get_expense_breakdown, get_financial_period_summary,
)
from app.services.arandu_orchestrator import (
    AranduUnavailableError, ask_arandu, get_shared_conversation,
)
from app.services.arandu_report_service import build_excel_report, build_pdf_report
from app.utils.dates import month_range, parse_ymd
from app.utils.money import ars


arandu_bp = Blueprint("arandu_bp", __name__, url_prefix="/arandu")


def _require_access():
    if not user_can_access_arandu(current_user):
        abort(403)


def _csrf_token():
    token = session.get("arandu_csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["arandu_csrf_token"] = token
    return token


def _require_csrf():
    supplied = request.form.get("csrf_token", "")
    expected = session.get("arandu_csrf_token", "")
    if not supplied or not expected or not secrets.compare_digest(supplied, expected):
        abort(400, description="Token de seguridad inválido.")


def _validated_period(from_value=None, to_value=None):
    try:
        default_start, default_end = month_range(date.today())
        start_date = parse_ymd(from_value) if from_value else default_start
        end_date = parse_ymd(to_value) if to_value else default_end
        analysis_start = parse_ymd(current_app.config["ARANDU_ANALYSIS_START_DATE"])
    except (TypeError, ValueError, KeyError):
        abort(400, description="El período solicitado no es válido.")
    if start_date > end_date:
        abort(400, description="La fecha inicial no puede superar la fecha final.")
    if start_date < analysis_start:
        abort(400, description=f"Arandú analiza desde {analysis_start.isoformat()}.")
    if (end_date - start_date).days > 366:
        abort(400, description="El período máximo permitido es de 367 días.")
    return start_date, end_date


def _render_page(*args, **kwargs):
    from app_owners import render_page
    return render_page(*args, **kwargs)


def _audit(event_type, payload=None):
    db.session.add(AranduAuditEvent(
        event_type=event_type,
        user_id=current_user.id,
        payload_json=json.dumps(payload or {}, ensure_ascii=False, default=str),
    ))
    db.session.commit()


@arandu_bp.get("/")
@login_required
def index():
    _require_access()
    period_start, period_end = month_range(date.today())
    summary = get_financial_period_summary(period_start, period_end)
    period_alerts = get_period_alerts(period_start, period_end)
    conversation = get_shared_conversation(current_user.id)
    messages = AranduMessage.query.filter_by(conversation_id=conversation.id).order_by(AranduMessage.id.desc()).limit(40).all()
    messages.reverse()
    message_user_ids = {item.user_id for item in messages if item.user_id is not None}
    message_users = {
        user.id: user.username
        for user in User.query.filter(User.id.in_(message_user_ids)).all()
    } if message_user_ids else {}
    documents = AranduDocument.query.order_by(AranduDocument.id.desc()).limit(20).all()
    rules = AranduRule.query.order_by(AranduRule.id.desc()).limit(20).all()
    token = _csrf_token()

    calculated, liquid = summary["calculated"], summary["liquid"]
    real, quality = summary["real_accumulated"], summary["data_quality"]
    real_label = "—" if real["balance"] is None else ars(real["balance"])
    real_date = "Sin arqueo cargado" if real["as_of"] is None else f"Al {real['as_of']}"
    alert_html = "".join(
        f"<div class='card'><span class='pill {'bad' if item['severity']=='critical' else 'warn' if item['severity']=='warning' else ''}'>{html.escape(item['severity'].upper())}</span> "
        f"<strong>{html.escape(item['title'])}</strong><p class='muted'>{html.escape(item['detail'])}</p></div>"
        for item in period_alerts["alerts"]
    ) or "<p class='muted'>Sin advertencias para el período.</p>"
    message_html = "".join(
        f"<div class='card' style='margin:8px 0;background:{'#eff6ff' if item.role=='user' else '#f9fafb'}'>"
        f"<strong>{html.escape(message_users.get(item.user_id, 'Usuario')) if item.role=='user' else 'Arandú'}</strong>"
        f"<div style='white-space:pre-wrap;margin-top:6px'>{html.escape(item.content)}</div></div>"
        for item in messages
    ) or "<p class='muted'>Todavía no hay conversaciones.</p>"
    document_html = "".join(
        f"<li><a href='{url_for('arandu_bp.download_document', document_id=item.id)}'>{html.escape(item.original_name)}</a> "
        f"<span class='muted'>({item.size_bytes // 1024} KB · {html.escape(item.status)})</span></li>"
        for item in documents
    ) or "<li class='muted'>Sin documentos.</li>"
    rule_html = "".join(
        f"<div class='card' style='margin:8px 0'><strong>{html.escape(item.title)}</strong> "
        f"<span class='pill {'ok' if item.status=='approved' else 'warn'}'>{html.escape(item.status)}</span>"
        f"<p>{html.escape(item.content)}</p>"
        + (f"<form method='post' action='{url_for('arandu_bp.approve_rule', rule_id=item.id)}'><input type='hidden' name='csrf_token' value='{token}'><button class='btn primary' type='submit'>Aprobar regla</button></form>" if item.status == "proposed" else "")
        + "</div>" for item in rules
    ) or "<p class='muted'>Sin reglas adicionales.</p>"

    body = f"""
    <h1>Arandú</h1><p class="muted">CFO virtual consultivo · espacio compartido y auditable.</p>
    <div class="grid3">
      <div class="card kpi profit"><div class="label">Ganancia calculada</div><div class="value">{ars(calculated['profit'])}</div><p class="muted small">Mes actual · No incluye saldo anterior</p></div>
      <div class="card kpi blue"><div class="label">Ganancia líquida</div><div class="value">{ars(liquid['profit'])}</div><p class="muted small">Mes actual · No incluye saldo anterior</p></div>
      <div class="card kpi income"><div class="label">Ganancia real acumulada</div><div class="value">{real_label}</div><p class="muted small">{real_date} · Puede incluir períodos anteriores</p></div>
    </div>
    <div class="card" style="margin-top:14px"><h3>Calidad de datos</h3><p>Días cargados: <strong>{quality['loaded_working_days']}</strong> de <strong>{quality['expected_working_days_to_date']}</strong>. Faltantes: <strong>{len(quality['missing_dates'])}</strong>.</p></div>
    <h2>Advertencias</h2>{alert_html}
    <div class="card" style="margin-top:14px"><h2>Chat compartido</h2>{message_html}
      <form method="post" action="{url_for('arandu_bp.chat')}"><input type="hidden" name="csrf_token" value="{token}"><textarea name="question" rows="4" maxlength="8000" required placeholder="Consultale a Arandú..."></textarea><button class="btn primary" type="submit">Enviar</button></form>
    </div>
    <div class="grid3" style="margin-top:14px">
      <div class="card"><h3>Documentos</h3><ul>{document_html}</ul><form method="post" enctype="multipart/form-data" action="{url_for('arandu_bp.upload_document')}"><input type="hidden" name="csrf_token" value="{token}"><input type="file" name="document" required><button class="btn" type="submit">Cargar</button></form><p class="muted small">PDF, DOCX, XLSX, CSV, JSON, texto e imágenes. Máximo 15 MB. No se ejecutan macros ni scripts.</p></div>
      <div class="card"><h3>Informes</h3><a class="btn primary" href="{url_for('arandu_bp.report_pdf', **{'from': period_start.isoformat(), 'to': period_end.isoformat()})}">PDF</a> <a class="btn" href="{url_for('arandu_bp.report_excel', **{'from': period_start.isoformat(), 'to': period_end.isoformat()})}">Excel</a></div>
      <div class="card"><h3>Nueva regla permanente</h3><form method="post" action="{url_for('arandu_bp.propose_rule')}"><input type="hidden" name="csrf_token" value="{token}"><input name="title" maxlength="200" required placeholder="Título"><textarea name="content" rows="3" maxlength="4000" required placeholder="Definición exacta"></textarea><button class="btn" type="submit">Proponer</button></form></div>
    </div><h2>Reglas permanentes</h2>{rule_html}
    """
    return _render_page(body, show_nav=True)


@arandu_bp.post("/chat")
@login_required
def chat():
    _require_access(); _require_csrf()
    try:
        ask_arandu(current_user, request.form.get("question")); flash("Arandú respondió la consulta.", "ok")
    except (ValueError, AranduUnavailableError) as exc:
        flash(str(exc), "error")
    return redirect(url_for("arandu_bp.index"))


@arandu_bp.post("/documents")
@login_required
def upload_document():
    _require_access(); _require_csrf()
    try:
        document = save_document(request.files.get("document"), current_user.id)
        _audit("document.uploaded", {"document_id": document.id, "sha256": document.sha256}); flash("Documento cargado y registrado.", "ok")
    except (AttributeError, DocumentValidationError) as exc:
        flash(str(exc) or "No se recibió un archivo.", "error")
    return redirect(url_for("arandu_bp.index"))


@arandu_bp.get("/documents/<int:document_id>")
@login_required
def download_document(document_id):
    _require_access(); document = db.session.get(AranduDocument, document_id)
    if document is None: abort(404)
    _audit("document.downloaded", {"document_id": document.id})
    return send_file(BytesIO(document.file_content), mimetype=document.mime_type, as_attachment=True, download_name=document.original_name)


@arandu_bp.post("/rules")
@login_required
def propose_rule():
    _require_access(); _require_csrf()
    title, content = (request.form.get("title") or "").strip(), (request.form.get("content") or "").strip()
    if not title or not content:
        flash("La regla necesita título y contenido.", "error"); return redirect(url_for("arandu_bp.index"))
    base_key = re.sub(r"[^a-z0-9]+", "-", title.casefold()).strip("-")[:90] or "regla"
    rule = AranduRule(rule_key=f"{base_key}-{secrets.token_hex(4)}", title=title, content=content, proposed_by_user_id=current_user.id)
    db.session.add(rule); db.session.commit(); _audit("rule.proposed", {"rule_id": rule.id})
    flash("Regla propuesta. Debe aprobarse antes de influir en Arandú.", "ok"); return redirect(url_for("arandu_bp.index"))


@arandu_bp.post("/rules/<int:rule_id>/approve")
@login_required
def approve_rule(rule_id):
    _require_access(); _require_csrf(); rule = db.session.get(AranduRule, rule_id)
    if rule is None: abort(404)
    if rule.status != "proposed": flash("La regla ya fue procesada.", "error")
    else:
        rule.status = "approved"; rule.approved_by_user_id = current_user.id; rule.approved_at = datetime.now(timezone.utc)
        db.session.commit(); _audit("rule.approved", {"rule_id": rule.id}); flash("Regla permanente aprobada.", "ok")
    return redirect(url_for("arandu_bp.index"))


@arandu_bp.get("/reports/pdf")
@login_required
def report_pdf():
    _require_access(); start, end = _validated_period(request.args.get("from"), request.args.get("to"))
    output = build_pdf_report(start, end)
    _audit("report.generated", {"format": "pdf", "from": start, "to": end})
    return send_file(output, mimetype="application/pdf", as_attachment=True, download_name=f"arandu_{start}_{end}.pdf")


@arandu_bp.get("/reports/excel")
@login_required
def report_excel():
    _require_access(); start, end = _validated_period(request.args.get("from"), request.args.get("to"))
    output = build_excel_report(start, end)
    _audit("report.generated", {"format": "xlsx", "from": start, "to": end})
    return send_file(output, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", as_attachment=True, download_name=f"arandu_{start}_{end}.xlsx")


@arandu_bp.get("/api/v1/financial-summary")
@login_required
def financial_summary():
    _require_access(); start, end = _validated_period(request.args.get("from"), request.args.get("to")); return jsonify(get_financial_period_summary(start, end))


@arandu_bp.get("/api/v1/expense-breakdown")
@login_required
def expense_breakdown():
    _require_access(); start, end = _validated_period(request.args.get("from"), request.args.get("to")); summary = get_financial_period_summary(start, end); return jsonify(get_expense_breakdown(start, end, period_summary=summary))


@arandu_bp.get("/api/v1/period-comparison")
@login_required
def period_comparison():
    _require_access()
    if not request.args.get("compare_from") or not request.args.get("compare_to"): abort(400, description="La comparación requiere compare_from y compare_to.")
    current_start, current_end = _validated_period(request.args.get("from"), request.args.get("to")); comparison_start, comparison_end = _validated_period(request.args.get("compare_from"), request.args.get("compare_to")); return jsonify(compare_financial_periods(current_start, current_end, comparison_start, comparison_end))


@arandu_bp.get("/api/v1/alerts")
@login_required
def alerts():
    _require_access(); start, end = _validated_period(request.args.get("from"), request.args.get("to")); return jsonify(get_period_alerts(start, end))
