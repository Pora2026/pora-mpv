from datetime import datetime

from flask import Blueprint, request, send_file, url_for
from flask_login import login_required

from app.services.management_report_service import build_management_report, last_complete_month
from app.services.management_report_pdf import build_management_pdf
from app.utils.money import ars

reports_bp = Blueprint("reports_bp", __name__, url_prefix="/informes")


def _render_page(*args, **kwargs):
    from app_owners import render_page
    return render_page(*args, **kwargs)


def _parse_period():
    mode = request.values.get("mode", "last_month")
    if mode == "custom":
        start_raw = request.values.get("start", "")
        end_raw = request.values.get("end", "")
        if not start_raw or not end_raw:
            raise ValueError("Indicá las fechas Desde y Hasta.")
        return mode, datetime.strptime(start_raw, "%Y-%m-%d").date(), datetime.strptime(end_raw, "%Y-%m-%d").date()
    start, end = last_complete_month()
    return "last_month", start, end


@reports_bp.get("/")
@login_required
def index():
    start, end = last_complete_month()
    body = f"""
    <h1>Informe Gerencial</h1>
    <p class="muted">Generá un informe ejecutivo del último mes completo o de un período personalizado.</p>
    <div class="card" style="max-width:760px">
      <form method="get" action="{url_for('reports_bp.preview')}">
        <label><b>Período</b></label>
        <div style="display:flex;gap:18px;flex-wrap:wrap;margin:10px 0 18px">
          <label><input type="radio" name="mode" value="last_month" checked onchange="toggleDates()"> Último mes completo ({start.strftime('%d/%m/%Y')} al {end.strftime('%d/%m/%Y')})</label>
          <label><input type="radio" name="mode" value="custom" onchange="toggleDates()"> Período personalizado</label>
        </div>
        <div id="customDates" style="display:none;gap:12px;flex-wrap:wrap;margin-bottom:18px">
          <label>Desde<br><input type="date" name="start" style="padding:9px"></label>
          <label>Hasta<br><input type="date" name="end" style="padding:9px"></label>
        </div>
        <button class="btn primary" type="submit">Vista previa</button>
      </form>
    </div>
    <script>
      function toggleDates(){{
        const custom=document.querySelector('input[name="mode"]:checked').value==='custom';
        document.getElementById('customDates').style.display=custom?'flex':'none';
      }}
    </script>
    """
    return _render_page(body, show_nav=True)


@reports_bp.get("/vista-previa")
@login_required
def preview():
    try:
        mode, start, end = _parse_period()
        data = build_management_report(start, end)
    except ValueError as exc:
        body = f'<h1>Informe Gerencial</h1><div class="card flash-error">{exc}</div><a class="btn" href="{url_for("reports_bp.index")}">Volver</a>'
        return _render_page(body, show_nav=True), 400

    def money(v):
        return "—" if v is None else ars(v)
    def pct(v):
        return "—" if v is None else f"{v:.1f}%".replace(".", ",")

    pdf_url = url_for("reports_bp.pdf") + f"?mode={mode}&start={start.isoformat()}&end={end.isoformat()}"
    body = f"""
    <h1>Informe Gerencial</h1>
    <p class="muted">{start.strftime('%d/%m/%Y')} al {end.strftime('%d/%m/%Y')} · {data['days_loaded']} días cargados</p>
    <div class="grid3">
      <div class="card"><div class="muted">Ingresos brutos</div><h2>{money(data['gross_income'])}</h2></div>
      <div class="card"><div class="muted">Gastos</div><h2>{money(data['expenses'])}</h2></div>
      <div class="card"><div class="muted">Ganancia calculada</div><h2>{money(data['calculated_profit'])}</h2><div class="muted">Margen {pct(data['calculated_margin'])}</div></div>
      <div class="card"><div class="muted">Ingresos líquidos</div><h2>{money(data['liquid_income'])}</h2></div>
      <div class="card"><div class="muted">Ganancia líquida</div><h2>{money(data['liquid_profit'])}</h2><div class="muted">Margen {pct(data['liquid_margin'])}</div></div>
      <div class="card"><div class="muted">Saldo real disponible</div><h2>{money(data['actual_balance'])}</h2></div>
    </div>
    <div style="display:flex;gap:10px;margin-top:18px;flex-wrap:wrap">
      <a class="btn primary" href="{pdf_url}">Generar PDF</a>
      <a class="btn" href="{url_for('reports_bp.index')}">Cambiar período</a>
    </div>
    """
    return _render_page(body, show_nav=True)


@reports_bp.get("/pdf")
@login_required
def pdf():
    try:
        _mode, start, end = _parse_period()
        data = build_management_report(start, end)
    except ValueError as exc:
        body = f'<h1>Informe Gerencial</h1><div class="card flash-error">{exc}</div>'
        return _render_page(body, show_nav=True), 400
    stream = build_management_pdf(data)
    filename = f"PORA_Informe_Gerencial_{start.isoformat()}_{end.isoformat()}.pdf"
    return send_file(stream, mimetype="application/pdf", as_attachment=True, download_name=filename)
