import json
from datetime import date

from flask import Blueprint, request
from flask_login import login_required
from sqlalchemy import func

from app.extensions import db
from app.models import BusinessDay, ExpenseCategory, ExpenseEntry
from app.utils.money import ars
from app.utils.dates import parse_ymd, iso, fmt_date_ar, month_range

io_bp = Blueprint("io_bp", __name__)


def _owners():
    from app_owners import render_page, range_series, period_previous
    return render_page, range_series, period_previous


@io_bp.get("/io")
@login_required
def io_dashboard():
    render_page, range_series, period_previous = _owners()
    avg_week_income = 0.0
    avg_week_expense = 0.0
    avg_week_profit = 0.0

    today = date.today()
    d1s = (request.args.get("from") or "").strip()
    d2s = (request.args.get("to") or "").strip()

    compare_mode = (request.args.get("compare_mode") or "prev").strip()
    c1s = (request.args.get("cfrom") or "").strip()
    c2s = (request.args.get("cto") or "").strip()

    if d1s and d2s:
        try:
            d1 = parse_ymd(d1s)
            d2 = parse_ymd(d2s)
        except ValueError:
            d1, d2 = month_range(today)
            d1s, d2s = iso(d1), iso(d2)
    else:
        d1, d2 = month_range(today)
        d1s, d2s = iso(d1), iso(d2)

    if d1 > d2:
        d1, d2 = d2, d1
        d1s, d2s = iso(d1), iso(d2)

    series = range_series(d1, d2)
    income = sum(x["income"] for x in series)
    expense = sum(x["expense_total"] for x in series)
    profit = income - expense

    weekly = {}
    for x in series:
        d = parse_ymd(x["date"])
        yw = d.isocalendar()[:2]
        weekly.setdefault(yw, {"income": 0.0, "expense": 0.0})
        weekly[yw]["income"] += x["income"]
        weekly[yw]["expense"] += x["expense_total"]

    weekly_rows = []
    for (y, w), v in sorted(weekly.items()):
        weekly_rows.append(
            {"label": f"{y}-W{w:02d}", "income": v["income"], "expense": v["expense"], "profit": v["income"] - v["expense"]}
        )

    if weekly_rows:
        avg_week_income = (sum(r["income"] for r in weekly_rows) / len(weekly_rows))
        avg_week_expense = (sum(r["expense"] for r in weekly_rows) / len(weekly_rows))
        avg_week_profit = (sum(r["profit"] for r in weekly_rows) / len(weekly_rows))

    monthly = {}
    for x in series:
        d = parse_ymd(x["date"])
        key = f"{d.year}-{d.month:02d}"
        monthly.setdefault(key, {"income": 0.0, "expense": 0.0})
        monthly[key]["income"] += x["income"]
        monthly[key]["expense"] += x["expense_total"]

    monthly_rows = [
        {"label": k, "income": v["income"], "expense": v["expense"], "profit": v["income"] - v["expense"]}
        for k, v in sorted(monthly.items())
    ]

    cat_rows = (
        db.session.query(
            ExpenseCategory.kind,
            ExpenseCategory.name,
            func.coalesce(func.sum(ExpenseEntry.amount), 0.0).label("total"),
        )
        .join(ExpenseEntry, ExpenseEntry.category_id == ExpenseCategory.id)
        .join(BusinessDay, BusinessDay.id == ExpenseEntry.business_day_id)
        .filter(BusinessDay.day >= d1, BusinessDay.day <= d2)
        .group_by(ExpenseCategory.kind, ExpenseCategory.name)
        .order_by(func.sum(ExpenseEntry.amount).desc())
        .all()
    )

    def _cat_row_html(r):
        kind = "Fijo" if r.kind == "fixed" else "Variable"
        return f"<tr><td>{kind}</td><td>{r.name}</td><td class='num'>{ars(r.total)}</td></tr>"

    if not cat_rows:
        cat_rank_html = (
            "<div class='muted'>"
            "No hay gastos por categorías en este rango (si venís de Excel, todavía no cargaste detalles por categoría)."
            "</div>"
        )
    else:
        top = cat_rows[:3]
        rest = cat_rows[3:50]
        top_html = "".join(_cat_row_html(r) for r in top)
        rest_html = "".join(_cat_row_html(r) for r in rest)
        cat_rank_html = """
        <table>
          <thead><tr><th>Tipo</th><th>Categoría</th><th class='num'>Total</th></tr></thead>
          <tbody>{top_html}</tbody>
        </table>
        """.format(top_html=top_html)
        if rest:
            cat_rank_html += """
            <details style="margin-top:10px;">
              <summary>Ver más</summary>
              <table style="margin-top:10px;">
                <thead><tr><th>Tipo</th><th>Categoría</th><th class='num'>Total</th></tr></thead>
                <tbody>{rest_html}</tbody>
              </table>
            </details>
            """.format(rest_html=rest_html)

    top_cats = [(r.kind, r.name) for r in cat_rows[:6]]
    trace = {}
    top_cat_objs = []
    for kind, name in top_cats:
        c = ExpenseCategory.query.filter_by(kind=kind, name=name).first()
        if c:
            top_cat_objs.append(c)
    top_cat_ids = [c.id for c in top_cat_objs]
    top_cat_names = {c.id: c.name for c in top_cat_objs}

    dialect = db.engine.dialect.name
    ym_expr = func.to_char(BusinessDay.day, "YYYY-MM") if dialect == "postgresql" else func.strftime("%Y-%m", BusinessDay.day)

    rows_tr = []
    if top_cat_ids:
        rows_tr = (
            db.session.query(
                ym_expr.label("ym"),
                ExpenseEntry.category_id,
                func.coalesce(func.sum(ExpenseEntry.amount), 0.0).label("total"),
            )
            .join(BusinessDay, BusinessDay.id == ExpenseEntry.business_day_id)
            .filter(BusinessDay.day >= d1, BusinessDay.day <= d2)
            .filter(ExpenseEntry.category_id.in_(top_cat_ids))
            .group_by(ym_expr, ExpenseEntry.category_id)
            .order_by(ym_expr)
            .all()
        )

    for r in rows_tr:
        trace.setdefault(r.ym, {})
        trace[r.ym][top_cat_names.get(r.category_id, str(r.category_id))] = float(r.total or 0.0)

    trace_months = sorted(trace.keys())
    trace_labels = trace_months
    trace_datasets = []
    for cid in top_cat_ids:
        name = top_cat_names[cid]
        data = []
        for m in trace_months:
            data.append(trace.get(m, {}).get(name, 0.0))
        trace_datasets.append({"label": name, "data": data})

    if compare_mode == "custom" and c1s and c2s:
        try:
            cd1 = parse_ymd(c1s)
            cd2 = parse_ymd(c2s)
        except ValueError:
            cd1, cd2 = period_previous(d1, d2)
            c1s, c2s = iso(cd1), iso(cd2)
    else:
        cd1, cd2 = period_previous(d1, d2)
        c1s, c2s = iso(cd1), iso(cd2)

    if cd1 > cd2:
        cd1, cd2 = cd2, cd1
        c1s, c2s = iso(cd1), iso(cd2)

    cseries = range_series(cd1, cd2)
    cincome = sum(x["income"] for x in cseries)
    cexpense = sum(x["expense_total"] for x in cseries)
    cprofit = cincome - cexpense

    def delta(a, b):
        return a - b

    def delta_pct(a, b):
        if b == 0:
            return None
        return (a - b) / b * 100.0

    di = delta(income, cincome)
    de = delta(expense, cexpense)
    dp = delta(profit, cprofit)
    dip = delta_pct(income, cincome)
    dep = delta_pct(expense, cexpense)
    dpp = delta_pct(profit, cprofit)

    def fmt_pct(x):
        if x is None:
            return "—"
        return f"{x:+.1f}%"

    wk_html = ""
    if not weekly_rows:
        wk_html = "<tr><td colspan='4' class='muted'>Sin datos</td></tr>"
    else:
        for r in weekly_rows[-14:]:
            wk_html += (
                f"<tr><td>{r['label']}</td>"
                f"<td class='num'>{ars(r['income'])}</td>"
                f"<td class='num'>{ars(r['expense'])}</td>"
                f"<td class='num'>{ars(r['profit'])}</td></tr>"
            )

    mo_html = ""
    if not monthly_rows:
        mo_html = "<tr><td colspan='4' class='muted'>Sin datos</td></tr>"
    else:
        for r in monthly_rows:
            mo_html += (
                f"<tr><td>{r['label']}</td>"
                f"<td class='num'>{ars(r['income'])}</td>"
                f"<td class='num'>{ars(r['expense'])}</td>"
                f"<td class='num'>{ars(r['profit'])}</td></tr>"
            )

    trace_payload = {"labels": trace_labels, "datasets": trace_datasets}
    trace_json = json.dumps(trace_payload, ensure_ascii=False)

    body = f"""
    <h1>Gestión de Ingresos y Gastos</h1>

    <div class="card">
      <form method="get" action="/io">
        <div class="row-actions">
          <div class="field">
            <label>Desde</label>
            <input type="date" name="from" value="{d1s}" />
          </div>
          <div class="field">
            <label>Hasta</label>
            <input type="date" name="to" value="{d2s}" />
          </div>

          <div class="field">
            <label>Comparar contra</label>
            <select name="compare_mode">
              <option value="prev" {"selected" if compare_mode=="prev" else ""}>Período anterior (mismo largo)</option>
              <option value="custom" {"selected" if compare_mode=="custom" else ""}>Rango personalizado</option>
            </select>
          </div>

          <div class="field">
            <label>Comparar Desde</label>
            <input type="date" name="cfrom" value="{c1s}" />
          </div>
          <div class="field">
            <label>Comparar Hasta</label>
            <input type="date" name="cto" value="{c2s}" />
          </div>

          <div style="min-width:160px;">
            <label>&nbsp;</label>
            <button class="btn primary" type="submit" style="width:100%;">Aplicar</button>
          </div>
        </div>

        <p class="muted" style="margin-top:10px;">
          Rango: {fmt_date_ar(d1)} a {fmt_date_ar(d2)} (Domingos excluidos).
          Comparación: {fmt_date_ar(cd1)} a {fmt_date_ar(cd2)}.
        </p>
      </form>
    </div>

    <div class="grid3">
      <div class="card kpi income">
        <div class="label">Ingresos (rango)</div>
        <div class="value">{ars(income)}</div>
      </div>
      <div class="card kpi expense">
        <div class="label">Gastos (rango)</div>
        <div class="value">{ars(expense)}</div>
      </div>
      <div class="card kpi profit">
        <div class="label">Ganancia (rango)</div>
        <div class="value">{ars(profit)}</div>
      </div>
    </div>

    <div class="grid3">
      <div class="card kpi">
        <div class="label">Promedio semanal (ingresos)</div>
        <div class="value">{ars(avg_week_income)}</div>
      </div>
      <div class="card kpi">
        <div class="label">Promedio semanal (gastos)</div>
        <div class="value">{ars(avg_week_expense)}</div>
      </div>
      <div class="card kpi">
        <div class="label">Promedio semanal (ganancia)</div>
        <div class="value">{ars(avg_week_profit)}</div>
        <div class="muted">En semanas con data</div>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <h3>Comparativa vs período elegido</h3>
        <table>
          <thead>
            <tr>
              <th>Métrica</th>
              <th class="num">Actual</th>
              <th class="num">Comparación</th>
              <th class="num">Δ</th>
              <th class="num">Δ%</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Ingresos</td><td class="num">{ars(income)}</td><td class="num">{ars(cincome)}</td><td class="num">{ars(di)}</td><td class="num">{fmt_pct(dip)}</td></tr>
            <tr><td>Gastos</td><td class="num">{ars(expense)}</td><td class="num">{ars(cexpense)}</td><td class="num">{ars(de)}</td><td class="num">{fmt_pct(dep)}</td></tr>
            <tr><td>Ganancia</td><td class="num">{ars(profit)}</td><td class="num">{ars(cprofit)}</td><td class="num">{ars(dp)}</td><td class="num">{fmt_pct(dpp)}</td></tr>
          </tbody>
        </table>
      </div>

      <div class="card">
        <h3>Ranking de categorías (gastos)</h3>
        {cat_rank_html}
        <p class="muted" style="margin-top:10px;">
          Nota: solo aparece si cargaste gastos con categorías (no alcanza con el Excel legacy).
        </p>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <h3>Ingresos/Gastos por semana (últimas 14 semanas con data)</h3>
        <table>
          <thead><tr><th>Semana</th><th class="num">Ingresos</th><th class="num">Gastos</th><th class="num">Ganancia</th></tr></thead>
          <tbody>{wk_html}</tbody>
        </table>
      </div>

      <div class="card">
        <h3>Ingresos/Gastos por mes (en rango)</h3>
        <table>
          <thead><tr><th>Mes</th><th class="num">Ingresos</th><th class="num">Gastos</th><th class="num">Ganancia</th></tr></thead>
          <tbody>{mo_html}</tbody>
        </table>
      </div>
    </div>

    <div class="card">
      <h3>Trazabilidad mensual (Top categorías)</h3>
      <div class="chartbox"><canvas id="traceChart"></canvas></div>
      <p class="muted" style="margin-top:10px;">
        (Lo dejamos como está por ahora, lo corregimos después).
      </p>
    </div>

    <script>
      const trace = {trace_json};

      const shadowPlugin = {{
        id: 'shadowPlugin',
        beforeDatasetDraw(chart) {{
          const ctx = chart.ctx;
          ctx.save();
          ctx.shadowColor = 'rgba(0,0,0,0.12)';
          ctx.shadowBlur = 10;
          ctx.shadowOffsetX = 0;
          ctx.shadowOffsetY = 5;
        }},
        afterDatasetDraw(chart) {{
          chart.ctx.restore();
        }}
      }};

      function fmtMoney(v){{
        const n = Math.round(v||0);
        const s = n.toString().replace(/\\B(?=(\\d{{3}})+(?!\\d))/g, ".");
        return "$ " + s;
      }}

      const tc = document.getElementById("traceChart");
      if (tc) {{
        new Chart(tc, {{
          type: 'line',
          data: {{
            labels: trace.labels,
            datasets: trace.datasets.map((ds) => {{
              return {{
                label: ds.label,
                data: ds.data,
                tension: 0.25,
                fill: false,
                borderWidth: 2,
                pointRadius: 3
              }}
            }})
          }},
          options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{
              legend: {{ position: 'bottom' }},
              tooltip: {{
                callbacks: {{
                  label: function(ctx) {{
                    return `${{ctx.dataset.label}}: ${{fmtMoney(ctx.raw)}}`;
                  }}
                }}
              }}
            }},
            scales: {{
              y: {{
                beginAtZero: true,
                ticks: {{
                  callback: function(value){{ return fmtMoney(value); }}
                }}
              }}
            }}
          }},
          plugins: [shadowPlugin]
        }});
      }}
    </script>
    """
    return render_page(body, show_nav=True)


