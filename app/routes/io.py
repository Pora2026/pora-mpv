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

TOP_GASTOS_KEY = "__TOP_GASTOS__"


def _owners():
    from app_owners import render_page, range_series, period_previous
    return render_page, range_series, period_previous


def _pill_money(value, kind):
    if kind == "income":
        style = "color:#16a34a; border-color: rgba(22,163,74,.25); background: rgba(22,163,74,.10);"
    elif kind == "expense":
        style = "color:#dc2626; border-color: rgba(220,38,38,.25); background: rgba(220,38,38,.10);"
    else:
        style = "color:#2563eb; border-color: rgba(37,99,235,.25); background: rgba(37,99,235,.10);"
    return f"<span class='pill' style='{style} font-weight:700;'>{ars(value)}</span>"


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

    selected_raw = request.args.getlist("cats")
    selected_top = TOP_GASTOS_KEY in selected_raw

    selected_ids = []
    for x in selected_raw:
        if x == TOP_GASTOS_KEY:
            continue
        try:
            selected_ids.append(int(x))
        except Exception:
            pass

    selected_ids_set = {str(x) for x in selected_ids}
    if selected_top:
        selected_ids_set.add(TOP_GASTOS_KEY)

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
            {
                "label": f"{y}-W{w:02d}",
                "income": v["income"],
                "expense": v["expense"],
                "profit": v["income"] - v["expense"],
            }
        )

    if weekly_rows:
        avg_week_income = sum(r["income"] for r in weekly_rows) / len(weekly_rows)
        avg_week_expense = sum(r["expense"] for r in weekly_rows) / len(weekly_rows)
        avg_week_profit = sum(r["profit"] for r in weekly_rows) / len(weekly_rows)

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
            ExpenseCategory.id.label("category_id"),
            ExpenseCategory.kind,
            ExpenseCategory.name,
            func.coalesce(func.sum(ExpenseEntry.amount), 0.0).label("total"),
        )
        .join(ExpenseEntry, ExpenseEntry.category_id == ExpenseCategory.id)
        .join(BusinessDay, BusinessDay.id == ExpenseEntry.business_day_id)
        .filter(BusinessDay.day >= d1, BusinessDay.day <= d2)
        .group_by(ExpenseCategory.id, ExpenseCategory.kind, ExpenseCategory.name)
        .order_by(func.sum(ExpenseEntry.amount).desc(), ExpenseCategory.name.asc())
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

    all_categories = ExpenseCategory.query.order_by(ExpenseCategory.name.asc()).all()

    use_top_gastos = selected_top or not selected_ids

    if use_top_gastos:
        top_cat_ids = [r.category_id for r in cat_rows[:6]]
    else:
        top_cat_ids = selected_ids

    selected_cat_objs = []
    if top_cat_ids:
        selected_cat_objs = (
            ExpenseCategory.query
            .filter(ExpenseCategory.id.in_(top_cat_ids))
            .order_by(ExpenseCategory.name.asc())
            .all()
        )

    top_cat_names = {c.id: c.name for c in selected_cat_objs}

    dialect = db.engine.dialect.name
    ym_expr = func.to_char(BusinessDay.day, "YYYY-MM") if dialect == "postgresql" else func.strftime("%Y-%m", BusinessDay.day)

    rows_tr_all = []
    if top_cat_ids:
        rows_tr_all = (
            db.session.query(
                ym_expr.label("ym"),
                ExpenseEntry.category_id,
                func.coalesce(func.sum(ExpenseEntry.amount), 0.0).label("total"),
            )
            .join(BusinessDay, BusinessDay.id == ExpenseEntry.business_day_id)
            .filter(ExpenseEntry.category_id.in_(top_cat_ids))
            .group_by(ym_expr, ExpenseEntry.category_id)
            .order_by(ym_expr)
            .all()
        )

    trace_all = {}
    for r in rows_tr_all:
        trace_all.setdefault(r.ym, {})
        trace_all[r.ym][r.category_id] = float(r.total or 0.0)

    start_year = 2026
    start_month = 1
    end_year = today.year
    end_month = today.month

    trace_months = []
    y, m = start_year, start_month
    while (y < end_year) or (y == end_year and m <= end_month):
        trace_months.append(f"{y}-{m:02d}")
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1

    trace_datasets = []
    for c in selected_cat_objs:
        data = []
        for month_key in trace_months:
            data.append(trace_all.get(month_key, {}).get(c.id, 0.0))
        trace_datasets.append({"label": c.name, "data": data})

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
                f"<td class='num'>{_pill_money(r['income'], 'income')}</td>"
                f"<td class='num'>{_pill_money(r['expense'], 'expense')}</td>"
                f"<td class='num'>{_pill_money(r['profit'], 'profit')}</td></tr>"
            )

    mo_html = ""
    if not monthly_rows:
        mo_html = "<tr><td colspan='4' class='muted'>Sin datos</td></tr>"
    else:
        for r in monthly_rows:
            mo_html += (
                f"<tr><td>{r['label']}</td>"
                f"<td class='num'>{_pill_money(r['income'], 'income')}</td>"
                f"<td class='num'>{_pill_money(r['expense'], 'expense')}</td>"
                f"<td class='num'>{_pill_money(r['profit'], 'profit')}</td></tr>"
            )

    trace_payload = {"labels": trace_months, "datasets": trace_datasets}
    trace_json = json.dumps(trace_payload, ensure_ascii=False)

    cats_options_html = (
        f"<option value='{TOP_GASTOS_KEY}' {'selected' if use_top_gastos else ''}>Top Gastos</option>"
        + "".join(
            f"<option value='{c.id}' {'selected' if (not use_top_gastos and str(c.id) in selected_ids_set) else ''}>{c.name}</option>"
            for c in all_categories
        )
    )

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

    <div class="card">
      <h3>Trazabilidad mensual</h3>

      <form method="get" action="/io" style="margin-bottom:12px;">
        <input type="hidden" name="from" value="{d1s}" />
        <input type="hidden" name="to" value="{d2s}" />
        <input type="hidden" name="compare_mode" value="{compare_mode}" />
        <input type="hidden" name="cfrom" value="{c1s}" />
        <input type="hidden" name="cto" value="{c2s}" />

        <div class="row-actions">
          <div class="field" style="min-width:280px;">
            <label>Categorías a visualizar</label>
            <select name="cats" multiple style="min-width:280px; height:130px;">
              {cats_options_html}
            </select>
            <div class="muted" style="margin-top:6px;">
              “Top Gastos” toma automáticamente las 6 categorías con mayor gasto del rango actual. Si no seleccionás nada, también se usa ese criterio.
            </div>
          </div>

          <div style="min-width:160px;">
            <label>&nbsp;</label>
            <button class="btn" type="submit" style="width:100%;">Aplicar categorías</button>
          </div>
        </div>
      </form>

      <div class="chartbox"><canvas id="traceChart"></canvas></div>
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