from datetime import date
import json

from flask import Blueprint, request, jsonify
from flask_login import login_required
from sqlalchemy import func

from app.extensions import db
from app.models import BusinessDay, ExpenseCategory, ExpenseEntry
from app.utils.money import safe_float, ars
from app.utils.dates import (
    is_sunday,
    parse_ymd,
    iso,
    fmt_date_ar,
    fmt_date_ar_from_iso,
    iter_workdays,
    month_range,
)

dashboard_bp = Blueprint("dashboard_bp", __name__)


def _render_page(*args, **kwargs):
    from app_owners import render_page
    return render_page(*args, **kwargs)


def _helpers():
    from app_owners import ensure_shifts, recalc_day_status, day_totals, margin_bucket, range_series
    return ensure_shifts, recalc_day_status, day_totals, margin_bucket, range_series


def _money_input(v):
    return "" if v is None else str(float(v))


def _real_parts(bday):
    cash = getattr(bday, "real_cash_profit", None)
    digital = getattr(bday, "real_digital_profit", None)
    apps = getattr(bday, "real_apps_pending", None)
    legacy = getattr(bday, "real_profit", None)

    if cash is None and digital is None and legacy is not None:
        cash = float(legacy)
        digital = 0.0

    total = None
    if cash is not None or digital is not None:
        total = float(cash or 0.0) + float(digital or 0.0)

    explained_total = None
    if total is not None or apps is not None:
        explained_total = float(total or 0.0) + float(apps or 0.0)

    return cash, digital, apps, total, explained_total


def _delta_bucket(calc, total):
    if total is None:
        return ("muted", "—")
    delta = float(total) - float(calc)
    ad = abs(delta)
    if ad <= 30000:
        return ("ok", ars(total))
    if ad <= 60000:
        return ("warn", ars(total))
    return ("bad", ars(total))


def _total_html(calc, total):
    if total is None:
        return "<span class='muted'>—</span>"
    klass, label = _delta_bucket(calc, total)
    return f"<span class='pill {klass}'>{label}</span>"


def _explained_html(calc, explained_total):
    if explained_total is None:
        return "<span class='muted'>—</span>"
    delta = float(explained_total) - float(calc)
    ad = abs(delta)
    cls = "ok" if ad <= 30000 else ("warn" if ad <= 60000 else "bad")
    return f"<span class='pill {cls}'>{ars(explained_total)}</span>"


def _desfasaje_html(calc, explained_total):
    if explained_total is None:
        return "<span class='muted'>—</span>"
    diff = float(calc) - float(explained_total)
    ad = abs(diff)
    cls = "ok" if ad <= 30000 else ("warn" if ad <= 60000 else "bad")
    return f"<span class='pill {cls}'>{ars(diff)}</span>"


@dashboard_bp.get("/finanzas")
@login_required
def dashboard_finanzas():
    ensure_shifts, recalc_day_status, day_totals, margin_bucket, range_series = _helpers()
    today = date.today()
    from_str = (request.args.get("from") or "").strip()
    to_str = (request.args.get("to") or "").strip()

    if from_str and to_str:
        try:
            d1 = parse_ymd(from_str)
            d2 = parse_ymd(to_str)
        except ValueError:
            d1, d2 = month_range(today)
            from_str, to_str = iso(d1), iso(d2)
    else:
        d1, d2 = month_range(today)
        from_str, to_str = iso(d1), iso(d2)

    if d1 > d2:
        d1, d2 = d2, d1
        from_str, to_str = iso(d1), iso(d2)

    series = range_series(d1, d2)

    income = sum(x["income"] for x in series)
    expense = sum(x["expense_total"] for x in series)
    profit = income - expense

    margen_periodo = (profit / income * 100.0) if income else None
    bucket_label, bucket_class = margin_bucket(margen_periodo)
    promedio_diario = (income / len(series)) if series else 0.0

    sueldo_ximena = (
        db.session.query(func.coalesce(func.sum(ExpenseEntry.amount), 0.0))
        .join(ExpenseCategory, ExpenseCategory.id == ExpenseEntry.category_id)
        .join(BusinessDay, BusinessDay.id == ExpenseEntry.business_day_id)
        .filter(BusinessDay.day >= d1, BusinessDay.day <= d2)
        .filter(ExpenseEntry.kind == "fixed")
        .filter(func.lower(ExpenseCategory.name) == "sueldo ximena")
        .scalar()
        or 0.0
    )

    existing_days = {parse_ymd(x["date"]) for x in series}
    missing_days = [d for d in iter_workdays(d1, d2) if d not in existing_days]

    ranked = []
    for x in series:
        day_income = x["income"]
        day_exp = x["expense_total"]
        day_profit = x["profit"]
        m = (day_profit / day_income * 100.0) if day_income else None
        ranked.append(
            {
                "date_iso": x["date"],
                "date_ar": fmt_date_ar_from_iso(x["date"]),
                "income": day_income,
                "expense": day_exp,
                "profit": day_profit,
                "margin": m,
            }
        )

    ranked_sorted = sorted(ranked, key=lambda r: r["profit"])
    worst3 = ranked_sorted[:3]
    best3 = list(reversed(ranked_sorted[-3:]))

    ALERT_EXPENSE_THRESHOLD = 500_000
    alerts_clean = []
    for r in ranked:
        if r["expense"] > ALERT_EXPENSE_THRESHOLD:
            dday = parse_ymd(r["date_iso"])
            bday = BusinessDay.query.filter_by(day=dday).first()
            detail = ""
            if bday:
                if bday.expenses and len(bday.expenses) > 0:
                    parts = []
                    for e in sorted(bday.expenses, key=lambda x: x.amount or 0, reverse=True)[:6]:
                        parts.append(f"{e.category.name}: {ars(e.amount)}")
                    detail = " | ".join(parts)
                else:
                    parts = []
                    if (bday.note or "").strip():
                        parts.append((bday.note or "").strip())
                    for s in bday.shifts:
                        if (s.note or "").strip():
                            parts.append(f"{s.shift}: {(s.note or '').strip()}")
                    detail = " | ".join(parts).strip()

            if not detail:
                detail = "Sin detalle cargado."

            alerts_clean.append({"date_ar": fmt_date_ar(dday), "expense": r["expense"], "detail": detail})

    def rank_rows(items):
        if not items:
            return "<tr><td colspan='3' class='muted'>Sin datos</td></tr>"
        out = ""
        for rr in items:
            cls = "neg" if rr["profit"] < 0 else ""
            out += (
                "<tr>"
                f"<td>{rr['date_ar']}</td>"
                f"<td class='num'>{ars(rr['income'])}</td>"
                f"<td class='num {cls}'>{ars(rr['profit'])}</td>"
                "</tr>"
            )
        return out

    best_html = rank_rows(best3)
    worst_html = rank_rows(worst3)

    if not alerts_clean:
        alerts_html = "<div class='muted'>Sin alertas (no hubo días con gastos mayores a $ 500.000).</div>"
    else:
        alerts_html = "<ul style='margin:0; padding-left:18px;'>"
        for a in alerts_clean[:50]:
            alerts_html += (
                f"<li><b>{a['date_ar']}</b> — Gastos: <b>{ars(a['expense'])}</b><br/>"
                f"<span class='muted'>{a['detail']}</span></li>"
            )
        alerts_html += "</ul>"

    if income > 0:
        pie_labels = ["Ingresos", "Gastos", "Ganancia"] if profit >= 0 else ["Ingresos", "Gastos", "Pérdida"]
        pie_values = [max(income, 0), max(expense, 0), max(profit, 0) if profit >= 0 else abs(profit)]
    else:
        pie_labels = ["Ingresos", "Gastos", "Ganancia"]
        pie_values = [0, 0, 0]

    charts_payload = {"pie": {"labels": pie_labels, "values": pie_values}}
    charts_json = json.dumps(charts_payload, ensure_ascii=False)

    if missing_days:
        options_html = "".join(f"<option value='{iso(d)}'>{fmt_date_ar(d)}</option>" for d in missing_days)
    else:
        options_html = "<option value='' disabled selected>No hay días faltantes</option>"

    all_days = list(iter_workdays(d1, d2))
    bdays = (
        BusinessDay.query.filter(BusinessDay.day >= d1, BusinessDay.day <= d2)
        .order_by(BusinessDay.day.asc())
        .all()
    )
    bmap = {b.day: b for b in bdays}

    cmp_rows = []
    real_accum = 0.0
    real_fina_accum = 0.0
    calc_accum_running = 0.0
    real_accum_running = 0.0
    cmp_dates = []
    cmp_labels = []
    cmp_calc = []
    cmp_real = []
    cmp_calc_accum = []
    cmp_real_accum = []

    for d in all_days:
        b = bmap.get(d)
        if b:
            ensure_shifts(b)
            recalc_day_status(b)
            t = day_totals(b)
            calc = float(t["profit"])
            cash, digital, apps, total, explained_total = _real_parts(b)
        else:
            calc = 0.0
            cash = None
            digital = None
            apps = None
            total = None
            explained_total = None

        if total is not None:
            real_accum += float(total)

        if explained_total is not None:
            real_fina_accum += float(explained_total)

        cmp_rows.append(
            {
                "date": d,
                "date_ar": fmt_date_ar(d),
                "date_iso": d.isoformat(),
                "calc": calc,
                "cash": cash,
                "digital": digital,
                "apps": apps,
                "real_total": total,
                "explained_total": explained_total,
            }
        )

        cmp_dates.append(d.isoformat())
        cmp_labels.append(fmt_date_ar(d))
        cmp_calc.append(round(calc, 2))
        cmp_real.append(None if total is None else round(float(total), 2))
        calc_accum_running += float(calc)
        cmp_calc_accum.append(round(calc_accum_running, 2))
        if total is not None:
            real_accum_running += float(total)
        cmp_real_accum.append(round(real_accum_running, 2))

    cmp_payload = {
        "dates": cmp_dates,
        "labels": cmp_labels,
        "calc": cmp_calc,
        "real": cmp_real,
        "calc_accum": cmp_calc_accum,
        "real_accum": cmp_real_accum,
    }
    cmp_json = json.dumps(cmp_payload, ensure_ascii=False)

    def _row_has_data(r):
        return (
            abs(float(r["calc"] or 0.0)) > 0
            or r["cash"] is not None
            or r["digital"] is not None
            or r["apps"] is not None
        )

    rows_with_data = [r for r in cmp_rows if _row_has_data(r)]
    rows_without_data = [r for r in cmp_rows if not _row_has_data(r)]

    head_rows = rows_with_data[-5:]
    tail_rows = rows_with_data[:-5] + rows_without_data

    def _cmp_tr(r):
        form_id = f"rp_{r['date_iso']}"
        total_html = _total_html(r["calc"], r["real_total"])
        explained_html = _explained_html(r["calc"], r["explained_total"])
        desfasaje_html = _desfasaje_html(r["calc"], r["explained_total"])
        return (
            "<tr>"
            f"<td>{r['date_ar']}</td>"
            f"<td class='num'>{ars(r['calc'])}</td>"
            f"<td>"
            f"<form id='{form_id}' class='realProfitForm' data-day='{r['date_iso']}' style='margin:0;'>"
            f"<input type='hidden' name='day' value='{r['date_iso']}' />"
            f"</form>"
            f"<input form='{form_id}' name='real_cash_profit' placeholder='Efectivo' value='{_money_input(r['cash'])}' />"
            f"</td>"
            f"<td>"
            f"<input form='{form_id}' name='real_digital_profit' placeholder='Digital' value='{_money_input(r['digital'])}' />"
            f"</td>"
            f"<td>"
            f"<input form='{form_id}' name='real_apps_pending' placeholder='Apps' value='{_money_input(r['apps'])}' />"
            f"</td>"
            f"<td class='num'>"
            f"<button form='{form_id}' class='btn' type='submit' style='min-width:120px;'>Guardar</button>"
            f"</td>"
            f"<td class='num totalCell'>{total_html}</td>"
            f"<td class='num explainedCell'>{explained_html}</td>"
            f"<td class='num desfasajeCell'>{desfasaje_html}</td>"
            "</tr>"
        )

    head_html = "".join(_cmp_tr(r) for r in head_rows) if head_rows else "<tr><td colspan='9' class='muted'>Sin datos</td></tr>"
    tail_html = "".join(_cmp_tr(r) for r in tail_rows)

    details_html = ""
    if tail_rows:
        details_html = f"""
        <details>
          <summary>Ver más días</summary>
          <div style="height:10px;"></div>
          <table>
            <thead>
              <tr>
                <th>Fecha</th>
                <th class="num">Ganancia calculada</th>
                <th style="text-align:center;">Ganancia Efectivo</th>
                <th style="text-align:center;">Ganancia Digital</th>
                <th style="text-align:center;">Apps pendientes</th>
                <th style="text-align:center;"></th>
                <th class="num">Ganancia Real Total</th>
                <th class="num">Real + Apps</th>
                <th class="num">Desfasaje final</th>
              </tr>
            </thead>
            <tbody>{tail_html}</tbody>
          </table>
        </details>
        """

    body = f"""
    <h1>Panel Central</h1>

    <div class="card">
      <h3>Bloque 0 · Filtro del período</h3>
      <form method="get" action="/finanzas">
        <div class="row-actions">
          <div class="field">
            <label>Desde</label>
            <input type="date" name="from" value="{from_str}" />
          </div>
          <div class="field">
            <label>Hasta</label>
            <input type="date" name="to" value="{to_str}" />
          </div>
          <div style="min-width:160px;">
            <label>&nbsp;</label>
            <button class="btn primary" type="submit" style="width:100%;">Aplicar</button>
          </div>
        </div>
        <p class="muted" style="margin-top:10px;">Rango: {fmt_date_ar(d1)} a {fmt_date_ar(d2)} (Domingos excluidos)</p>
      </form>
    </div>

    <details>
      <summary>Completar día faltante (sin domingos)</summary>
      <form method="get" action="/days/go" style="margin-top:10px;">
        <div class="inline">
          <div class="field">
            <label>Día</label>
            <select name="day" {"disabled" if not missing_days else ""}>
              {options_html}
            </select>
          </div>
          <div style="min-width:180px;">
            <label>&nbsp;</label>
            <button class="btn primary" type="submit" style="width:100%;" {"disabled" if not missing_days else ""}>Crear / Completar</button>
          </div>
        </div>
      </form>
    </details>

    <div class="grid8">
      <div class="card kpi income">
        <div class="label">Ingresos</div>
        <div class="value">{ars(income)}</div>
      </div>

      <div class="card kpi expense">
        <div class="label">Gastos</div>
        <div class="value">{ars(expense)}</div>
      </div>

      <div class="card kpi profit">
        <div class="label">Ganancia</div>
        <div class="value">{ars(profit)}</div>
      </div>

      <div class="card kpi profit">
        <div class="label">Ganancia real (acumulada)</div>
        <div class="value">{ars(real_accum)}</div>
        <div class="muted">Suma efectivo + digital cargado</div>
      </div>

      <div class="card kpi">
        <div class="margen-kpi">
          <div class="margen-left">
            <div class="label">Margen</div>
            <div class="value">{(f"{margen_periodo:.1f}%" if margen_periodo is not None else "—")}</div>
            <div style="margin-top:6px;"><span class="{bucket_class}">{bucket_label}</span></div>
          </div>

          <div class="margen-right">
            <div class="muted">Ref.</div>
            <span class="pill bad">Malo ≤ 20</span>
            <span class="pill warn">Regular ≤ 30</span>
            <span class="pill ok">Bueno ≥ 31</span>
          </div>
        </div>
      </div>

      <div class="card kpi">
        <div class="label">Promedio diario (Ingresos)</div>
        <div class="value">{ars(promedio_diario)}</div>
      </div>

      <div class="card kpi">
        <div class="label">Sueldo Ximena</div>
        <div class="value">{ars(sueldo_ximena)}</div>
        <div class="muted">Gasto fijo en el rango</div>
      </div>

      <div class="card kpi profit">
        <div class="label">Ganancia Real Final</div>
        <div class="value">{ars(real_fina_accum)}</div>
        <div class="muted">Efectivo + digital + apps pendientes</div>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <h3>Bloque 1 · Torta del período</h3>
        <div class="chartbox"><canvas id="pieChart"></canvas></div>
        <p class="muted" style="margin-top:10px;">(Domingos excluidos del cálculo)</p>
      </div>
      <div class="card">
        <h3>Bloque 2 · Top 3 mejores días (ganancia)</h3>
        <table>
          <thead><tr><th>Fecha</th><th class="num">Ingresos</th><th class="num">Ganancia</th></tr></thead>
          <tbody>{best_html}</tbody>
        </table>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <h3>Bloque 3 · Top 3 peores días (ganancia)</h3>
        <table>
          <thead><tr><th>Fecha</th><th class="num">Ingresos</th><th class="num">Ganancia</th></tr></thead>
          <tbody>{worst_html}</tbody>
        </table>
      </div>
      <div class="card">
        <h3>Bloque 4 · Alertas (Gastos &gt; {ars(500000)})</h3>
        {alerts_html}
      </div>
    </div>

    <div class="card" id="profit-control">
      <h3>Bloque 5 · Control de Ganancia Calculada vs Real (últimos 5 días visibles)</h3>
      <div class="chartbox"><canvas id="profitCompareChart"></canvas></div>
      <p class="muted" style="margin-top:10px;">
        Total real = Efectivo + Digital. “Apps pendientes” no suma a caja real: solo explica parte del desfase contra la calculada. El semáforo usa verde hasta ±30.000, amarillo hasta ±60.000 y rojo si supera ±60.000.
      </p>

      <div style="height:10px;"></div>

      <table>
        <thead>
          <tr>
            <th>Fecha</th>
            <th class="num">Ganancia calculada</th>
            <th style="text-align:center;">Ganancia Efectivo</th>
            <th style="text-align:center;">Ganancia Digital</th>
            <th style="text-align:center;">Apps pendientes</th>
            <th style="text-align:center;"></th>
            <th class="num">Ganancia Real Total</th>
            <th class="num">Real + Apps</th>
            <th class="num">Desfasaje final</th>
          </tr>
        </thead>
        <tbody>{head_html}</tbody>
      </table>

      {details_html}
    </div>

    <script>
      const payload = {charts_json};
      const profitCmp = {cmp_json};
      let profitCompareChartInstance = null;

      const shadowPlugin = {{
        id: 'shadowPlugin',
        beforeDatasetDraw(chart) {{
          const ctx = chart.ctx;
          ctx.save();
          ctx.shadowColor = 'rgba(0,0,0,0.14)';
          ctx.shadowBlur = 14;
          ctx.shadowOffsetX = 0;
          ctx.shadowOffsetY = 7;
        }},
        afterDatasetDraw(chart) {{
          chart.ctx.restore();
        }}
      }};

      function fmtMoney(v){{
        const n = Math.round(Number(v||0));
        const s = n.toString().replace(/\\B(?=(\\d{{3}})+(?!\\d))/g, ".");
        return "$ " + s;
      }}

      const pieValuePlugin = {{
        id: 'pieValuePlugin',
        afterDatasetsDraw(chart) {{
          if (chart.config.type !== 'pie') return;
          const ctx = chart.ctx;
          const dataset = chart.data.datasets[0];
          const meta = chart.getDatasetMeta(0);
          const data = dataset.data || [];

          ctx.save();
          ctx.font = '800 12px Arial';
          ctx.fillStyle = '#111827';
          ctx.textAlign = 'center';
          ctx.textBaseline = 'middle';

          meta.data.forEach((arc, i) => {{
            const v = Number(data[i] || 0);
            if (!v) return;
            const label = fmtMoney(v);
            const angle = (arc.startAngle + arc.endAngle) / 2;
            const r = arc.outerRadius * 0.70;
            const x = arc.x + Math.cos(angle) * r;
            const y = arc.y + Math.sin(angle) * r;
            ctx.fillText(label, x, y);
          }});

          ctx.restore();
        }}
      }};

      const pieCanvas = document.getElementById('pieChart');
      if (pieCanvas) {{
        new Chart(pieCanvas, {{
          type: 'pie',
          data: {{
            labels: payload.pie.labels,
            datasets: [
              {{
                data: payload.pie.values,
                backgroundColor: [
                  'rgba(22,163,74,0.28)',
                  'rgba(220,38,38,0.22)',
                  'rgba(37,99,235,0.22)'
                ],
                borderColor: [
                  'rgba(22,163,74,0.55)',
                  'rgba(220,38,38,0.55)',
                  'rgba(37,99,235,0.55)'
                ],
                borderWidth: 1
              }}
            ]
          }},
          options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{
              legend: {{ position: 'bottom' }}
            }}
          }},
          plugins: [shadowPlugin, pieValuePlugin]
        }});
      }}

      const pc = document.getElementById("profitCompareChart");
      if (pc) {{
        profitCompareChartInstance = new Chart(pc, {{
          type: 'line',
          data: {{
            labels: profitCmp.labels,
            datasets: [
              {{
                label: 'Ganancia Calculada',
                data: profitCmp.calc,
                tension: 0.25,
                fill: false,
                borderWidth: 2,
                pointRadius: 3,
                borderColor: '#2563eb',
                backgroundColor: '#2563eb',
                pointBackgroundColor: '#2563eb'
              }},
              {{
                label: 'Ganancia Real',
                data: profitCmp.real,
                tension: 0.25,
                fill: false,
                borderWidth: 2,
                pointRadius: 3,
                spanGaps: false,
                borderColor: '#dc2626',
                backgroundColor: '#dc2626',
                pointBackgroundColor: '#dc2626'
              }},
              {{
                label: 'Calculada Acumulada',
                data: profitCmp.calc_accum,
                tension: 0.2,
                fill: false,
                borderWidth: 2,
                pointRadius: 2,
                borderDash: [6,4],
                borderColor: '#60a5fa',
                backgroundColor: '#60a5fa',
                pointBackgroundColor: '#60a5fa'
              }},
              {{
                label: 'Real Acumulada',
                data: profitCmp.real_accum,
                tension: 0.2,
                fill: false,
                borderWidth: 2,
                pointRadius: 2,
                borderDash: [6,4],
                spanGaps: false,
                borderColor: '#fb923c',
                backgroundColor: '#fb923c',
                pointBackgroundColor: '#fb923c'
              }}
            ]
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
                ticks: {{
                  callback: function(value){{ return fmtMoney(value); }}
                }}
              }}
            }}
          }},
          plugins: [shadowPlugin]
        }});
      }}

      function recomputeRealAccum(arr){{
        let run = 0;
        return arr.map(v => {{
          if(v !== null && v !== undefined && !Number.isNaN(v)) run += Number(v);
          return run;
        }});
      }}

      async function postRealProfit(form) {{
        const fd = new FormData(form);
        const res = await fetch('/finanzas/real_profit/save_json', {{
          method: 'POST',
          body: fd
        }});
        const data = await res.json();
        if(!data.ok) {{
          alert(data.error || "Error guardando ganancia real");
          return;
        }}

        const tr = form.closest('tr');
        if(tr) {{
          const totalCell = tr.querySelector('.totalCell');
          if(totalCell) totalCell.innerHTML = data.total_html;

          const explainedCell = tr.querySelector('.explainedCell');
          if(explainedCell) explainedCell.innerHTML = data.explained_html;

          const desfasajeCell = tr.querySelector('.desfasajeCell');
          if(desfasajeCell) desfasajeCell.innerHTML = data.desfasaje_html;
        }}

        const idx = profitCmp.dates.indexOf(data.day);
        if(idx >= 0) {{
          profitCmp.real[idx] = data.real_total_value;
          profitCmp.real_accum = recomputeRealAccum(profitCmp.real);
          if(profitCompareChartInstance) {{
            profitCompareChartInstance.data.datasets[1].data = profitCmp.real;
            profitCompareChartInstance.data.datasets[3].data = profitCmp.real_accum;
            profitCompareChartInstance.update();
          }}
        }}
      }}

      document.querySelectorAll('.realProfitForm').forEach((form) => {{
        form.addEventListener('submit', function(ev){{
          ev.preventDefault();
          postRealProfit(form);
        }});
      }});
    </script>
    """
    db.session.commit()
    return _render_page(body, show_nav=True)


@dashboard_bp.post("/finanzas/real_profit/save_json")
@login_required
def save_real_profit_json():
    ensure_shifts, recalc_day_status, day_totals, _, _ = _helpers()

    day = (request.form.get("day") or "").strip()
    v_cash = (request.form.get("real_cash_profit") or "").strip()
    v_digital = (request.form.get("real_digital_profit") or "").strip()
    v_apps = (request.form.get("real_apps_pending") or "").strip()

    if not day:
        return jsonify({"ok": False, "error": "Falta fecha"}), 400
    try:
        d = parse_ymd(day)
    except ValueError:
        return jsonify({"ok": False, "error": "Fecha inválida"}), 400
    if is_sunday(d):
        return jsonify({"ok": False, "error": "Domingo: no se trabaja"}), 400

    cash = None if v_cash == "" else safe_float(v_cash)
    digital = None if v_digital == "" else safe_float(v_digital)
    apps = None if v_apps == "" else safe_float(v_apps)

    bday = BusinessDay.query.filter_by(day=d).first()
    if not bday:
        bday = BusinessDay(day=d, note="", status="draft")
        db.session.add(bday)
        db.session.flush()
        ensure_shifts(bday)
        recalc_day_status(bday)

    bday.real_cash_profit = cash
    bday.real_digital_profit = digital
    bday.real_apps_pending = apps

    total = None
    if cash is not None or digital is not None:
        total = float(cash or 0.0) + float(digital or 0.0)
        bday.real_profit = total
    else:
        bday.real_profit = None

    ensure_shifts(bday)
    recalc_day_status(bday)
    db.session.commit()

    t = day_totals(bday)
    calc = float(t["profit"])
    total_html = _total_html(calc, total)

    explained_total = None
    explained_html = "<span class='muted'>—</span>"
    desfasaje_html = "<span class='muted'>—</span>"

    if total is not None or apps is not None:
        explained_total = float(total or 0.0) + float(apps or 0.0)
        explained_html = _explained_html(calc, explained_total)
        desfasaje_html = _desfasaje_html(calc, explained_total)

    return jsonify({
        "ok": True,
        "day": day,
        "real_total_value": total,
        "apps_value": apps,
        "explained_total_value": explained_total,
        "total_html": total_html,
        "explained_html": explained_html,
        "desfasaje_html": desfasaje_html,
    })