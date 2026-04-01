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

    # ✅ KPI NUEVO: Sueldo restante
    SUELDO_XIMENA_META = 3_000_000
    sueldo_restante = SUELDO_XIMENA_META - float(sueldo_ximena or 0.0)

    existing_days = {parse_ymd(x["date"]) for x in series}
    missing_days = [d for d in iter_workdays(d1, d2) if d not in existing_days]

    bar_labels, bar_income, bar_expense, bar_profit = [], [], [], []
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

        bar_labels.append(fmt_date_ar_from_iso(x["date"]))
        bar_income.append(round(day_income, 2))
        bar_expense.append(round(day_exp, 2))
        bar_profit.append(round(day_profit, 2))

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

    # Pie chart
    if income > 0:
        pie_labels = ["Ingresos", "Gastos", "Ganancia"] if profit >= 0 else ["Ingresos", "Gastos", "Pérdida"]
        pie_values = [max(income, 0), max(expense, 0), max(profit, 0) if profit >= 0 else abs(profit)]
    else:
        pie_labels = ["Ingresos", "Gastos", "Ganancia"]
        pie_values = [0, 0, 0]

    charts_payload = {"bar": {"labels": bar_labels, "income": bar_income, "expense": bar_expense, "profit": bar_profit},
                      "pie": {"labels": pie_labels, "values": pie_values}}
    charts_json = json.dumps(charts_payload, ensure_ascii=False)

    if missing_days:
        options_html = "".join(f"<option value='{iso(d)}'>{fmt_date_ar(d)}</option>" for d in missing_days)
    else:
        options_html = "<option value='' disabled selected>No hay días faltantes</option>"

    # ---------------------------------------------------------
    # Control Ganancia Calculada vs Real (DIARIO)
    # ---------------------------------------------------------
    all_days = list(iter_workdays(d1, d2))
    bdays = (
        BusinessDay.query.filter(BusinessDay.day >= d1, BusinessDay.day <= d2)
        .order_by(BusinessDay.day.asc())
        .all()
    )
    bmap = {b.day: b for b in bdays}

    cmp_rows = []
    real_accum = 0.0  # ✅ KPI NUEVO: ganancia real acumulada en rango (sum real_profit)
    for d in all_days:
        b = bmap.get(d)
        if b:
            ensure_shifts(b)
            recalc_day_status(b)
            t = day_totals(b)
            calc = float(t["profit"])
            real = b.real_profit if b.real_profit is not None else None
        else:
            calc = 0.0
            real = None

        if real is not None:
            real_accum += float(real)

        diff = (calc - float(real)) if (real is not None) else None

        cmp_rows.append(
            {"date": d, "date_ar": fmt_date_ar(d), "date_iso": d.isoformat(), "calc": calc, "real": real, "diff": diff}
        )

    cmp_labels = [r["date_ar"] for r in cmp_rows]
    cmp_calc = [round(r["calc"], 2) for r in cmp_rows]
    cmp_real = [None if r["real"] is None else round(float(r["real"]), 2) for r in cmp_rows]
    cmp_payload = {"labels": cmp_labels, "calc": cmp_calc, "real": cmp_real}
    cmp_json = json.dumps(cmp_payload, ensure_ascii=False)

    head_rows = cmp_rows[:3]
    tail_rows = cmp_rows[3:]

    def _cmp_tr(r):
        diff = r["diff"]
        if diff is None:
            diff_html = "<span class='muted'>—</span>"
            status_html = "<span class='pill warn'>NO OK</span>"
        else:
            diff_cls = "neg" if diff != 0 else ""
            diff_html = f"<span class='{diff_cls}'>{ars(diff)}</span>"
            status_html = "<span class='pill ok'>OK</span>" if diff == 0 else "<span class='pill bad'>NO OK</span>"

        real_val = "" if r["real"] is None else str(float(r["real"]))

        # ✅ Guardado por AJAX: no recarga ni mueve pantalla
        form = f"""
        <form class="realProfitForm" data-day="{r['date_iso']}" style="margin:0;">
          <input type="hidden" name="day" value="{r['date_iso']}" />
          <div class="inline" style="justify-content:flex-end;">
            <div class="field" style="min-width:180px;">
              <input name="real_profit" placeholder="Ej: 120000" value="{real_val}" />
            </div>
            <div style="min-width:120px;">
              <button class="btn" type="submit" style="width:100%;">Guardar</button>
            </div>
          </div>
        </form>
        """
        return (
            "<tr>"
            f"<td>{r['date_ar']}</td>"
            f"<td class='num'>{ars(r['calc'])}</td>"
            f"<td>{form}</td>"
            f"<td class='num diffCell'>{diff_html}</td>"
            f"<td class='statusCell'>{status_html}</td>"
            "</tr>"
        )

    head_html = "".join(_cmp_tr(r) for r in head_rows) if head_rows else "<tr><td colspan='5' class='muted'>Sin datos</td></tr>"
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
                <th>Ganancia real (editable)</th>
                <th class="num">Diferencia</th>
                <th>Estado</th>
              </tr>
            </thead>
            <tbody>{tail_html}</tbody>
          </table>
        </details>
        """

    body = f"""
    <h1>Panel Central</h1>

    <div class="card">
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

    <!-- ✅ KPI 8 (4 columnas x 2 filas) -->
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

      <!-- NUEVO -->
      <div class="card kpi profit">
        <div class="label">Ganancia real (acumulada)</div>
        <div class="value">{ars(real_accum)}</div>
        <div class="muted">Suma de la ganancia real cargada</div>
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

      <!-- NUEVO -->
      <div class="card kpi">
        <div class="label">Sueldo Ximena restante</div>
        <div class="value">{ars(sueldo_restante)}</div>
        <div class="muted">{ars(SUELDO_XIMENA_META)} − Sueldo Ximena</div>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <h3>Barras diarias: Ingresos / Gastos / Ganancia</h3>
        <div class="chartbox"><canvas id="barChart"></canvas></div>
      </div>
      <div class="card">
        <h3>Torta del período</h3>
        <div class="chartbox"><canvas id="pieChart"></canvas></div>
        <p class="muted" style="margin-top:10px;">(Domingos excluidos del cálculo)</p>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <h3>Top 3 mejores días (ganancia)</h3>
        <table>
          <thead><tr><th>Fecha</th><th class="num">Ingresos</th><th class="num">Ganancia</th></tr></thead>
          <tbody>{best_html}</tbody>
        </table>
      </div>
      <div class="card">
        <h3>Top 3 peores días (ganancia)</h3>
        <table>
          <thead><tr><th>Fecha</th><th class="num">Ingresos</th><th class="num">Ganancia</th></tr></thead>
          <tbody>{worst_html}</tbody>
        </table>
      </div>
    </div>

    <div class="card">
      <h3>Alertas (Gastos &gt; {ars(500000)})</h3>
      {alerts_html}
    </div>

    <div class="card" id="profit-control">
      <h3>Control de Ganancia Calculada vs Real (DIARIO)</h3>
      <div class="chartbox"><canvas id="profitCompareChart"></canvas></div>
      <p class="muted" style="margin-top:10px;">
        Ganancia calculada = Ingresos − Gastos. Ganancia real = valor manual (guardado). Diferencia = Calculada − Real.
      </p>

      <div style="height:10px;"></div>

      <table>
        <thead>
          <tr>
            <th>Fecha</th>
            <th class="num">Ganancia calculada</th>
            <th>Ganancia real (editable)</th>
            <th class="num">Diferencia</th>
            <th>Estado</th>
          </tr>
        </thead>
        <tbody>{head_html}</tbody>
      </table>

      {details_html}
    </div>

    <script>
      const payload = {charts_json};
      const profitCmp = {cmp_json};

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

      function makeBarGradient(ctx, baseColor) {{
        const g = ctx.createLinearGradient(0, 0, 0, 280);
        g.addColorStop(0, baseColor.replace('0.28', '0.45').replace('0.22','0.40'));
        g.addColorStop(1, baseColor.replace('0.28', '0.15').replace('0.22','0.12'));
        return g;
      }}

      const barCanvas = document.getElementById('barChart');
      if (barCanvas) {{
        const ctx = barCanvas.getContext('2d');
        const incomeBase = 'rgba(22,163,74,0.28)';
        const expenseBase = 'rgba(220,38,38,0.22)';
        const profitBase  = 'rgba(37,99,235,0.22)';

        new Chart(barCanvas, {{
          type: 'bar',
          data: {{
            labels: payload.bar.labels,
            datasets: [
              {{
                label: 'Ingresos',
                data: payload.bar.income,
                backgroundColor: makeBarGradient(ctx, incomeBase),
                borderColor: 'rgba(22,163,74,0.55)',
                borderWidth: 1,
                borderRadius: 12
              }},
              {{
                label: 'Gastos',
                data: payload.bar.expense,
                backgroundColor: makeBarGradient(ctx, expenseBase),
                borderColor: 'rgba(220,38,38,0.55)',
                borderWidth: 1,
                borderRadius: 12
              }},
              {{
                label: 'Ganancia',
                data: payload.bar.profit,
                backgroundColor: makeBarGradient(ctx, profitBase),
                borderColor: 'rgba(37,99,235,0.55)',
                borderWidth: 1,
                borderRadius: 12
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
                  label: function(context) {{
                    return `${{context.dataset.label}}: ${{fmtMoney(context.raw || 0)}}`;
                  }}
                }}
              }}
            }},
            scales: {{
              y: {{ beginAtZero: true }}
            }}
          }},
          plugins: [shadowPlugin]
        }});
      }}

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

      // Comparativo Calc vs Real
      const pc = document.getElementById("profitCompareChart");
      if (pc) {{
        new Chart(pc, {{
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
                pointRadius: 3
              }},
              {{
                label: 'Ganancia Real',
                data: profitCmp.real,
                tension: 0.25,
                fill: false,
                borderWidth: 2,
                pointRadius: 3,
                spanGaps: false
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

      // ✅ AJAX save ganancia real (no reload)
      async function postRealProfit(form) {{
        const fd = new FormData(form);
        const day = fd.get('day');
        const real_profit = fd.get('real_profit') || "";
        const res = await fetch('/finanzas/real_profit/save_json', {{
          method: 'POST',
          body: fd
        }});
        const data = await res.json();
        if(!data.ok) {{
          alert(data.error || "Error guardando ganancia real");
          return;
        }}
        // Actualizamos diff/estado en la fila
        const tr = form.closest('tr');
        if(tr) {{
          const diffCell = tr.querySelector('.diffCell');
          const statusCell = tr.querySelector('.statusCell');
          if(diffCell) diffCell.innerHTML = data.diff_html;
          if(statusCell) statusCell.innerHTML = data.status_html;
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


# Guarda ganancia real (AJAX)
@login_required


@dashboard_bp.post("/finanzas/real_profit/save_json")
@login_required
def save_real_profit_json():
    ensure_shifts, recalc_day_status, day_totals, margin_bucket, range_series = _helpers()
    day = (request.form.get("day") or "").strip()
    v = (request.form.get("real_profit") or "").strip()

    if not day:
        return jsonify({"ok": False, "error": "Falta fecha"}), 400
    try:
        d = parse_ymd(day)
    except ValueError:
        return jsonify({"ok": False, "error": "Fecha inválida"}), 400
    if is_sunday(d):
        return jsonify({"ok": False, "error": "Domingo: no se trabaja"}), 400

    real_profit = None
    if v != "":
        try:
            real_profit = safe_float(v)
        except Exception:
            return jsonify({"ok": False, "error": "Ganancia real inválida"}), 400

    bday = BusinessDay.query.filter_by(day=d).first()
    if not bday:
        bday = BusinessDay(day=d, note="", status="draft")
        db.session.add(bday)
        db.session.flush()
        ensure_shifts(bday)
        recalc_day_status(bday)

    bday.real_profit = real_profit
    ensure_shifts(bday)
    recalc_day_status(bday)
    db.session.commit()

    # Calculada para diff/estado
    t = day_totals(bday)
    calc = float(t["profit"])
    if real_profit is None:
        diff_html = "<span class='muted'>—</span>"
        status_html = "<span class='pill warn'>NO OK</span>"
    else:
        diff = calc - float(real_profit)
        cls = "neg" if diff != 0 else ""
        diff_html = f"<span class='{cls}'>{ars(diff)}</span>"
        status_html = "<span class='pill ok'>OK</span>" if diff == 0 else "<span class='pill bad'>NO OK</span>"

    return jsonify({"ok": True, "diff_html": diff_html, "status_html": status_html})