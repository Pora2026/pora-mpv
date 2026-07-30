from datetime import date
import json
from datetime import timedelta

from flask import Blueprint, request, jsonify
from flask_login import login_required
from sqlalchemy import func
from app.services.finance_service import (
    compute_explained_total,
    compute_expected_cash_balance,
    compute_comparable_liquid_balance,
    compute_reserved_funds_series,
)

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

# Factor de retención de apps (comisiones + impuestos + cargos)
# Basado en liquidación: 582.237,50 brutos → 341.039,27 netos = 41,4% retención
APPS_RETENTION_FACTOR = 0.414


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
    apps_collected = getattr(bday, "real_apps_collected", None)
    legacy = getattr(bday, "real_profit", None)

    if cash is None and digital is None and legacy is not None:
        cash = float(legacy)
        digital = 0.0

    total = None
    if cash is not None or digital is not None or apps_collected is not None:
        total = float(cash or 0.0) + float(digital or 0.0) + float(apps_collected or 0.0)

    explained_total = None

    return cash, digital, apps, apps_collected, total, explained_total


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


def _desfasaje_pct(liquid_accum, real_accum):
    if liquid_accum is None or real_accum is None:
        return "<span class='muted'>—</span>"
    if abs(float(liquid_accum)) < 1e-9:
        return "<span class='muted'>—</span>"

    pct = abs(float(real_accum) - float(liquid_accum)) / abs(float(liquid_accum)) * 100

    cls = "ok" if pct <= 10 else ("warn" if pct <= 20 else "bad")
    return f"<span class='pill {cls}'>{pct:.1f}%</span>"


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

    # =========================================================
    # COBRO POR APPS (próximo viernes)
    # =========================================================
    def next_friday(d):
        days_ahead = 4 - d.weekday()
        if days_ahead <= 0:
            days_ahead += 7
        return d + timedelta(days=days_ahead)

    def prev_monday(d):
        days_back = d.weekday()
        return d - timedelta(days=days_back)

    proximo_viernes = next_friday(today)
    lunes_cobro = prev_monday(proximo_viernes - timedelta(days=14))
    sabado_cobro = lunes_cobro + timedelta(days=6)

    apps_a_cobrar_bruto = 0.0
    days_in_range = []
    cur = lunes_cobro
    while cur <= sabado_cobro:
        if not is_sunday(cur):
            days_in_range.append(cur)
            b = bmap.get(cur)
            if b:
                apps_a_cobrar_bruto += float(getattr(b, "real_apps_pending", 0.0) or 0.0)
        cur += timedelta(days=1)

    apps_a_cobrar_estimado = apps_a_cobrar_bruto * (1 - APPS_RETENTION_FACTOR)
    periodo_cobro_str = f"{fmt_date_ar(lunes_cobro)} al {fmt_date_ar(sabado_cobro)}"

    cmp_rows = []
    cmp_dates = []
    cmp_labels = []

    cmp_calc = []
    cmp_liquid_profit = []
    cmp_real_profit = []

    cmp_calc_accum = []
    cmp_liquid_profit_accum = []
    cmp_real_profit_accum = []
    cmp_reserved_funds = []

    # Para que un filtro iniciado a mitad de mes conserve los acumulados
    # correctos, calculamos internamente desde el primer día del mes inicial.
    chart_start = d1.replace(day=1)
    chart_days = list(iter_workdays(chart_start, d2))
    chart_bdays = (
        BusinessDay.query
        .filter(BusinessDay.day >= chart_start, BusinessDay.day <= d2)
        .order_by(BusinessDay.day.asc())
        .all()
    )
    chart_bmap = {b.day: b for b in chart_bdays}

    opening_reserved_by_month = {}
    for b in chart_bdays:
        month_key = (b.day.year, b.day.month)
        if month_key not in opening_reserved_by_month:
            opening_reserved_by_month[month_key] = float(
                getattr(b, "opening_cash_balance", 0.0) or 0.0
            )

    current_month_key = None
    opening_reserved_funds = 0.0
    calc_running = 0.0
    liquid_profit_running = 0.0
    reserve_movements = []

    for d in chart_days:
        month_key = (d.year, d.month)
        if month_key != current_month_key:
            current_month_key = month_key
            opening_reserved_funds = float(
                opening_reserved_by_month.get(month_key, 0.0) or 0.0
            )
            calc_running = 0.0
            liquid_profit_running = 0.0
            reserve_movements = []

        b = chart_bmap.get(d)

        if b:
            ensure_shifts(b)
            recalc_day_status(b)
            t = day_totals(b)

            calc = float(t["profit"] or 0.0)

            daily_liquidity = (
                float(getattr(b, "daily_mercadopago", 0.0) or 0.0)
                + float(getattr(b, "daily_cash_withdrawn", 0.0) or 0.0)
                + float(getattr(b, "real_apps_collected", 0.0) or 0.0)
            )

            has_liquid_data = (
                getattr(b, "daily_mercadopago", None) is not None
                or getattr(b, "daily_cash_withdrawn", None) is not None
                or getattr(b, "real_apps_collected", None) is not None
            )

            liquid_profit = (
                daily_liquidity - float(t["expense_total"] or 0.0)
                if has_liquid_data
                else None
            )

            if has_liquid_data:
                expected_total_balance = compute_expected_cash_balance(
                    opening_balance=getattr(b, "opening_cash_balance", None),
                    cash_income=daily_liquidity,
                    paid_expenses=t["expense_total"],
                    safe_box_transfer=getattr(b, "safe_box_transfer", None),
                )
            elif getattr(b, "expected_cash_balance", None) is not None:
                expected_total_balance = float(b.expected_cash_balance)
            else:
                expected_total_balance = None

            reserve_addition = max(
                float(getattr(b, "safe_box_transfer", 0.0) or 0.0),
                0.0,
            )

            has_real_data = (
                getattr(b, "real_cash_profit", None) is not None
                or getattr(b, "real_digital_profit", None) is not None
                or getattr(b, "real_apps_collected", None) is not None
            )

            real_profit = (
                float(getattr(b, "real_cash_profit", 0.0) or 0.0)
                + float(getattr(b, "real_digital_profit", 0.0) or 0.0)
                + float(getattr(b, "real_apps_collected", 0.0) or 0.0)
                if has_real_data
                else None
            )

        else:
            calc = 0.0
            liquid_profit = None
            expected_total_balance = None
            reserve_addition = 0.0
            real_profit = None
            real_profit_accum = None

        calc_running += float(calc or 0.0)

        actual_balance = (
            getattr(b, "actual_cash_balance", None)
            if b is not None
            else None
        )

        reserve_movements.append(
            {
                "net_liquidity": float(liquid_profit or 0.0)
                if liquid_profit is not None
                else 0.0,
                "reserve_addition": reserve_addition,
                "actual_balance": actual_balance,
            }
        )
        reserve_state = compute_reserved_funds_series(
            opening_reserved_funds,
            reserve_movements,
        )[-1]
        reserved_funds_available = reserve_state["reserve_available"]

        if expected_total_balance is not None:
            liquid_profit_running = compute_comparable_liquid_balance(
                expected_total_balance,
                reserved_funds_available,
            )

        real_profit_accum = (
            reserve_state["real_month_available"]
            if actual_balance is not None
            else None
        )

        # Los días anteriores al filtro solo sirven para reconstruir el
        # acumulado mensual; no se muestran en la tabla ni en el gráfico.
        if d < d1:
            continue

        cmp_rows.append(
            {
                "date": d,
                "date_ar": fmt_date_ar(d),
                "date_iso": d.isoformat(),
                "calc": calc,
                "calc_accum": calc_running,
                "liquid_profit": liquid_profit,
                "liquid_profit_accum": liquid_profit_running,
                "real_profit": real_profit,
                "real_profit_accum": real_profit_accum,
                "reserved_funds_available": reserved_funds_available,
            }
        )

        cmp_dates.append(d.isoformat())
        cmp_labels.append(fmt_date_ar(d))

        cmp_calc.append(round(calc, 2))
        cmp_liquid_profit.append(
            None if liquid_profit is None else round(float(liquid_profit), 2)
        )
        cmp_real_profit.append(
            None if real_profit is None else round(float(real_profit), 2)
        )

        cmp_calc_accum.append(round(calc_running, 2))
        cmp_liquid_profit_accum.append(round(liquid_profit_running, 2))
        cmp_real_profit_accum.append(
            None if real_profit_accum is None else round(float(real_profit_accum), 2)
        )
        cmp_reserved_funds.append(round(reserved_funds_available, 2))

    # Los KPI toman el último valor mensual disponible dentro del filtro.
    latest_real_accum = next(
        (value for value in reversed(cmp_real_profit_accum) if value is not None),
        None,
    )
    latest_reserved_funds = (
        cmp_reserved_funds[-1] if cmp_reserved_funds else 0.0
    )

    cmp_payload = {
        "dates": cmp_dates,
        "labels": cmp_labels,
        "calc": cmp_calc,
        "liquid_profit": cmp_liquid_profit,
        "real_profit": cmp_real_profit,
        "calc_accum": cmp_calc_accum,
        "liquid_profit_accum": cmp_liquid_profit_accum,
        "real_profit_accum": cmp_real_profit_accum,
    }
    cmp_json = json.dumps(cmp_payload, ensure_ascii=False)

    def _row_has_data(r):
        return (
            abs(float(r["calc"] or 0.0)) > 0
            or r["liquid_profit"] is not None
            or r["real_profit"] is not None
            or r["real_profit_accum"] is not None
        )

    rows_with_data = [
        r for r in cmp_rows
        if _row_has_data(r)
    ]

    rows_with_data.sort(key=lambda r: r["date"], reverse=True)

    def _fmt_or_dash(value):
        if value is None:
            return "<span class='muted'>—</span>"
        return ars(value)

    def _cmp_tr(r):
        liquid_profit_html = _fmt_or_dash(r["liquid_profit"])
        real_profit_html = _fmt_or_dash(r["real_profit"])
        real_profit_accum_html = _fmt_or_dash(r["real_profit_accum"])

        desfasaje_html = _desfasaje_pct(
            r["liquid_profit_accum"],
            r["real_profit_accum"],
        )

        return (
            "<tr>"
            f"<td>{r['date_ar']}</td>"
            f"<td class='num' style='color:#2563eb; font-weight:800;'>{ars(r['calc'])}</td>"
            f"<td class='num' style='color:#2563eb; font-weight:700;'>{ars(r['calc_accum'])}</td>"
            f"<td class='num' style='color:#7c3aed; font-weight:800;'>{liquid_profit_html}</td>"
            f"<td class='num' style='color:#7c3aed; font-weight:700;'>{ars(r['liquid_profit_accum'])}</td>"
            f"<td class='num' style='color:#16a34a; font-weight:800;'>{real_profit_html}</td>"
            f"<td class='num' style='color:#16a34a; font-weight:700;'>{real_profit_accum_html}</td>"
            f"<td class='num'>{desfasaje_html}</td>"
            "</tr>"
        )

    head_html = (
        "".join(_cmp_tr(r) for r in rows_with_data)
        if rows_with_data
        else "<tr><td colspan='8' class='muted'>Sin datos</td></tr>"
    )

    details_html = ""

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
        <div class="label">Ganancia Líquida Acumulada</div>
        <div class="value">{ars(cmp_liquid_profit_accum[-1] if cmp_liquid_profit_accum else 0.0)}</div>
        <div class="muted">Saldo esperado al cierre menos fondos reservados disponibles</div>
      </div>

      <div class="card kpi blue">
        <div class="label">Ganancia Calculada Acumulada</div>
        <div class="value">{ars(cmp_calc_accum[-1] if cmp_calc_accum else 0.0)}</div>
        <div class="muted">Acumulado del mes</div>
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
        <div class="label">Cobro por Apps (próximo viernes)</div>
        <div class="value">{ars(apps_a_cobrar_estimado)}</div>
        <div class="muted">período: {periodo_cobro_str}</div>
      </div>

      <div class="card kpi">
        <div class="label">Sueldo Ximena</div>
        <div class="value">{ars(sueldo_ximena)}</div>
        <div class="muted">Gasto fijo en el rango</div>
      </div>

      <div class="card kpi income">
        <div class="label">Liquidez Real Atribuible al Mes</div>
        <div class="value">{ars(latest_real_accum) if latest_real_accum is not None else "—"}</div>
        <div class="muted">Liquidez real total descontando solo la reserva aún disponible</div>
      </div>

      <div class="card kpi">
        <div class="label">Fondos reservados disponibles</div>
        <div class="value">{ars(latest_reserved_funds)}</div>
        <div class="muted">Saldo anterior + agregados - consumos</div>
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
      <h3>Bloque 5 · Control: Ganancia Calculada, Líquida y Real</h3>
      <div class="chartbox"><canvas id="profitCompareChart"></canvas></div>
      <p class="muted" style="margin-top:10px;">
        Azul = ganancia calculada (ventas - gastos). Violeta sólido = ganancia líquida diaria. Violeta punteado = saldo esperado al cierre descontando solo la reserva aún disponible. Verde sólido = ganancia real cargada manualmente. Verde punteado = liquidez real atribuible al mes.
      </p>

      <div style="height:10px;"></div>

      <table>
        <thead>
          <tr>
            <th>Fecha</th>
            <th class="num">Calculada</th>
            <th class="num">Calc.<br>Acum.</th>
            <th class="num">Líquida</th>
            <th class="num">Líq.<br>Acum.</th>
            <th class="num">Real</th>
            <th class="num">Real<br>Acum.</th>
            <th class="num">Desf.<br>%</th>
          </tr>
        </thead>
        <tbody>{head_html}</tbody>
      </table>

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
        const n = Math.round(Number(v || 0));
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
        const colorCalc = '#2563eb';
        const colorLiquidProfit = '#7c3aed';
        const colorRealProfit = '#16a34a';

        profitCompareChartInstance = new Chart(pc, {{
          type: 'line',
          data: {{
            labels: profitCmp.labels,
            datasets: [
              {{
                label: 'Ganancia Calculada (diaria)',
                data: profitCmp.calc,
                tension: 0.25,
                fill: false,
                borderWidth: 2,
                pointRadius: 3,
                spanGaps: false,
                borderColor: colorCalc,
                backgroundColor: colorCalc,
                pointBackgroundColor: colorCalc
              }},
              {{
                label: 'Ganancia Líquida (diaria)',
                data: profitCmp.liquid_profit,
                tension: 0.25,
                fill: false,
                borderWidth: 2,
                pointRadius: 3,
                spanGaps: false,
                borderColor: colorLiquidProfit,
                backgroundColor: colorLiquidProfit,
                pointBackgroundColor: colorLiquidProfit
              }},
              {{
                label: 'Ganancia Real (diaria)',
                data: profitCmp.real_profit,
                tension: 0.25,
                fill: false,
                borderWidth: 2,
                pointRadius: 3,
                spanGaps: false,
                borderColor: colorRealProfit,
                backgroundColor: colorRealProfit,
                pointBackgroundColor: colorRealProfit
              }},
              {{
                label: 'Ganancia Calculada Acumulada',
                data: profitCmp.calc_accum,
                tension: 0.2,
                fill: false,
                borderWidth: 2,
                pointRadius: 2,
                borderDash: [6, 4],
                spanGaps: false,
                borderColor: colorCalc,
                backgroundColor: colorCalc,
                pointBackgroundColor: colorCalc
              }},
              {{
                label: 'Liquidez Esperada Comparable',
                data: profitCmp.liquid_profit_accum,
                tension: 0.2,
                fill: false,
                borderWidth: 2,
                pointRadius: 2,
                borderDash: [6, 4],
                spanGaps: false,
                borderColor: colorLiquidProfit,
                backgroundColor: colorLiquidProfit,
                pointBackgroundColor: colorLiquidProfit
              }},
              {{
                label: 'Liquidez Real del Mes',
                data: profitCmp.real_profit_accum,
                tension: 0.2,
                fill: false,
                borderWidth: 2,
                pointRadius: 2,
                borderDash: [6, 4],
                spanGaps: false,
                borderColor: colorRealProfit,
                backgroundColor: colorRealProfit,
                pointBackgroundColor: colorRealProfit
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
    v_apps_collected = (request.form.get("real_apps_collected") or "").strip()

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
    apps_collected = None if v_apps_collected == "" else safe_float(v_apps_collected)

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
    bday.real_apps_collected = apps_collected

    total = None
    if cash is not None or digital is not None or apps_collected is not None:
        total = float(cash or 0.0) + float(digital or 0.0) + float(apps_collected or 0.0)
        bday.real_profit = total
    else:
        bday.real_profit = None

    ensure_shifts(bday)
    recalc_day_status(bday)
    db.session.commit()

    t = day_totals(bday)
    calc = float(t["profit"])
    total_html = _total_html(calc, total)

    explained_total = compute_explained_total(cash, digital, apps, apps_collected)

    if explained_total is not None:
        explained_html = _explained_html(calc, explained_total)
        desfasaje_html = _desfasaje_html(calc, explained_total)
    else:
        explained_html = "<span class='muted'>—</span>"
        desfasaje_html = "<span class='muted'>—</span>"

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