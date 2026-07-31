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

    # Rangos específicos del dashboard:
    # Malo <= 10%; Regular > 10% y < 20%; Bueno >= 20%.
    if margen_periodo is None:
        bucket_label, bucket_class = "—", "pill"
    elif margen_periodo <= 10:
        bucket_label, bucket_class = "Malo", "pill bad"
    elif margen_periodo < 20:
        bucket_label, bucket_class = "Regular", "pill warn"
    else:
        bucket_label, bucket_class = "Bueno", "pill ok"

    if bucket_label == "Malo":
        margin_card_style = "background:rgba(220,38,38,.10); border-color:rgba(220,38,38,.25);"
    elif bucket_label == "Regular":
        margin_card_style = "background:rgba(245,158,11,.14); border-color:rgba(245,158,11,.32);"
    elif bucket_label == "Bueno":
        margin_card_style = "background:rgba(22,163,74,.11); border-color:rgba(22,163,74,.24);"
    else:
        margin_card_style = ""

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

    def rank_rows(items, positive_highlight=False):
        if not items:
            return "<tr><td colspan='3' class='muted'>Sin datos</td></tr>"
        out = ""
        for rr in items:
            if positive_highlight:
                profit_style = "color:#16a34a; font-weight:800;"
                profit_class = ""
            else:
                profit_style = ""
                profit_class = "neg" if rr["profit"] < 0 else ""
            out += (
                "<tr>"
                f"<td>{rr['date_ar']}</td>"
                f"<td class='num'>{ars(rr['income'])}</td>"
                f"<td class='num {profit_class}' style='{profit_style}'>{ars(rr['profit'])}</td>"
                "</tr>"
            )
        return out

    best_html = rank_rows(best3, positive_highlight=True)
    worst_html = rank_rows(worst3)

    if income > 0:
        pie_labels = ["Ingresos", "Gastos", "Ganancia"] if profit >= 0 else ["Ingresos", "Gastos", "Pérdida"]
        pie_values = [max(income, 0), max(expense, 0), max(profit, 0) if profit >= 0 else abs(profit)]
    else:
        pie_labels = ["Ingresos", "Gastos", "Ganancia"]
        pie_values = [0, 0, 0]

    # =========================================================
    # ANÁLISIS MENSUAL DE LIQUIDEZ DEL AÑO SELECCIONADO
    # =========================================================
    analysis_year = d2.year
    base_month_labels = [
        "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
        "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
    ]

    # Barras verdes: ingresos líquidos efectivamente registrados.
    # Barras rojas: gastos del mismo conjunto de días.
    # Barra violeta: saldo líquido comparable al cierre de cada mes,
    # calculado con exactamente la misma lógica que el KPI superior.
    monthly_liquid_income = [0.0] * 12
    monthly_liquid_expense = [0.0] * 12
    monthly_liquid_profit = [None] * 12
    monthly_total_days = [0] * 12
    monthly_liquid_days = [0] * 12

    annual_series = range_series(
        date(analysis_year, 1, 1),
        date(analysis_year, 12, 31),
    )
    annual_bdays = (
        BusinessDay.query
        .filter(
            BusinessDay.day >= date(analysis_year, 1, 1),
            BusinessDay.day <= date(analysis_year, 12, 31),
        )
        .order_by(BusinessDay.day.asc())
        .all()
    )
    annual_bmap = {b.day: b for b in annual_bdays}
    annual_items_by_month = {month: [] for month in range(1, 13)}

    for item in annual_series:
        item_day = parse_ymd(item["date"])
        annual_items_by_month[item_day.month].append(item)

    for month_number in range(1, 13):
        month_index = month_number - 1
        month_items = annual_items_by_month[month_number]
        if not month_items:
            continue

        monthly_total_days[month_index] = len(month_items)

        month_bdays = [
            annual_bmap[parse_ymd(item["date"])]
            for item in month_items
            if parse_ymd(item["date"]) in annual_bmap
        ]
        opening_reserved_funds = (
            float(getattr(month_bdays[0], "opening_cash_balance", 0.0) or 0.0)
            if month_bdays
            else 0.0
        )

        reserve_movements = []
        last_comparable_balance = None

        for item in month_items:
            item_day = parse_ymd(item["date"])
            b = annual_bmap.get(item_day)
            if not b:
                continue

            has_liquid_data = (
                getattr(b, "daily_mercadopago", None) is not None
                or getattr(b, "daily_cash_withdrawn", None) is not None
                or getattr(b, "real_apps_collected", None) is not None
            )

            daily_liquidity = (
                float(getattr(b, "daily_mercadopago", 0.0) or 0.0)
                + float(getattr(b, "daily_cash_withdrawn", 0.0) or 0.0)
                + float(getattr(b, "real_apps_collected", 0.0) or 0.0)
            )
            expense_day = float(item["expense_total"] or 0.0)
            net_liquidity = daily_liquidity - expense_day if has_liquid_data else 0.0

            if has_liquid_data:
                monthly_liquid_days[month_index] += 1
                monthly_liquid_income[month_index] += daily_liquidity
                monthly_liquid_expense[month_index] += expense_day
                expected_total_balance = compute_expected_cash_balance(
                    opening_balance=getattr(b, "opening_cash_balance", None),
                    cash_income=daily_liquidity,
                    paid_expenses=expense_day,
                    safe_box_transfer=getattr(b, "safe_box_transfer", None),
                )
            elif getattr(b, "expected_cash_balance", None) is not None:
                expected_total_balance = float(b.expected_cash_balance)
            else:
                expected_total_balance = None

            reserve_movements.append(
                {
                    "net_liquidity": float(net_liquidity),
                    "reserve_addition": max(
                        float(getattr(b, "safe_box_transfer", 0.0) or 0.0),
                        0.0,
                    ),
                    "actual_balance": getattr(b, "actual_cash_balance", None),
                }
            )
            reserve_state = compute_reserved_funds_series(
                opening_reserved_funds,
                reserve_movements,
            )[-1]

            if expected_total_balance is not None:
                last_comparable_balance = compute_comparable_liquid_balance(
                    expected_total_balance,
                    reserve_state["reserve_available"],
                )

        if monthly_liquid_days[month_index] > 0:
            monthly_liquid_profit[month_index] = last_comparable_balance

    # El gráfico arranca en el primer mes con carga líquida completa.
    # Para 2026, ese mes es junio.
    first_complete_month_index = next(
        (
            i
            for i in range(12)
            if monthly_total_days[i] > 0
            and monthly_liquid_days[i] == monthly_total_days[i]
        ),
        None,
    )
    if first_complete_month_index is None:
        first_complete_month_index = next(
            (i for i in range(12) if monthly_liquid_days[i] > 0),
            0,
        )

    visible_month_indexes = list(range(first_complete_month_index, 12))
    month_labels = [base_month_labels[i] for i in visible_month_indexes]

    monthly_margin_pct = [
        (
            (monthly_liquid_profit[i] / monthly_liquid_income[i]) * 100.0
            if monthly_liquid_profit[i] is not None
            and abs(monthly_liquid_income[i]) > 1e-9
            else None
        )
        for i in visible_month_indexes
    ]

    def _monthly_table_value(value):
        return "—" if value is None else ars(value)

    month_header_html = "".join(
        f"<th class='num'>{base_month_labels[i]}</th>"
        for i in visible_month_indexes
    )
    monthly_income_cells = "".join(
        f"<td class='num'>{_monthly_table_value(monthly_liquid_income[i] if monthly_liquid_days[i] > 0 else None)}</td>"
        for i in visible_month_indexes
    )
    monthly_expense_cells = "".join(
        f"<td class='num'>{_monthly_table_value(monthly_liquid_expense[i] if monthly_liquid_days[i] > 0 else None)}</td>"
        for i in visible_month_indexes
    )
    monthly_profit_cells = "".join(
        f"<td class='num'>{_monthly_table_value(monthly_liquid_profit[i])}</td>"
        for i in visible_month_indexes
    )
    charts_payload = {
        "pie": {
            "labels": pie_labels,
            "values": pie_values,
        },
        "monthly": {
            "year": analysis_year,
            "labels": month_labels,
            "income": [
                round(monthly_liquid_income[i], 2)
                if monthly_liquid_days[i] > 0
                else None
                for i in visible_month_indexes
            ],
            "expense": [
                round(monthly_liquid_expense[i], 2)
                if monthly_liquid_days[i] > 0
                else None
                for i in visible_month_indexes
            ],
            "profit": [
                None
                if monthly_liquid_profit[i] is None
                else round(monthly_liquid_profit[i], 2)
                for i in visible_month_indexes
            ],
            "margin_pct": [
                None if value is None else round(value, 2)
                for value in monthly_margin_pct
            ],
        },
    }
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

    ingresos_liquidos_acumulados = sum(
        float(getattr(b, "daily_mercadopago", 0.0) or 0.0)
        + float(getattr(b, "daily_cash_withdrawn", 0.0) or 0.0)
        + float(getattr(b, "real_apps_collected", 0.0) or 0.0)
        for b in bdays
        if not is_sunday(b.day)
    )

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
      <!-- Fila 1 -->
      <div class="card kpi" style="background:rgba(22,163,74,.11); border-color:rgba(22,163,74,.24);">
        <div class="label">Ingresos brutos</div>
        <div class="value">{ars(income)}</div>
        <div class="muted">Ventas registradas en el período</div>
      </div>

      <div class="card kpi expense">
        <div class="label">Gastos</div>
        <div class="value">{ars(expense)}</div>
        <div class="muted">Gastos totales del período</div>
      </div>

      <div class="card kpi blue">
        <div class="label">Ganancia calculada acumulada</div>
        <div class="value">{ars(cmp_calc_accum[-1] if cmp_calc_accum else 0.0)}</div>
        <div class="muted">Ventas menos gastos</div>
      </div>

      <div class="card kpi" style="background:rgba(22,163,74,.17); border-color:rgba(22,163,74,.34);">
        <div class="label">Ganancia real acumulada</div>
        <div class="value">{ars(latest_real_accum) if latest_real_accum is not None else "—"}</div>
        <div class="muted">Liquidez real atribuible al mes</div>
      </div>

      <!-- Fila 2 -->
      <div class="card kpi" style="background:rgba(5,150,105,.14); border-color:rgba(5,150,105,.30);">
        <div class="label">Ingresos líquidos acumulados</div>
        <div class="value">{ars(ingresos_liquidos_acumulados)}</div>
        <div class="muted">Apps cobradas + Mercado Pago + efectivo retirado</div>
      </div>

      <div class="card kpi" style="{margin_card_style}">
        <div class="margen-kpi">
          <div class="margen-left">
            <div class="label">Margen</div>
            <div class="value">{(f"{margen_periodo:.1f}%" if margen_periodo is not None else "—")}</div>
            <div style="margin-top:6px;"><span class="{bucket_class}">{bucket_label}</span></div>
          </div>

          <div class="margen-right">
            <div class="muted">Ref.</div>
            <span class="pill bad">Malo ≤ 10</span>
            <span class="pill warn">Regular &lt; 20</span>
            <span class="pill ok">Bueno ≥ 20</span>
          </div>
        </div>
      </div>

      <div class="card kpi" style="background:rgba(124,58,237,.13); border-color:rgba(124,58,237,.30);">
        <div class="label">Ganancia líquida acumulada</div>
        <div class="value">{ars(cmp_liquid_profit_accum[-1] if cmp_liquid_profit_accum else 0.0)}</div>
        <div class="muted">Saldo esperado al cierre menos fondos reservados</div>
      </div>

      <div class="card kpi" style="background:rgba(244,63,94,.11); border-color:rgba(244,63,94,.27);">
        <div class="label">Sueldo Ximena</div>
        <div class="value">{ars(sueldo_ximena)}</div>
        <div class="muted">Gasto fijo en el rango</div>
      </div>

      <!-- Fila 3 -->
      <div class="card kpi">
        <div class="label">Cobro por Apps (próximo viernes)</div>
        <div class="value">{ars(apps_a_cobrar_estimado)}</div>
        <div class="muted">Período: {periodo_cobro_str}</div>
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
        <h3>Bloque 2 · Mejores y peores días (ganancia)</h3>

        <div class="ranking-section">
          <h4>Top 3 mejores días</h4>
          <table>
            <thead><tr><th>Fecha</th><th class="num">Ingresos</th><th class="num">Ganancia</th></tr></thead>
            <tbody>{best_html}</tbody>
          </table>
        </div>

        <div class="ranking-section">
          <h4>Top 3 peores días</h4>
          <table>
            <thead><tr><th>Fecha</th><th class="num">Ingresos</th><th class="num">Ganancia</th></tr></thead>
            <tbody>{worst_html}</tbody>
          </table>
        </div>
      </div>
    </div>

    <div class="card">
      <h3>Bloque 3 · Resumen mensual de liquidez {analysis_year}</h3>
      <div style="overflow-x:auto;">
        <table class="monthly-summary-table">
          <thead>
            <tr>
              <th>Mes</th>
              {month_header_html}
            </tr>
          </thead>
          <tbody>
            <tr>
              <th style="color:#16a34a;">Ingresos líquidos</th>
              {monthly_income_cells}
            </tr>
            <tr>
              <th style="color:#dc2626;">Gastos</th>
              {monthly_expense_cells}
            </tr>
            <tr>
              <th style="color:#7c3aed;">Ganancia líquida</th>
              {monthly_profit_cells}
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="card">
      <h3>Bloque 4 · Análisis mensual de liquidez {analysis_year}</h3>
      <div class="chartbox monthly-chartbox"><canvas id="monthlyBarChart"></canvas></div>
      <p class="muted" style="margin-top:10px;">
        Ingresos líquidos = Apps cobradas + Mercado Pago diario + efectivo retirado.
        La barra violeta muestra la ganancia líquida acumulada al cierre de cada mes, con la misma lógica que el KPI superior.
        El porcentaje sobre cada grupo representa ganancia líquida / ingresos líquidos.
      </p>
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

      const monthlyValuePlugin = {{
        id: 'monthlyValuePlugin',
        afterDatasetsDraw(chart) {{
          if (chart.canvas.id !== 'monthlyBarChart') return;

          const ctx = chart.ctx;

          function drawRoundedLabel(x, y, text, options = {{}}) {{
            const paddingX = options.paddingX ?? 8;
            const paddingY = options.paddingY ?? 4;
            const radius = options.radius ?? 8;
            const bg = options.bg ?? 'rgba(255,255,255,0.96)';
            const border = options.border ?? 'rgba(148,163,184,0.65)';
            const color = options.color ?? '#111827';

            ctx.save();
            ctx.font = options.font ?? '700 10px Arial';
            const metrics = ctx.measureText(text);
            const width = metrics.width + paddingX * 2;
            const height = 18 + paddingY * 2;
            const left = x - width / 2;
            const top = y - height / 2;

            ctx.beginPath();
            ctx.moveTo(left + radius, top);
            ctx.lineTo(left + width - radius, top);
            ctx.quadraticCurveTo(left + width, top, left + width, top + radius);
            ctx.lineTo(left + width, top + height - radius);
            ctx.quadraticCurveTo(left + width, top + height, left + width - radius, top + height);
            ctx.lineTo(left + radius, top + height);
            ctx.quadraticCurveTo(left, top + height, left, top + height - radius);
            ctx.lineTo(left, top + radius);
            ctx.quadraticCurveTo(left, top, left + radius, top);
            ctx.closePath();

            ctx.fillStyle = bg;
            ctx.fill();
            ctx.lineWidth = 1;
            ctx.strokeStyle = border;
            ctx.stroke();

            ctx.fillStyle = color;
            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.fillText(text, x, y + 0.5);
            ctx.restore();
          }}

          function drawLeaderLabel(bar, text, direction) {{
            const anchorX = bar.x;
            const anchorY = bar.y;
            const labelX = anchorX + (direction === 'left' ? -38 : 38);
            const labelY = anchorY - 22;

            ctx.save();
            ctx.strokeStyle = 'rgba(107,114,128,0.9)';
            ctx.lineWidth = 1.2;
            ctx.beginPath();
            ctx.moveTo(anchorX, anchorY - 2);
            ctx.lineTo(anchorX, anchorY - 14);
            ctx.lineTo(labelX, labelY + 8);
            ctx.stroke();
            ctx.restore();

            drawRoundedLabel(labelX, labelY, text, {{
              bg: 'rgba(255,255,255,0.98)',
              border: 'rgba(203,213,225,0.95)',
              color: '#1f2937',
              font: '700 10px Arial',
              paddingX: 7,
              paddingY: 3,
              radius: 7
            }});
          }}

          ctx.save();

          chart.data.datasets.forEach((dataset, datasetIndex) => {{
            const meta = chart.getDatasetMeta(datasetIndex);
            if (meta.hidden) return;

            meta.data.forEach((bar, index) => {{
              const raw = dataset.data[index];
              if (raw === null || raw === undefined) return;
              const value = Number(raw);
              if (!Number.isFinite(value) || value === 0) return;

              const label = fmtMoney(value);

              if (datasetIndex === 0) {{
                drawLeaderLabel(bar, label, 'left');
              }} else if (datasetIndex === 1) {{
                drawLeaderLabel(bar, label, 'right');
              }} else {{
                const y = value >= 0 ? bar.y - 8 : bar.y + 18;
                drawRoundedLabel(bar.x, y, label, {{
                  bg: 'rgba(255,255,255,0.96)',
                  border: 'rgba(203,213,225,0.9)',
                  color: '#4c1d95',
                  font: '700 10px Arial',
                  paddingX: 7,
                  paddingY: 3,
                  radius: 7
                }});
              }}
            }});
          }});

          ctx.font = '800 12px Arial';
          ctx.fillStyle = '#111827';
          ctx.textAlign = 'center';
          ctx.textBaseline = 'middle';

          const marginValues = payload.monthly.margin_pct || [];
          chart.data.labels.forEach((_, index) => {{
            const pct = marginValues[index];
            if (pct === null || pct === undefined || !Number.isFinite(Number(pct))) return;

            const bars = [];
            chart.data.datasets.forEach((dataset, datasetIndex) => {{
              const raw = dataset.data[index];
              const meta = chart.getDatasetMeta(datasetIndex);
              if (
                !meta.hidden
                && raw !== null
                && raw !== undefined
                && meta.data[index]
              ) {{
                bars.push(meta.data[index]);
              }}
            }});

            if (!bars.length) return;

            const centerX = bars.reduce((sum, bar) => sum + bar.x, 0) / bars.length;
            const percentageLevel = 29_000_000;
            const labelY = chart.scales.y.getPixelForValue(percentageLevel);
            const pctLabel = Number(pct).toLocaleString('es-AR', {{
              minimumFractionDigits: 1,
              maximumFractionDigits: 1
            }}) + '%';

            drawRoundedLabel(centerX, labelY, pctLabel, {{
              bg: 'rgba(255,255,255,0.98)',
              border: 'rgba(156,163,175,0.9)',
              color: '#111827',
              font: '800 11px Arial',
              paddingX: 8,
              paddingY: 3,
              radius: 8
            }});
          }});

          ctx.restore();
        }}
      }};

      const monthlyCanvas = document.getElementById('monthlyBarChart');
      if (monthlyCanvas) {{
        new Chart(monthlyCanvas, {{
          type: 'bar',
          data: {{
            labels: payload.monthly.labels,
            datasets: [
              {{
                label: 'Ingresos líquidos',
                categoryPercentage: 0.84,
                barPercentage: 0.94,
                maxBarThickness: 58,
                data: payload.monthly.income,
                backgroundColor: 'rgba(22,163,74,0.82)',
                borderColor: '#16a34a',
                borderWidth: 1
              }},
              {{
                label: 'Gastos',
                categoryPercentage: 0.84,
                barPercentage: 0.94,
                maxBarThickness: 58,
                data: payload.monthly.expense,
                backgroundColor: 'rgba(220,38,38,0.78)',
                borderColor: '#dc2626',
                borderWidth: 1
              }},
              {{
                label: 'Ganancia líquida acumulada',
                categoryPercentage: 0.84,
                barPercentage: 0.94,
                maxBarThickness: 58,
                data: payload.monthly.profit,
                backgroundColor: 'rgba(124,58,237,0.82)',
                borderColor: '#7c3aed',
                borderWidth: 1
              }}
            ]
          }},
          options: {{
            responsive: true,
            maintainAspectRatio: false,
            layout: {{
              padding: {{ top: 68 }}
            }},
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
              x: {{
                grid: {{ display: false }}
              }},
              y: {{
                beginAtZero: true,
                max: 30000000,
                ticks: {{
                  stepSize: 5000000,
                  callback: function(value){{ return fmtMoney(value); }}
                }}
              }}
            }}
          }},
          plugins: [monthlyValuePlugin]
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