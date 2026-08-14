from datetime import date
import json
from datetime import timedelta

from flask import Blueprint, request, jsonify
from flask_login import login_required
from sqlalchemy import func
from app.services.finance_service import (
    APPS_RETENTION_FACTOR,
    compute_explained_total,
    compute_expected_cash_balance,
    compute_available_liquidity_change,
    resolve_reserved_funds_balance,
    compute_calc_liquid_reconciliation,
    compute_reconciliation_status,
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

def _render_page(*args, **kwargs):
    from app_owners import render_page
    return render_page(*args, **kwargs)


def _helpers():
    from app_owners import ensure_shifts, recalc_day_status, day_totals, margin_bucket, range_series
    return ensure_shifts, recalc_day_status, day_totals, margin_bucket, range_series


def _money_input(v):
    return "" if v is None else str(float(v))


def _reserved_funds_before(day):
    previous_change = (
        BusinessDay.query
        .filter(BusinessDay.day < day)
        .filter(BusinessDay.reserved_funds_balance.isnot(None))
        .order_by(BusinessDay.day.desc())
        .first()
    )
    if previous_change is None:
        return 0.0
    return max(
        float(getattr(previous_change, "reserved_funds_balance", 0.0) or 0.0),
        0.0,
    )


def _real_opening_base(month_start, month_days=None):
    previous_close = (
        BusinessDay.query
        .filter(BusinessDay.day < month_start)
        .filter(BusinessDay.actual_cash_balance.isnot(None))
        .order_by(BusinessDay.day.desc())
        .first()
    )
    if previous_close is not None:
        return float(previous_close.actual_cash_balance or 0.0)

    month_days = month_days or []
    if month_days:
        return float(getattr(month_days[0], "opening_cash_balance", 0.0) or 0.0)
    return 0.0


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
    apps_retention_estimate = sum(
        x.get("apps_retention_estimate", 0.0)
        for x in series
    )
    profit_raw = sum(x.get("profit_raw", x["profit"]) for x in series)
    profit = sum(x.get("profit_adjusted", x["profit"]) for x in series)

    margen_periodo = (profit / income * 100.0) if income else None

    # Rangos específicos del dashboard:
    # Malo <= 10%; Regular > 10% y < 20%; Bueno >= 20%.
    def margin_visual_state(value):
        if value is None:
            return "—", "pill", ""
        if value <= 10:
            return (
                "Malo",
                "pill bad",
                "background:rgba(220,38,38,.10); border-color:rgba(220,38,38,.25);",
            )
        if value < 20:
            return (
                "Regular",
                "pill warn",
                "background:rgba(245,158,11,.14); border-color:rgba(245,158,11,.32);",
            )
        return (
            "Bueno",
            "pill ok",
            "background:rgba(22,163,74,.11); border-color:rgba(22,163,74,.24);",
        )

    bucket_label, bucket_class, margin_card_style = margin_visual_state(margen_periodo)

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
        day_profit = x.get("profit_adjusted", x["profit"])
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
    # Barra violeta: variación de la liquidez disponible respecto de la base
    # real de apertura de cada mes. La liquidez esperada conserva su lógica
    # diaria: cada cierre parte del saldo real de apertura de ese día.
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
        month_start_date = date(analysis_year, month_number, 1)
        month_bdays = [
            annual_bmap[parse_ymd(item["date"])]
            for item in month_items
            if parse_ymd(item["date"]) in annual_bmap
        ]
        month_base_real = _real_opening_base(month_start_date, month_bdays)
        running_reserved = _reserved_funds_before(month_start_date)
        liquid_result_running = 0.0

        for item in month_items:
            item_day = parse_ymd(item["date"])
            b = annual_bmap.get(item_day)
            if not b:
                continue

            explicit_reserved = getattr(b, "reserved_funds_balance", None)
            current_reserved = resolve_reserved_funds_balance(
                running_reserved,
                explicit_reserved,
            )

            daily_liquidity = (
                float(getattr(b, "daily_mercadopago", 0.0) or 0.0)
                + float(getattr(b, "daily_cash_withdrawn", 0.0) or 0.0)
                + float(getattr(b, "real_apps_collected", 0.0) or 0.0)
            )
            expense_day = float(item["expense_total"] or 0.0)

            has_liquid_data = (
                getattr(b, "daily_mercadopago", None) is not None
                or getattr(b, "daily_cash_withdrawn", None) is not None
                or getattr(b, "real_apps_collected", None) is not None
            )
            has_liquid_activity = (
                has_liquid_data
                or explicit_reserved is not None
                or abs(expense_day) > 1e-9
            )

            if has_liquid_data:
                monthly_liquid_days[month_index] += 1
                monthly_liquid_income[month_index] += daily_liquidity
                monthly_liquid_expense[month_index] += expense_day

            if has_liquid_activity:
                daily_liquid_change = compute_available_liquidity_change(
                    cash_income=daily_liquidity,
                    paid_expenses=expense_day,
                    previous_reserved_funds=running_reserved,
                    current_reserved_funds=current_reserved,
                )
                liquid_result_running += daily_liquid_change

            running_reserved = current_reserved

        if monthly_liquid_days[month_index] > 0:
            monthly_liquid_profit[month_index] = liquid_result_running

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

    cmp_rows = []

    # El eje X muestra todos los días laborables del período. Los días todavía
    # no cargados se representan con valores None, de modo que Chart.js deja
    # el espacio de la fecha pero no prolonga ninguna curva artificialmente.
    cmp_dates = []
    cmp_labels = []
    cmp_calc = []
    cmp_liquid_profit = []
    cmp_real_profit = []
    cmp_calc_balance = []
    cmp_liquid_balance = []
    cmp_real_balance = []
    cmp_reserved_markers = []

    # Resultados mensuales desde cero. Se mantienen separados de los saldos
    # del gráfico para que los KPI continúen midiendo resultado del mes.
    cmp_calc_result_accum = []
    cmp_liquid_result_accum = []
    cmp_real_result_accum = []

    # Para un filtro iniciado a mitad de mes reconstruimos internamente desde
    # el día 1, pero solo mostramos las fechas incluidas en el filtro.
    chart_start = d1.replace(day=1)
    chart_days = list(iter_workdays(chart_start, d2))
    chart_bdays = (
        BusinessDay.query
        .filter(BusinessDay.day >= chart_start, BusinessDay.day <= d2)
        .order_by(BusinessDay.day.asc())
        .all()
    )
    chart_bmap = {b.day: b for b in chart_bdays}

    current_month_key = None
    month_base_real = 0.0
    calc_result_running = 0.0
    liquid_result_running = 0.0
    running_reserved_funds = 0.0
    first_visible_month_point = True

    for d in chart_days:
        month_key = (d.year, d.month)
        if month_key != current_month_key:
            current_month_key = month_key
            month_start_date = d.replace(day=1)
            month_bdays = [
                item
                for item in chart_bdays
                if item.day.year == d.year and item.day.month == d.month
            ]
            month_base_real = _real_opening_base(
                month_start_date,
                month_bdays,
            )
            running_reserved_funds = _reserved_funds_before(month_start_date)
            calc_result_running = 0.0
            liquid_result_running = 0.0
            first_visible_month_point = True

        b = chart_bmap.get(d)

        # Por defecto el día no aporta puntos al gráfico.
        calc = None
        liquid_profit = None
        real_profit = None
        calc_balance = None
        liquid_balance = None
        real_balance = None
        real_result_running = None
        current_reserved_funds = running_reserved_funds
        reserve_marker = None
        has_calc_data = False
        has_liquid_activity = False
        has_real_data = False
        actual_balance = None

        if b:
            ensure_shifts(b)
            recalc_day_status(b)
            t = day_totals(b)

            expense_day = float(t["expense_total"] or 0.0)
            has_calc_data = (
                abs(float(t["income"] or 0.0)) > 1e-9
                or abs(expense_day) > 1e-9
                or getattr(b, "real_apps_pending", None) is not None
            )

            if has_calc_data:
                calc = float(t.get("profit_adjusted", t["profit"]) or 0.0)
                calc_result_running += calc
                calc_balance = month_base_real + calc_result_running

            daily_liquidity = (
                float(getattr(b, "daily_mercadopago", 0.0) or 0.0)
                + float(getattr(b, "daily_cash_withdrawn", 0.0) or 0.0)
                + float(getattr(b, "real_apps_collected", 0.0) or 0.0)
            )

            explicit_reserved = getattr(b, "reserved_funds_balance", None)
            current_reserved_funds = resolve_reserved_funds_balance(
                running_reserved_funds,
                explicit_reserved,
            )

            has_liquid_activity = (
                getattr(b, "daily_mercadopago", None) is not None
                or getattr(b, "daily_cash_withdrawn", None) is not None
                or getattr(b, "real_apps_collected", None) is not None
                or explicit_reserved is not None
                or abs(expense_day) > 1e-9
            )

            if has_liquid_activity:
                liquid_profit = compute_available_liquidity_change(
                    cash_income=daily_liquidity,
                    paid_expenses=expense_day,
                    previous_reserved_funds=running_reserved_funds,
                    current_reserved_funds=current_reserved_funds,
                )

                # La curva violeta debe evolucionar de forma independiente de
                # la caja real: parte de la misma base real mensual, pero luego
                # acumula exclusivamente los movimientos líquidos esperados.
                # Nunca se reinicia con el actual_cash_balance del día anterior.
                liquid_result_running += liquid_profit
                liquid_balance = month_base_real + liquid_result_running

            has_real_data = (
                getattr(b, "real_cash_profit", None) is not None
                or getattr(b, "real_digital_profit", None) is not None
                or getattr(b, "real_apps_collected", None) is not None
            )
            if has_real_data:
                real_profit = (
                    float(getattr(b, "real_cash_profit", 0.0) or 0.0)
                    + float(getattr(b, "real_digital_profit", 0.0) or 0.0)
                    + float(getattr(b, "real_apps_collected", 0.0) or 0.0)
                )

            actual_balance = getattr(b, "actual_cash_balance", None)
            if actual_balance is not None:
                real_balance = float(actual_balance)
                real_result_running = real_balance - month_base_real

            reserve_marker = (
                current_reserved_funds
                if explicit_reserved is not None
                else None
            )

        running_reserved_funds = current_reserved_funds

        # Los días anteriores al filtro reconstruyen los acumulados, pero no
        # se muestran en el eje X.
        if d < d1:
            continue

        has_any_chart_data = (
            has_calc_data
            or has_liquid_activity
            or has_real_data
            or actual_balance is not None
            or reserve_marker is not None
        )

        # Aunque no haya una modificación explícita de la reserva, el primer
        # día visible de cada mes muestra su saldo vigente. Después solo se
        # marcan modificaciones explícitas.
        if first_visible_month_point and reserve_marker is None:
            reserve_marker = current_reserved_funds

        first_visible_month_point = False

        # La tabla inferior conserva únicamente días con información cargada;
        # el gráfico, en cambio, muestra todas las fechas del período.
        if has_any_chart_data:
            cmp_rows.append(
                {
                    "date": d,
                    "date_ar": fmt_date_ar(d),
                    "date_iso": d.isoformat(),
                    "calc": calc,
                    "calc_accum": calc_balance,
                    "liquid_profit": liquid_profit,
                    "liquid_profit_accum": liquid_balance,
                    "real_profit": real_profit,
                    "real_profit_accum": real_balance,
                    "reserved_funds_available": current_reserved_funds,
                    "reserve_changed": explicit_reserved is not None if b is not None else False,
                }
            )

        cmp_dates.append(d.isoformat())
        cmp_labels.append(fmt_date_ar(d))
        cmp_calc.append(None if calc is None else round(calc, 2))
        cmp_liquid_profit.append(
            None if liquid_profit is None else round(float(liquid_profit), 2)
        )
        cmp_real_profit.append(
            None if real_profit is None else round(float(real_profit), 2)
        )
        cmp_calc_balance.append(
            None if calc_balance is None else round(calc_balance, 2)
        )
        cmp_liquid_balance.append(
            None if liquid_balance is None else round(liquid_balance, 2)
        )
        cmp_real_balance.append(
            None if real_balance is None else round(real_balance, 2)
        )
        cmp_reserved_markers.append(
            None if reserve_marker is None else round(float(reserve_marker), 2)
        )

        # Los KPI conservan el último acumulado efectivamente calculable, pero
        # esa persistencia no se dibuja como una línea en días sin datos.
        if calc is not None:
            cmp_calc_result_accum.append(round(calc_result_running, 2))
        if liquid_balance is not None:
            cmp_liquid_result_accum.append(round(liquid_result_running, 2))
        if real_result_running is not None:
            cmp_real_result_accum.append(round(real_result_running, 2))

    # Los KPI de resultado mensual permanecen en cero al inicio del mes.
    latest_calc_result = (
        cmp_calc_result_accum[-1] if cmp_calc_result_accum else 0.0
    )
    latest_liquid_result = (
        cmp_liquid_result_accum[-1] if cmp_liquid_result_accum else 0.0
    )
    latest_real_accum = next(
        (value for value in reversed(cmp_real_result_accum) if value is not None),
        None,
    )

    # Margen líquido: resultado líquido acumulado respecto de los ingresos
    # que efectivamente ingresaron como liquidez en el período.
    margen_liquido = (
        latest_liquid_result / ingresos_liquidos_acumulados * 100.0
        if ingresos_liquidos_acumulados
        else None
    )
    liquid_bucket_label, liquid_bucket_class, liquid_margin_card_style = (
        margin_visual_state(margen_liquido)
    )

    # El fondo reservado es un saldo persistente: se arrastra hasta una
    # modificación explícita, incluso entre meses.
    latest_reserved_funds = _reserved_funds_before(d2 + timedelta(days=1))

    # =========================================================
    # CONCILIACIÓN CALCULADA VS. LÍQUIDA (MES VIGENTE)
    # =========================================================
    reconciliation_month_start = d2.replace(day=1)
    reconciliation_days = (
        BusinessDay.query
        .filter(
            BusinessDay.day >= reconciliation_month_start,
            BusinessDay.day <= d2,
        )
        .order_by(BusinessDay.day.asc())
        .all()
    )

    reconciliation_active_days = []
    reconciliation_apps_gross = 0.0
    reconciliation_apps_collected = 0.0
    reconciliation_liquid_reference = 0.0

    for reconciliation_day in reconciliation_days:
        if is_sunday(reconciliation_day.day):
            continue

        ensure_shifts(reconciliation_day)
        recalc_day_status(reconciliation_day)
        reconciliation_totals = day_totals(reconciliation_day)
        reconciliation_expense = float(
            reconciliation_totals["expense_total"] or 0.0
        )
        reconciliation_has_activity = (
            abs(float(reconciliation_totals["income"] or 0.0)) > 1e-9
            or abs(reconciliation_expense) > 1e-9
            or getattr(reconciliation_day, "real_apps_pending", None) is not None
            or getattr(reconciliation_day, "daily_mercadopago", None) is not None
            or getattr(reconciliation_day, "daily_cash_withdrawn", None) is not None
            or getattr(reconciliation_day, "real_apps_collected", None) is not None
            or getattr(reconciliation_day, "reserved_funds_balance", None) is not None
            or getattr(reconciliation_day, "actual_cash_balance", None) is not None
        )
        if not reconciliation_has_activity:
            continue

        reconciliation_active_days.append(reconciliation_day)
        reconciliation_apps_gross += float(
            getattr(reconciliation_day, "real_apps_pending", 0.0) or 0.0
        )
        reconciliation_apps_collected += float(
            getattr(reconciliation_day, "real_apps_collected", 0.0) or 0.0
        )
        reconciliation_liquid_reference += (
            float(getattr(reconciliation_day, "daily_mercadopago", 0.0) or 0.0)
            + float(getattr(reconciliation_day, "daily_cash_withdrawn", 0.0) or 0.0)
            + float(getattr(reconciliation_day, "real_apps_collected", 0.0) or 0.0)
        )

    previous_operating_day = (
        BusinessDay.query
        .filter(BusinessDay.day < reconciliation_month_start)
        .order_by(BusinessDay.day.desc())
        .first()
    )
    opening_operating_cash = (
        getattr(previous_operating_day, "operating_cash_balance", None)
        if previous_operating_day is not None
        else None
    )
    latest_operating_cash = (
        getattr(reconciliation_active_days[-1], "operating_cash_balance", None)
        if reconciliation_active_days
        else None
    )

    opening_reserved_funds_reconciliation = _reserved_funds_before(
        reconciliation_month_start
    )

    accumulated_reconciliation = compute_calc_liquid_reconciliation(
        calculated_profit=latest_calc_result,
        liquid_profit=latest_liquid_result,
        apps_gross=reconciliation_apps_gross,
        apps_collected=reconciliation_apps_collected,
        previous_operating_cash=opening_operating_cash,
        current_operating_cash=latest_operating_cash,
        previous_reserved_funds=opening_reserved_funds_reconciliation,
        current_reserved_funds=latest_reserved_funds,
    )
    accumulated_reconciliation_status = compute_reconciliation_status(
        accumulated_reconciliation["unexplained_gap"],
        reconciliation_liquid_reference,
    )

    accumulated_reconciliation_style = ""
    if accumulated_reconciliation_status["label"] == "Aceptable":
        accumulated_reconciliation_style = "background:rgba(22,163,74,.10); border-color:rgba(22,163,74,.24);"
    elif accumulated_reconciliation_status["label"] == "Medio":
        accumulated_reconciliation_style = "background:rgba(245,158,11,.12); border-color:rgba(245,158,11,.30);"
    elif accumulated_reconciliation_status["label"] == "Riesgoso":
        accumulated_reconciliation_style = "background:rgba(220,38,38,.10); border-color:rgba(220,38,38,.25);"

    accumulated_unexplained_pct_text = (
        "—"
        if accumulated_reconciliation_status["pct"] is None
        else f'{accumulated_reconciliation_status["pct"]:.1f}%'
    )

    # Torta del período: ingreso efectivo, gasto y variación líquida acumulada
    # del mes, manteniendo la misma lógica del KPI superior.
    liquid_profit_period = float(latest_liquid_result or 0.0)

    pie_labels = [
        "Ingresos líquidos",
        "Gastos",
        "Ganancia líquida" if liquid_profit_period >= 0 else "Pérdida líquida",
    ]
    pie_values = [
        max(float(ingresos_liquidos_acumulados or 0.0), 0.0),
        max(float(expense or 0.0), 0.0),
        abs(liquid_profit_period),
    ]

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

    cmp_payload = {
        "dates": cmp_dates,
        "labels": cmp_labels,
        "calc": cmp_calc,
        "liquid_profit": cmp_liquid_profit,
        "real_profit": cmp_real_profit,
        # Las tres series acumuladas del gráfico son saldos y comparten la
        # misma base real de apertura mensual.
        "calc_accum": cmp_calc_balance,
        "liquid_profit_accum": cmp_liquid_balance,
        "real_profit_accum": cmp_real_balance,
        "reserved_markers": cmp_reserved_markers,
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
        calc_html = _fmt_or_dash(r["calc"])
        calc_accum_html = _fmt_or_dash(r["calc_accum"])
        liquid_profit_html = _fmt_or_dash(r["liquid_profit"])
        liquid_profit_accum_html = _fmt_or_dash(r["liquid_profit_accum"])
        real_profit_html = _fmt_or_dash(r["real_profit"])
        real_profit_accum_html = _fmt_or_dash(r["real_profit_accum"])

        desfasaje_html = _desfasaje_pct(
            r["liquid_profit_accum"],
            r["real_profit_accum"],
        )

        return (
            "<tr>"
            f"<td>{r['date_ar']}</td>"
            f"<td class='num' style='color:#2563eb; font-weight:800;'>{calc_html}</td>"
            f"<td class='num' style='color:#2563eb; font-weight:700;'>{calc_accum_html}</td>"
            f"<td class='num' style='color:#7c3aed; font-weight:800;'>{liquid_profit_html}</td>"
            f"<td class='num' style='color:#7c3aed; font-weight:700;'>{liquid_profit_accum_html}</td>"
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
        <div class="value">{ars(latest_calc_result)}</div>
        <div class="muted">
          Ventas menos gastos y menos {APPS_RETENTION_FACTOR * 100:.1f}% estimado
          sobre ventas de PY + Rappi
        </div>
      </div>

      <div class="card kpi" style="background:rgba(22,163,74,.17); border-color:rgba(22,163,74,.34);">
        <div class="label">Ganancia real acumulada</div>
        <div class="value">{ars(latest_real_accum) if latest_real_accum is not None else "—"}</div>
        <div class="muted">Variación del saldo real disponible desde la apertura del mes</div>
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
            <div class="label">Margen Calculado</div>
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
        <div class="value">{ars(latest_liquid_result)}</div>
        <div class="muted">Variación acumulada de liquidez disponible del mes</div>
      </div>

      <div class="card kpi" style="background:rgba(244,63,94,.11); border-color:rgba(244,63,94,.27);">
        <div class="label">Sueldo Ximena</div>
        <div class="value">{ars(sueldo_ximena)}</div>
        <div class="muted">Gasto fijo en el rango</div>
      </div>

      <!-- Fila 3 -->
      <div class="card kpi">
        <div class="label">Fondos reservados disponibles</div>
        <div class="value">{ars(latest_reserved_funds)}</div>
        <div class="muted">Saldo persistente; cambia solo con una modificación explícita</div>
      </div>

      <div class="card kpi" style="{liquid_margin_card_style}">
        <div class="margen-kpi">
          <div class="margen-left">
            <div class="label">Margen Líquido</div>
            <div class="value">{(f"{margen_liquido:.1f}%" if margen_liquido is not None else "—")}</div>
            <div style="margin-top:6px;"><span class="{liquid_bucket_class}">{liquid_bucket_label}</span></div>
          </div>

          <div class="margen-right">
            <div class="muted">Ref.</div>
            <span class="pill bad">Malo ≤ 10</span>
            <span class="pill warn">Regular &lt; 20</span>
            <span class="pill ok">Bueno ≥ 20</span>
          </div>
        </div>
      </div>

      <div class="card kpi" style="background:rgba(107,114,128,.08); border-color:rgba(107,114,128,.20);">
        <div class="label">Brecha explicada acumulada</div>
        <div class="value">{ars(accumulated_reconciliation["explained_gap"]) if accumulated_reconciliation["explained_gap"] is not None else "—"}</div>
        <div class="muted">
          Apps: {ars(accumulated_reconciliation["apps_effect"])} ·
          Caja: {ars(accumulated_reconciliation["operating_cash_change"]) if accumulated_reconciliation["operating_cash_change"] is not None else "—"} ·
          Reservas: {ars(accumulated_reconciliation["reserve_change"])}
        </div>
      </div>

      <div class="card kpi" style="{accumulated_reconciliation_style}">
        <div class="label">Desfase no explicado acumulado</div>
        <div class="value">{ars(accumulated_reconciliation["unexplained_gap"]) if accumulated_reconciliation["unexplained_gap"] is not None else "—"}</div>
        <div class="muted" style="display:flex; gap:7px; align-items:center; flex-wrap:wrap;">
          <span class="{accumulated_reconciliation_status['class']}">{accumulated_reconciliation_status['label']}</span>
          <span>{accumulated_unexplained_pct_text} de los ingresos líquidos</span>
        </div>
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
        La barra violeta muestra la variación mensual de liquidez disponible e incluye altas o liberaciones de fondos reservados.
        El porcentaje sobre cada grupo representa ganancia líquida / ingresos líquidos.
      </p>
    </div>

    <div class="card" id="profit-control">
      <h3>Bloque 5 · Control: Ganancia Calculada, Líquida y Real</h3>
      <div class="chartbox"><canvas id="profitCompareChart"></canvas></div>
      <p class="muted" style="margin-top:10px;">
        Las tres líneas punteadas usan como base la liquidez real disponible al cierre del mes anterior. Azul = Ganancia Calculada Acumulada. Violeta = Liquidez Acumulada. Verde = Ganancia Real Acumulada. Solo se muestran fechas con datos efectivamente cargados; no se prolongan valores sobre días vacíos. El primer punto gris de cada mes muestra el saldo vigente de fondos reservados; luego solo se marcan sus modificaciones explícitas.
      </p>

      <div style="height:10px;"></div>

      <table>
        <thead>
          <tr>
            <th>Fecha</th>
            <th class="num">Calculada</th>
            <th class="num">Saldo<br>Calc.</th>
            <th class="num">Líquida</th>
            <th class="num">Saldo<br>Líq.</th>
            <th class="num">Real</th>
            <th class="num">Saldo<br>Real</th>
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
                  'rgba(22,163,74,0.22)',
                  'rgba(220,38,38,0.22)',
                  'rgba(124,58,237,0.22)'
                ],
                borderColor: [
                  'rgba(22,163,74,0.55)',
                  'rgba(220,38,38,0.55)',
                  'rgba(124,58,237,0.55)'
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
                label: 'Ganancia Calculada Ajustada (diaria)',
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
                label: 'Liquidez Acumulada',
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
                label: 'Ganancia Real Acumulada',
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
              ,{{
                label: 'Fondos reservados',
                data: profitCmp.reserved_markers,
                showLine: false,
                pointRadius: 5,
                pointHoverRadius: 7,
                spanGaps: false,
                borderColor: '#9ca3af',
                backgroundColor: '#9ca3af',
                pointBackgroundColor: '#9ca3af',
                pointBorderColor: '#6b7280'
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
    calc = float(t.get("profit_adjusted", t["profit"]))
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