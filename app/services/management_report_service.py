from calendar import monthrange
from datetime import date, timedelta

from app.models import BusinessDay
from app.services.finance_service import (
    compute_adjusted_profit,
    compute_calc_liquid_reconciliation,
    compute_reconciliation_status,
)

MONTH_NAMES = (
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
)


def last_complete_month(today=None):
    today = today or date.today()
    first_this_month = today.replace(day=1)
    end = first_this_month - timedelta(days=1)
    return end.replace(day=1), end


def _month_bounds(year, month):
    return date(year, month, 1), date(year, month, monthrange(year, month)[1])


def _shift_month(year, month, offset):
    serial = year * 12 + (month - 1) + offset
    return serial // 12, serial % 12 + 1


def _is_full_month(start_date, end_date):
    return (
        start_date.day == 1
        and start_date.year == end_date.year
        and start_date.month == end_date.month
        and end_date.day == monthrange(end_date.year, end_date.month)[1]
    )


def _previous_period(start_date, end_date):
    if _is_full_month(start_date, end_date):
        year, month = _shift_month(start_date.year, start_date.month, -1)
        return _month_bounds(year, month)
    duration = (end_date - start_date).days + 1
    previous_end = start_date - timedelta(days=1)
    previous_start = previous_end - timedelta(days=duration - 1)
    return previous_start, previous_end


def _day_totals(bday):
    income = sum(float(s.income or 0.0) for s in bday.shifts)
    if bday.expenses:
        expense = sum(float(e.amount or 0.0) for e in bday.expenses)
    else:
        expense = sum(
            float(s.variable_expense_total or 0.0)
            + float(s.fixed_expense_total or 0.0)
            for s in bday.shifts
        )

    apps_gross = float(getattr(bday, "real_apps_pending", 0.0) or 0.0)
    calculated = compute_adjusted_profit(income, expense, apps_gross)
    liquid_income = (
        float(getattr(bday, "daily_mercadopago", 0.0) or 0.0)
        + float(getattr(bday, "daily_cash_withdrawn", 0.0) or 0.0)
        + float(getattr(bday, "real_apps_collected", 0.0) or 0.0)
    )
    has_liquid = any(
        getattr(bday, field, None) is not None
        for field in (
            "daily_mercadopago",
            "daily_cash_withdrawn",
            "real_apps_collected",
        )
    )
    return income, expense, calculated, liquid_income, has_liquid


def _reserved_at(end_date):
    row = (
        BusinessDay.query
        .filter(BusinessDay.day <= end_date)
        .filter(BusinessDay.reserved_funds_balance.isnot(None))
        .order_by(BusinessDay.day.desc())
        .first()
    )
    return float(row.reserved_funds_balance or 0.0) if row else 0.0


def _percent_change(current, previous):
    if current is None or previous is None or abs(float(previous)) < 1e-9:
        return None
    return (float(current) - float(previous)) / abs(float(previous)) * 100.0


def _period_label(start_date, end_date):
    if _is_full_month(start_date, end_date):
        return f"{MONTH_NAMES[start_date.month - 1]} {start_date.year}"
    return f"{start_date.strftime('%d/%m/%Y')} al {end_date.strftime('%d/%m/%Y')}"


def _aggregate_period(start_date, end_date):
    days = (
        BusinessDay.query
        .filter(BusinessDay.day >= start_date, BusinessDay.day <= end_date)
        .order_by(BusinessDay.day.asc())
        .all()
    )

    gross_income = 0.0
    expenses = 0.0
    calculated_profit = 0.0
    liquid_income = 0.0
    liquid_expenses = 0.0
    liquid_days = 0
    categories = {}
    apps_gross = 0.0
    apps_collected = 0.0

    for bday in days:
        income, expense, calculated, liquid, has_liquid = _day_totals(bday)
        gross_income += income
        expenses += expense
        calculated_profit += calculated
        apps_gross += float(getattr(bday, "real_apps_pending", 0.0) or 0.0)
        apps_collected += float(getattr(bday, "real_apps_collected", 0.0) or 0.0)

        if has_liquid:
            liquid_income += liquid
            liquid_expenses += expense
            liquid_days += 1

        for entry in bday.expenses:
            name = (
                getattr(getattr(entry, "category", None), "name", None)
                or "Sin categoría"
            )
            categories[name] = categories.get(name, 0.0) + float(entry.amount or 0.0)

    liquid_profit = liquid_income - liquid_expenses if liquid_days else None
    calc_margin = (
        calculated_profit / gross_income * 100.0 if gross_income else None
    )
    liquid_margin = (
        liquid_profit / liquid_income * 100.0
        if liquid_profit is not None and liquid_income
        else None
    )

    actual_row = next(
        (
            d for d in reversed(days)
            if getattr(d, "actual_cash_balance", None) is not None
        ),
        None,
    )
    actual_balance = (
        float(actual_row.actual_cash_balance)
        if actual_row is not None
        else None
    )

    return {
        "days": days,
        "days_loaded": len(days),
        "days_closed": sum(1 for d in days if (d.status or "") == "complete"),
        "gross_income": gross_income,
        "expenses": expenses,
        "calculated_profit": calculated_profit,
        "calculated_margin": calc_margin,
        "liquid_income": liquid_income if liquid_days else None,
        "liquid_expenses": liquid_expenses if liquid_days else None,
        "liquid_profit": liquid_profit,
        "liquid_margin": liquid_margin,
        "actual_balance": actual_balance,
        "actual_balance_date": actual_row.day if actual_row else None,
        "reserved_funds": _reserved_at(end_date),
        "categories": categories,
        "apps_gross": apps_gross,
        "apps_collected": apps_collected,
    }


def _monthly_history(end_date, months=4):
    history = []
    for offset in range(-(months - 1), 1):
        year, month = _shift_month(end_date.year, end_date.month, offset)
        start, end = _month_bounds(year, month)
        summary = _aggregate_period(start, end)
        if summary["liquid_income"] is None:
            continue
        history.append({
            "year": year,
            "month": month,
            "label": MONTH_NAMES[month - 1],
            "income": summary["liquid_income"],
            "expense": summary["liquid_expenses"],
            "profit": summary["liquid_profit"],
            "margin": summary["liquid_margin"],
        })
    return history


def _reconciliation(start_date, end_date, current):
    opening_reserved = _reserved_at(start_date - timedelta(days=1))
    current_reserved = current["reserved_funds"]
    reserve_change = current_reserved - opening_reserved

    previous_day = (
        BusinessDay.query
        .filter(BusinessDay.day < start_date)
        .order_by(BusinessDay.day.desc())
        .first()
    )
    opening_operating = (
        getattr(previous_day, "operating_cash_balance", None)
        if previous_day is not None
        else None
    )

    active_days = current["days"]
    current_operating = (
        getattr(active_days[-1], "operating_cash_balance", None)
        if active_days
        else None
    )

    comparable_liquid_profit = (
        None
        if current["liquid_profit"] is None
        else float(current["liquid_profit"]) - float(reserve_change)
    )

    result = compute_calc_liquid_reconciliation(
        calculated_profit=current["calculated_profit"],
        liquid_profit=comparable_liquid_profit,
        apps_gross=current["apps_gross"],
        apps_collected=current["apps_collected"],
        previous_operating_cash=opening_operating,
        current_operating_cash=current_operating,
        previous_reserved_funds=opening_reserved,
        current_reserved_funds=current_reserved,
    )
    status = compute_reconciliation_status(
        result["unexplained_gap"],
        current["liquid_income"],
    )
    result["status"] = status
    result["comparable_liquid_profit"] = comparable_liquid_profit
    return result


def build_management_report(start_date, end_date):
    if start_date > end_date:
        raise ValueError("La fecha Desde no puede ser posterior a Hasta.")

    current = _aggregate_period(start_date, end_date)
    if not current["days"]:
        raise ValueError("No hay días cargados en el período seleccionado.")

    previous_start, previous_end = _previous_period(start_date, end_date)
    previous = _aggregate_period(previous_start, previous_end)

    categories_sorted = sorted(
        current["categories"].items(),
        key=lambda item: item[1],
        reverse=True,
    )
    top_categories = categories_sorted[:5]
    categorized_total = sum(amount for _, amount in categories_sorted)
    category_other = max(float(current["expenses"]) - sum(a for _, a in top_categories), 0.0)
    pie_categories = list(top_categories)
    if category_other > 0.5:
        pie_categories.append(("Otros", category_other))

    comparison_label = (
        _period_label(previous_start, previous_end)
        if previous["days"]
        else "período anterior"
    )

    reconciliation = _reconciliation(start_date, end_date, current)

    days_pending = max(current["days_loaded"] - current["days_closed"], 0)
    closure_pct = (
        current["days_closed"] / current["days_loaded"] * 100.0
        if current["days_loaded"]
        else 0.0
    )

    top_expense = top_categories[0] if top_categories else ("Sin detalle", 0.0)
    top_expense_share = (
        top_expense[1] / current["expenses"] * 100.0
        if current["expenses"]
        else 0.0
    )

    changes = {
        "gross_income_pct": _percent_change(current["gross_income"], previous["gross_income"]),
        "liquid_income_pct": _percent_change(current["liquid_income"], previous["liquid_income"]),
        "expenses_pct": _percent_change(current["expenses"], previous["expenses"]),
        "liquid_profit_pct": _percent_change(current["liquid_profit"], previous["liquid_profit"]),
        "liquid_margin_pp": (
            None
            if current["liquid_margin"] is None or previous["liquid_margin"] is None
            else current["liquid_margin"] - previous["liquid_margin"]
        ),
        "actual_balance_pct": _percent_change(current["actual_balance"], previous["actual_balance"]),
        "reserved_pct": _percent_change(current["reserved_funds"], previous["reserved_funds"]),
    }

    return {
        "start": start_date,
        "end": end_date,
        "period_label": _period_label(start_date, end_date),
        "comparison_label": comparison_label,
        "days_loaded": current["days_loaded"],
        "days_closed": current["days_closed"],
        "days_pending": days_pending,
        "closure_pct": closure_pct,
        "gross_income": current["gross_income"],
        "expenses": current["expenses"],
        "calculated_profit": current["calculated_profit"],
        "calculated_margin": current["calculated_margin"],
        "liquid_income": current["liquid_income"],
        "liquid_expenses": current["liquid_expenses"],
        "liquid_profit": current["liquid_profit"],
        "liquid_margin": current["liquid_margin"],
        "actual_balance": current["actual_balance"],
        "actual_balance_date": current["actual_balance_date"],
        "reserved_funds": current["reserved_funds"],
        "previous": previous,
        "changes": changes,
        "monthly_history": _monthly_history(end_date, months=4),
        "top_categories": top_categories,
        "pie_categories": pie_categories,
        "top_expense_name": top_expense[0],
        "top_expense_amount": top_expense[1],
        "top_expense_share": top_expense_share,
        "reconciliation": reconciliation,
    }
