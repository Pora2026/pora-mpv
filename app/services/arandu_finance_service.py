"""Consultas financieras determinísticas y de solo lectura para Arandú."""

from datetime import date

from sqlalchemy import func

from app.extensions import db
from app.models import BusinessDay, ExpenseCategory, ExpenseEntry
from app.utils.dates import is_sunday, iter_workdays


def get_period_series(start_date, end_date):
    # Se reutiliza la misma fuente que alimenta el dashboard. Esta importación
    # diferida evita un ciclo durante el arranque de Flask.
    from app_owners import range_series

    return range_series(start_date, end_date)


def get_financial_period_summary(start_date, end_date, *, today=None):
    """Devuelve hechos financieros verificables, sin interpretación de IA.

    La ganancia real acumulada es un saldo y puede contener períodos previos;
    por eso se presenta separada de los resultados calculado y líquido del
    período solicitado.
    """

    today = today or date.today()
    series = get_period_series(start_date, end_date)
    days = (
        BusinessDay.query
        .filter(BusinessDay.day >= start_date, BusinessDay.day <= end_date)
        .order_by(BusinessDay.day.asc())
        .all()
    )
    working_days = [item for item in days if not is_sunday(item.day)]

    gross_sales = sum(float(item["income"] or 0.0) for item in series)
    paid_expenses = sum(float(item["expense_total"] or 0.0) for item in series)
    apps_gross = sum(float(item.get("apps_gross", 0.0) or 0.0) for item in series)
    apps_retention = sum(
        float(item.get("apps_retention_estimate", 0.0) or 0.0)
        for item in series
    )
    calculated_profit = sum(
        float(item.get("profit_adjusted", item["profit"]) or 0.0)
        for item in series
    )

    liquid_income = sum(
        float(getattr(item, "daily_cash_withdrawn", 0.0) or 0.0)
        + float(getattr(item, "daily_mercadopago", 0.0) or 0.0)
        + float(getattr(item, "real_apps_collected", 0.0) or 0.0)
        for item in working_days
    )
    liquid_profit = liquid_income - paid_expenses

    latest_real_day = next(
        (
            item
            for item in reversed(working_days)
            if getattr(item, "actual_cash_balance", None) is not None
        ),
        None,
    )
    latest_reserved_day = (
        BusinessDay.query
        .filter(BusinessDay.day <= end_date)
        .filter(BusinessDay.reserved_funds_balance.isnot(None))
        .order_by(BusinessDay.day.desc())
        .first()
    )

    completeness_end = min(end_date, today)
    expected_days = (
        list(iter_workdays(start_date, completeness_end))
        if start_date <= completeness_end
        else []
    )
    loaded_dates = {item.day for item in working_days}
    missing_dates = [item.isoformat() for item in expected_days if item not in loaded_dates]

    return {
        "period": {"from": start_date.isoformat(), "to": end_date.isoformat()},
        "calculated": {
            "gross_sales": round(gross_sales, 2),
            "paid_expenses": round(paid_expenses, 2),
            "apps_gross": round(apps_gross, 2),
            "apps_retention_estimate": round(apps_retention, 2),
            "profit": round(calculated_profit, 2),
            "includes_previous_balance": False,
        },
        "liquid": {
            "collected_income": round(liquid_income, 2),
            "paid_expenses": round(paid_expenses, 2),
            "profit": round(liquid_profit, 2),
            "includes_previous_balance": False,
            "reserved_funds_deducted": False,
        },
        "real_accumulated": {
            "balance": (
                None
                if latest_real_day is None
                else round(float(latest_real_day.actual_cash_balance), 2)
            ),
            "as_of": None if latest_real_day is None else latest_real_day.day.isoformat(),
            "may_include_previous_periods": True,
        },
        "reserved_funds": {
            "balance": (
                0.0
                if latest_reserved_day is None
                else round(float(latest_reserved_day.reserved_funds_balance or 0.0), 2)
            ),
            "as_of": (
                None if latest_reserved_day is None else latest_reserved_day.day.isoformat()
            ),
        },
        "data_quality": {
            "loaded_working_days": len(working_days),
            "expected_working_days_to_date": len(expected_days),
            "missing_dates": missing_dates,
            "last_loaded_day": (
                None if not working_days else working_days[-1].day.isoformat()
            ),
        },
    }


def _change(current, previous):
    current = float(current or 0.0)
    previous = float(previous or 0.0)
    return {
        "amount": round(current - previous, 2),
        "pct": (
            None
            if abs(previous) < 1e-9
            else round((current - previous) / abs(previous) * 100.0, 2)
        ),
    }


def compare_financial_periods(
    current_start,
    current_end,
    comparison_start,
    comparison_end,
    *,
    today=None,
):
    """Compara dos períodos sin inferencias ni proyecciones."""

    current = get_financial_period_summary(current_start, current_end, today=today)
    comparison = get_financial_period_summary(
        comparison_start, comparison_end, today=today
    )
    return {
        "current": current,
        "comparison": comparison,
        "changes": {
            "gross_sales": _change(
                current["calculated"]["gross_sales"],
                comparison["calculated"]["gross_sales"],
            ),
            "paid_expenses": _change(
                current["calculated"]["paid_expenses"],
                comparison["calculated"]["paid_expenses"],
            ),
            "calculated_profit": _change(
                current["calculated"]["profit"],
                comparison["calculated"]["profit"],
            ),
            "liquid_profit": _change(
                current["liquid"]["profit"],
                comparison["liquid"]["profit"],
            ),
        },
        "warning": (
            None
            if (current_end - current_start).days
            == (comparison_end - comparison_start).days
            else "Los períodos tienen distinta cantidad de días calendario."
        ),
    }


def get_expense_breakdown(start_date, end_date, *, period_summary=None):
    """Agrupa gastos pagados por tipo y categoría y concilia con el período."""

    rows = (
        db.session.query(
            ExpenseEntry.kind.label("kind"),
            ExpenseCategory.name.label("category"),
            func.coalesce(func.sum(ExpenseEntry.amount), 0.0).label("amount"),
            func.count(ExpenseEntry.id).label("entries"),
        )
        .join(ExpenseCategory, ExpenseCategory.id == ExpenseEntry.category_id)
        .join(BusinessDay, BusinessDay.id == ExpenseEntry.business_day_id)
        .filter(BusinessDay.day >= start_date, BusinessDay.day <= end_date)
        .filter(ExpenseEntry.kind.in_(("fixed", "variable")))
        .group_by(ExpenseEntry.kind, ExpenseCategory.name)
        .order_by(func.sum(ExpenseEntry.amount).desc(), ExpenseCategory.name.asc())
        .all()
    )

    items = [
        {
            "kind": row.kind,
            "category": row.category,
            "amount": round(float(row.amount or 0.0), 2),
            "entries": int(row.entries or 0),
        }
        for row in rows
    ]
    categorized_total = round(sum(item["amount"] for item in items), 2)
    summary = period_summary or get_financial_period_summary(start_date, end_date)
    period_total = round(float(summary["calculated"]["paid_expenses"] or 0.0), 2)
    unclassified = round(period_total - categorized_total, 2)

    return {
        "period": {"from": start_date.isoformat(), "to": end_date.isoformat()},
        "accounting_basis": "paid",
        "items": items,
        "categorized_total": categorized_total,
        "period_expense_total": period_total,
        "unclassified_difference": unclassified,
        "reconciled": abs(unclassified) < 0.01,
    }


def get_daily_financials(start_date, end_date):
    """Serie diaria verificable para tendencias, rankings y explicaciones."""

    series = get_period_series(start_date, end_date)
    days = {
        item.day: item
        for item in BusinessDay.query
        .filter(BusinessDay.day >= start_date, BusinessDay.day <= end_date)
        .all()
    }
    items = []
    for row in series:
        day_value = date.fromisoformat(row["date"])
        business_day = days.get(day_value)
        collected = 0.0
        real_balance = None
        if business_day is not None:
            collected = (
                float(business_day.daily_cash_withdrawn or 0.0)
                + float(business_day.daily_mercadopago or 0.0)
                + float(business_day.real_apps_collected or 0.0)
            )
            if business_day.actual_cash_balance is not None:
                real_balance = float(business_day.actual_cash_balance)
        items.append({
            "date": row["date"],
            "gross_sales": round(float(row["income"] or 0.0), 2),
            "paid_expenses": round(float(row["expense_total"] or 0.0), 2),
            "apps_gross": round(float(row.get("apps_gross", 0.0) or 0.0), 2),
            "calculated_profit": round(float(row.get("profit_adjusted", row["profit"]) or 0.0), 2),
            "collected_income": round(collected, 2),
            "liquid_profit": round(collected - float(row["expense_total"] or 0.0), 2),
            "real_accumulated_balance": (
                None if real_balance is None else round(real_balance, 2)
            ),
        })
    return {
        "period": {"from": start_date.isoformat(), "to": end_date.isoformat()},
        "items": items,
        "note": "Un resultado diario negativo no es por sí solo una anomalía.",
    }
