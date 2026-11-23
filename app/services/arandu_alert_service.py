"""Advertencias determinísticas del CFO; no contienen decisiones autónomas."""

from datetime import date, timedelta
from statistics import median

from flask import current_app

from app.services.arandu_finance_service import (
    compare_financial_periods,
    get_expense_breakdown,
    get_financial_period_summary,
    get_period_series,
)


def _alert(code, severity, title, detail, evidence):
    return {
        "code": code,
        "severity": severity,
        "title": title,
        "detail": detail,
        "evidence": evidence,
    }


def get_period_alerts(start_date, end_date, *, today=None):
    """Detecta señales revisables; un resultado negativo aislado no es anomalía."""

    today = today or date.today()
    effective_end = min(end_date, today)
    duration = effective_end - start_date
    previous_end = start_date - timedelta(days=1)
    previous_start = previous_end - duration
    summary = get_financial_period_summary(start_date, effective_end, today=today)
    expenses = get_expense_breakdown(
        start_date, effective_end, period_summary=summary
    )
    alerts = []
    sales_drop_threshold = float(
        current_app.config.get("ARANDU_SALES_DROP_ALERT_PCT", 15.0)
    )
    expense_pct_threshold = float(
        current_app.config.get("ARANDU_EXPENSE_INCREASE_ALERT_PCT", 30.0)
    )
    expense_amount_threshold = float(
        current_app.config.get("ARANDU_EXPENSE_INCREASE_ALERT_AMOUNT", 50000.0)
    )

    missing = summary["data_quality"]["missing_dates"]
    if missing:
        alerts.append(
            _alert(
                "missing-days",
                "warning",
                "Período con días sin carga",
                "La ausencia de datos limita las conclusiones; no implica por sí sola una pérdida.",
                {"missing_dates": missing, "count": len(missing)},
            )
        )

    if not expenses["reconciled"]:
        alerts.append(
            _alert(
                "expense-reconciliation",
                "critical",
                "Gastos sin conciliación completa",
                "El detalle por categoría no coincide con el gasto total usado por PORÁ.",
                {"difference": expenses["unclassified_difference"]},
            )
        )

    # Solo se compara si el período anterior está dentro de la base confiable.
    analysis_start = current_app.config.get("ARANDU_ANALYSIS_START_DATE", "2026-03-01")
    if previous_start.isoformat() >= analysis_start:
        comparison = compare_financial_periods(
            start_date,
            effective_end,
            previous_start,
            previous_end,
            today=today,
        )
        sales_change = comparison["changes"]["gross_sales"]
        if (
            sales_change["pct"] is not None
            and sales_change["pct"] <= -sales_drop_threshold
        ):
            alerts.append(
                _alert(
                    "sales-decline",
                    "warning",
                    "Caída relevante de ventas",
                    "Las ventas brutas disminuyeron frente al período anterior equivalente.",
                    sales_change,
                )
            )

        current_expenses = {
            (item["kind"], item["category"]): item
            for item in expenses["items"]
        }
        previous_expenses = get_expense_breakdown(previous_start, previous_end)
        previous_map = {
            (item["kind"], item["category"]): item
            for item in previous_expenses["items"]
        }
        for key, item in current_expenses.items():
            previous_amount = float(previous_map.get(key, {}).get("amount", 0.0))
            increase = float(item["amount"]) - previous_amount
            pct = None if previous_amount <= 0 else increase / previous_amount * 100.0
            if (
                increase >= expense_amount_threshold
                and (pct is None or pct >= expense_pct_threshold)
            ):
                alerts.append(
                    _alert(
                        f"expense-increase:{key[0]}:{key[1]}",
                        "warning",
                        f"Aumento de gasto: {key[1]}",
                        "Requiere explicación operativa; no se presume que sea un error.",
                        {
                            "current": item["amount"],
                            "previous": round(previous_amount, 2),
                            "increase": round(increase, 2),
                            "pct": None if pct is None else round(pct, 2),
                        },
                    )
                )

    daily_expenses = [
        float(item.get("expense_total", 0.0) or 0.0)
        for item in get_period_series(start_date, effective_end)
    ]
    positive = [value for value in daily_expenses if value > 0]
    if len(positive) >= 5:
        baseline = median(positive)
        unusual = [value for value in positive if value >= 50000 and value > baseline * 3]
        if unusual:
            alerts.append(
                _alert(
                    "large-outflow-days",
                    "info",
                    "Días con egresos inusualmente altos",
                    "Puede responder a sueldos, alquileres o impuestos; debe contextualizarse antes de concluir.",
                    {"median": round(baseline, 2), "values": [round(v, 2) for v in unusual]},
                )
            )

    alerts.append(
        _alert(
            "apps-rate-provisional",
            "info",
            "Retención de apps estimada",
            "La ganancia calculada continúa usando el 41,4% provisional para PedidosYa y Rappi.",
            {"retention_pct": 41.4},
        )
    )
    return {
        "period": summary["period"],
        "alerts": alerts,
        "counts": {
            level: sum(1 for item in alerts if item["severity"] == level)
            for level in ("critical", "warning", "info")
        },
    }
