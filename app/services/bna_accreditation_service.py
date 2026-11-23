"""Proyección y conciliación de acreditaciones BNA.

No acredita dinero automáticamente: sólo confirmaciones bancarias verificadas
pueden alimentar la liquidez efectiva.
"""
from datetime import date, timedelta
from math import isfinite


def eighth_banking_day(sale_date: date, holidays=()) -> date:
    """Octavo día hábil bancario posterior a la fecha de venta.

    `holidays` debe contener feriados y días no laborables bancarios de BCRA,
    provistos por un calendario verificado; nunca se asumen automáticamente.
    """
    if not isinstance(sale_date, date):
        raise TypeError("sale_date debe ser date")
    excluded = set(holidays)
    current = sale_date
    count = 0
    while count < 8:
        current += timedelta(days=1)
        if current.weekday() < 5 and current not in excluded:
            count += 1
    return current


def summarize_bna_receivables(sales, confirmed, as_of: date, holidays=()):
    """Resume ventas BNA y cobros comprobados.

    sales: iterables (fecha_venta, monto); confirmed: (fecha_venta,
    monto_confirmado). Las acreditaciones parciales se admiten. Los importes
    confirmados se asignan a su venta, no a un total global indistinto.
    """
    by_sale = {}
    for sale_date, amount in sales:
        value = float(amount)
        if not isfinite(value) or value < 0:
            raise ValueError("Venta BNA inválida")
        by_sale[sale_date] = by_sale.get(sale_date, 0.0) + value
    by_confirmed = {}
    for sale_date, amount in confirmed:
        value = float(amount)
        if not isfinite(value) or value < 0:
            raise ValueError("Acreditación BNA inválida")
        by_confirmed[sale_date] = by_confirmed.get(sale_date, 0.0) + value
    if set(by_confirmed) - set(by_sale):
        raise ValueError("Hay acreditaciones sin venta de origen")
    pending = overdue = not_due = 0.0
    for sale_date, gross in by_sale.items():
        collected = by_confirmed.get(sale_date, 0.0)
        if collected > gross + 0.005:
            raise ValueError("Acreditación superior a venta BNA")
        remainder = max(0.0, gross - collected)
        pending += remainder
        if eighth_banking_day(sale_date, holidays) < as_of:
            overdue += remainder
        else:
            not_due += remainder
    return {"pending": round(pending, 2), "overdue": round(overdue, 2),
            "not_due": round(not_due, 2)}
