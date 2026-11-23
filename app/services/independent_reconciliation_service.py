"""Conciliación independiente de tesorería (no reemplaza la brecha económica).

Todos los importes corresponden al MISMO perímetro de cuentas y período.
Los ajustes explicativos son movimientos verificados aún NO incluidos en
los flujos registrados. Una vez registrados, deben retirarse de los ajustes.
"""
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP


def _money(value, name):
    if value is None or isinstance(value, bool):
        raise ValueError(f"{name} es obligatorio")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"{name} debe ser numérico") from None
    if not number.is_finite():
        raise ValueError(f"{name} debe ser finito")
    return number.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


def reconcile_treasury(opening_actual, inflows, outflows, closing_actual,
                       verified_adjustments=(), external_net_transfers=0):
    """Signo: real - esperado. Ajustes verificados con el mismo signo.

    inflows/outflows: cobros/pagos efectivos, NO ventas ni cobros proyectados.
    external_net_transfers: entradas netas desde fuera del perímetro conciliado.
    No incluir transferencias internas entre cuentas del mismo perímetro.
    """
    opening = _money(opening_actual, 'opening_actual')
    income = _money(inflows, 'inflows')
    expense = _money(outflows, 'outflows')
    actual = _money(closing_actual, 'closing_actual')
    transfers = _money(external_net_transfers, 'external_net_transfers')
    if income < 0 or expense < 0:
        raise ValueError('inflows y outflows deben ser no negativos')
    expected = opening + income - expense + transfers
    difference = actual - expected
    adjustments = [_money(item, 'verified_adjustment') for item in verified_adjustments]
    explained = sum(adjustments, Decimal('0.00'))
    unexplained = difference - explained
    return {
        'expected_balance': float(expected),
        'actual_balance': float(actual),
        'financial_difference': float(difference),
        'explained_financial_difference': float(explained),
        'unexplained_financial_difference': float(unexplained),
    }


def reconcile_calc_liquid(calculated_profit, liquid_profit, apps_effect,
                          bna_sales, bna_accredited, operating_cash_change,
                          reserve_change):
    """Descomposición económica; NO representa conciliación independiente."""
    values = [_money(v, n) for n, v in (
        ('calculated_profit', calculated_profit), ('liquid_profit', liquid_profit),
        ('apps_effect', apps_effect), ('bna_sales', bna_sales),
        ('bna_accredited', bna_accredited),
        ('operating_cash_change', operating_cash_change),
        ('reserve_change', reserve_change))]
    calc, liquid, apps, sales, credited, cash, reserves = values
    gap = calc - liquid
    bna_effect = sales - credited
    explained = apps + bna_effect + cash + reserves
    return {'gap': float(gap), 'apps_effect': float(apps),
            'bna_effect': float(bna_effect),
            'operating_cash_change': float(cash), 'reserve_change': float(reserves),
            'explained_gap': float(explained),
            'calc_liquid_residual': float(gap - explained)}
