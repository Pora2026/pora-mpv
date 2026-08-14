# app/services/finance_service.py


# =========================================================
# ESTIMACIÓN DE COSTOS DE APPS
# =========================================================

# Estimación provisional basada en la liquidación analizada:
# $582.237,50 brutos -> $341.039,27 netos.
# Se reemplazará más adelante por un promedio ponderado de varias liquidaciones.
APPS_RETENTION_FACTOR = 0.414
APPS_NET_FACTOR = 1.0 - APPS_RETENTION_FACTOR


def compute_apps_retention_estimate(apps_gross, retention_factor=APPS_RETENTION_FACTOR):
    """Costo estimado de comisiones, impuestos y cargos de PY + Rappi."""
    return float(apps_gross or 0.0) * float(retention_factor)


def compute_apps_net_estimate(apps_gross, retention_factor=APPS_RETENTION_FACTOR):
    """Ingreso neto estimado que dejan las ventas brutas realizadas por apps."""
    gross = float(apps_gross or 0.0)
    return gross - compute_apps_retention_estimate(gross, retention_factor)


def compute_adjusted_profit(
    total_sales,
    paid_expenses,
    apps_gross,
    retention_factor=APPS_RETENTION_FACTOR,
):
    """
    Ganancia calculada ajustada.

    ventas brutas
    - gastos cargados
    - retención estimada sobre ventas brutas de PY + Rappi
    """
    return (
        float(total_sales or 0.0)
        - float(paid_expenses or 0.0)
        - compute_apps_retention_estimate(apps_gross, retention_factor)
    )



# =========================================================
# GANANCIA REAL
# =========================================================

def compute_real_total(cash, digital, apps_collected):
    return (
        float(cash or 0.0)
        + float(digital or 0.0)
        + float(apps_collected or 0.0)
    )


def compute_pending_net(apps, apps_collected):
    """
    Parte pendiente usada para explicar el desfase diario.

    ``apps`` contiene ventas brutas de PY + Rappi, por lo que se convierten a
    neto estimado. ``apps_collected`` ya es dinero efectivamente acreditado.
    """
    pending_net = 0.0

    if apps is not None:
        pending_net += compute_apps_net_estimate(apps)

    if apps_collected is not None:
        pending_net -= float(apps_collected)

    return pending_net


def compute_explained_total(cash, digital, apps, apps_collected):
    total = compute_real_total(cash, digital, apps_collected)
    pending_net = compute_pending_net(apps, apps_collected)

    if total is not None or pending_net != 0:
        return total + pending_net

    return None


# =========================================================
# LIQUIDEZ
# =========================================================


def resolve_reserved_funds_balance(previous_balance, explicit_balance):
    """
    Devuelve el saldo de fondos reservados vigente.

    ``explicit_balance`` tiene semántica de estado:
      - None: no hubo modificación; se conserva el saldo anterior.
      - 0: el fondo fue liberado/agotado explícitamente.
      - > 0: nuevo saldo reservado vigente.
    """

    previous = max(float(previous_balance or 0.0), 0.0)
    if explicit_balance is None:
        return previous
    return max(float(explicit_balance or 0.0), 0.0)


def compute_reserved_funds_change(previous_balance, current_balance):
    """Variación del saldo reservado: positiva al reservar, negativa al liberar."""

    return (
        max(float(current_balance or 0.0), 0.0)
        - max(float(previous_balance or 0.0), 0.0)
    )


def compute_operating_cash_change(previous_balance, current_balance):
    """
    Variación de la caja operativa (fondo de cambio).

    La caja operativa existe, pero no se considera liquidez disponible para
    tesorería. Su variación sirve únicamente para conciliar la ganancia
    calculada con la liquidez. Si falta alguno de los dos saldos, no se puede
    determinar la variación diaria con seguridad y se devuelve ``None``.
    """

    if previous_balance is None or current_balance is None:
        return None

    return float(current_balance) - float(previous_balance)


def compute_calc_liquid_reconciliation(
    calculated_profit,
    liquid_profit,
    apps_gross,
    apps_collected,
    previous_operating_cash,
    current_operating_cash,
    previous_reserved_funds=0.0,
    current_reserved_funds=0.0,
):
    """
    Concilia la brecha entre ganancia calculada y ganancia líquida.

    Brecha total:
        calculada - líquida

    Componentes explicativos:
        efecto Apps
        + variación de caja operativa
        + variación de fondos reservados

    El efecto Apps es ventas brutas de Apps convertidas a neto estimado menos
    los cobros efectivos de Apps del día. Esto permite que una acreditación de
    ventas de días anteriores reduzca naturalmente la brecha.

    La caja operativa se considera dinero existente pero inmovilizado para el
    manejo cotidiano del local. No se suma a la liquidez disponible.
    """

    operating_cash_change = compute_operating_cash_change(
        previous_operating_cash,
        current_operating_cash,
    )
    reserve_change = compute_reserved_funds_change(
        previous_reserved_funds,
        current_reserved_funds,
    )
    apps_effect = compute_pending_net(apps_gross, apps_collected)

    complete = (
        calculated_profit is not None
        and liquid_profit is not None
        and operating_cash_change is not None
    )

    gap = None
    explained_gap = None
    unexplained_gap = None

    if calculated_profit is not None and liquid_profit is not None:
        gap = float(calculated_profit) - float(liquid_profit)

    if complete:
        explained_gap = (
            float(apps_effect)
            + float(operating_cash_change)
            + float(reserve_change)
        )
        unexplained_gap = float(gap) - explained_gap

    return {
        "complete": complete,
        "gap": None if gap is None else round(gap, 2),
        "apps_effect": round(float(apps_effect), 2),
        "operating_cash_change": (
            None
            if operating_cash_change is None
            else round(float(operating_cash_change), 2)
        ),
        "reserve_change": round(float(reserve_change), 2),
        "explained_gap": (
            None if explained_gap is None else round(explained_gap, 2)
        ),
        "unexplained_gap": (
            None if unexplained_gap is None else round(unexplained_gap, 2)
        ),
    }


def compute_reconciliation_status(
    unexplained_gap,
    reference_amount,
    acceptable_pct=1.0,
    medium_pct=3.0,
):
    """
    Clasifica el desfase no explicado respecto de una base de liquidez.

    Umbrales iniciales de control gerencial:
      - Aceptable: <= 1%
      - Medio: > 1% y <= 3%
      - Riesgoso: > 3%

    Los umbrales quedan parametrizados para poder calibrarlos luego con datos
    históricos reales sin cambiar la lógica de conciliación.
    """

    if unexplained_gap is None:
        return {
            "label": "Incompleto",
            "class": "pill",
            "pct": None,
        }

    reference = abs(float(reference_amount or 0.0))
    if reference < 1e-9:
        return {
            "label": "Sin base",
            "class": "pill",
            "pct": None,
        }

    pct = abs(float(unexplained_gap)) / reference * 100.0

    if pct <= float(acceptable_pct):
        label, css_class = "Aceptable", "pill ok"
    elif pct <= float(medium_pct):
        label, css_class = "Medio", "pill warn"
    else:
        label, css_class = "Riesgoso", "pill bad"

    return {
        "label": label,
        "class": css_class,
        "pct": round(pct, 2),
    }


def compute_available_liquidity_change(
    cash_income,
    paid_expenses,
    previous_reserved_funds=0.0,
    current_reserved_funds=0.0,
):
    """
    Variación diaria de la liquidez disponible.

    Entradas efectivas
    - gastos pagados
    - aumento de fondos reservados
    + liberación de fondos reservados

    La reserva nunca modifica la ganancia económica: únicamente reclasifica
    dinero entre disponible y reservado.
    """

    reserve_change = compute_reserved_funds_change(
        previous_reserved_funds,
        current_reserved_funds,
    )

    return (
        float(cash_income or 0.0)
        - float(paid_expenses or 0.0)
        - reserve_change
    )


def compute_expected_cash_balance(
    opening_balance,
    cash_income,
    paid_expenses,
    reserved_funds_change=0.0,
    safe_box_transfer=0.0,
):
    """
    Calcula la liquidez disponible esperada al cierre del día.

    Fórmula:
    saldo disponible inicial
    + ingresos efectivos
    - egresos pagados
    - aumento del fondo reservado
    + liberación del fondo reservado

    ``safe_box_transfer`` se conserva únicamente por compatibilidad con código
    y datos legacy. La nueva lógica utiliza ``reserved_funds_change``.
    """

    _ = safe_box_transfer

    return (
        float(opening_balance or 0.0)
        + float(cash_income or 0.0)
        - float(paid_expenses or 0.0)
        - float(reserved_funds_change or 0.0)
    )


def compute_reserved_balance_series(opening_reserved_funds, movements):
    """
    Construye el saldo persistente de fondos reservados.

    Cada movimiento acepta ``reserved_funds_balance``:
      - None: arrastra el valor anterior.
      - 0 o positivo: establece explícitamente el nuevo saldo.

    Devuelve un estado por día con el saldo previo, saldo vigente, variación y
    un indicador ``changed`` que permite dibujar solo las modificaciones.
    """

    current = max(float(opening_reserved_funds or 0.0), 0.0)
    states = []

    for movement in movements:
        movement = movement or {}
        explicit = movement.get("reserved_funds_balance")
        previous = current
        current = resolve_reserved_funds_balance(previous, explicit)
        change = compute_reserved_funds_change(previous, current)

        states.append(
            {
                "previous_reserved_funds": round(previous, 2),
                "reserve_available": round(current, 2),
                "reserve_change": round(change, 2),
                "changed": explicit is not None,
            }
        )

    return states


def compute_cash_difference(expected_balance, actual_balance):
    """Diferencia entre liquidez disponible esperada y real cargada."""

    return (
        float(actual_balance or 0.0)
        - float(expected_balance or 0.0)
    )


# -------------------------------------------------------------------------
# Helpers legacy conservados para no romper consumidores anteriores.
# La lógica nueva de reservas usa compute_reserved_balance_series().
# -------------------------------------------------------------------------


def compute_comparable_liquid_balance(expected_balance, reserved_funds_available):
    if expected_balance is None:
        return None
    return (
        float(expected_balance or 0.0)
        - max(float(reserved_funds_available or 0.0), 0.0)
    )


def compute_real_month_accum(actual_balance, opening_reserved_funds):
    if actual_balance is None:
        return None
    return (
        float(actual_balance or 0.0)
        - float(opening_reserved_funds or 0.0)
    )


def compute_reserved_funds_series(opening_reserved_funds, movements):
    """
    Compatibilidad con la API anterior.

    No se utiliza en la lógica vigente. Se mantiene para evitar romper código
    externo que todavía pueda importarla.
    """

    reserve_available = max(float(opening_reserved_funds or 0.0), 0.0)
    real_month_available = 0.0
    previous_total = reserve_available
    states = []

    for movement in movements:
        movement = movement or {}
        actual_balance = movement.get("actual_balance")
        reserve_addition = max(float(movement.get("reserve_addition") or 0.0), 0.0)
        net_liquidity = float(movement.get("net_liquidity") or 0.0)

        if actual_balance is not None:
            current_total = max(float(actual_balance or 0.0), 0.0)
            total_change = current_total - previous_total
            source = "actual"
        else:
            total_change = net_liquidity
            current_total = max(previous_total + total_change, 0.0)
            source = "estimated"

        reserve_used = 0.0
        real_month_used = 0.0
        uncovered_deficit = 0.0

        if total_change >= 0.0:
            real_month_available += total_change
        else:
            outflow = -total_change
            reserve_used = min(reserve_available, outflow)
            reserve_available -= reserve_used
            outflow -= reserve_used

            real_month_used = min(real_month_available, outflow)
            real_month_available -= real_month_used
            outflow -= real_month_used
            uncovered_deficit = max(outflow, 0.0)

        reserve_addition_applied = min(reserve_addition, real_month_available)
        if reserve_addition_applied:
            real_month_available -= reserve_addition_applied
            reserve_available += reserve_addition_applied

        if actual_balance is not None:
            reserve_available = min(reserve_available, current_total)
            real_month_available = max(current_total - reserve_available, 0.0)

        previous_total = current_total
        states.append(
            {
                "reserve_available": round(reserve_available, 2),
                "real_month_available": round(real_month_available, 2),
                "operating_available": round(real_month_available, 2),
                "reserve_used": round(reserve_used, 2),
                "real_month_used": round(real_month_used, 2),
                "reserve_addition_applied": round(reserve_addition_applied, 2),
                "uncovered_deficit": round(uncovered_deficit, 2),
                "total_allocated": round(reserve_available + real_month_available, 2),
                "source": source,
            }
        )

    return states


# =========================================================
# RENTABILIDAD OPERATIVA
# =========================================================

def compute_operational_profit(
    total_sales,
    variable_expenses,
    fixed_expenses,
    fixed_days=30,
):
    """
    Rentabilidad operativa diaria.

    ventas
    - variables
    - fijos prorrateados
    """

    fixed_daily = float(fixed_expenses or 0.0) / float(fixed_days or 30)

    return (
        float(total_sales or 0.0)
        - float(variable_expenses or 0.0)
        - fixed_daily
    )
