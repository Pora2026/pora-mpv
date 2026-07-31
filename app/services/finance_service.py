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

def compute_expected_cash_balance(
    opening_balance,
    cash_income,
    paid_expenses,
    safe_box_transfer=0.0,
):
    """
    Calcula cuánto dinero total debería existir al cierre del día.

    Fórmula:
    saldo inicial
    + ingresos efectivos
    - egresos pagados

    ``safe_box_transfer`` se conserva en la firma por compatibilidad, pero no
    se descuenta: separar dinero como fondo reservado es una reclasificación
    interna y no reduce la liquidez real total del negocio.
    """

    _ = safe_box_transfer

    return (
        float(opening_balance or 0.0)
        + float(cash_income or 0.0)
        - float(paid_expenses or 0.0)
    )



def compute_comparable_liquid_balance(expected_balance, reserved_funds_available):
    """
    Liquidez esperada comparable con la liquidez real atribuible al mes.

    Se parte del saldo total esperado al cierre y se descuenta únicamente la
    porción de fondos reservados que todavía permanece disponible. Cuando la
    reserva se agotó, el valor coincide con el saldo esperado total.
    """

    if expected_balance is None:
        return None

    return (
        float(expected_balance or 0.0)
        - max(float(reserved_funds_available or 0.0), 0.0)
    )

def compute_cash_difference(expected_balance, actual_balance):
    """Diferencia entre saldo esperado y liquidez real total cargada."""

    return (
        float(actual_balance or 0.0)
        - float(expected_balance or 0.0)
    )


def compute_real_month_accum(actual_balance, opening_reserved_funds):
    """
    Helper legado mantenido por compatibilidad.

    No debe usarse para la curva mensual nueva, porque restar siempre el fondo
    inicial completo genera valores negativos incluso después de que la reserva
    ya fue consumida. La lógica vigente está en
    ``compute_reserved_funds_series`` y usa ``real_month_available``.
    """

    if actual_balance is None:
        return None

    return (
        float(actual_balance or 0.0)
        - float(opening_reserved_funds or 0.0)
    )


def compute_reserved_funds_series(opening_reserved_funds, movements):
    """
    Distribuye la liquidez real total entre dos componentes:

      - fondos reservados disponibles;
      - liquidez real atribuible al mes.

    Cada movimiento puede contener:
      - ``actual_balance``: liquidez real total contada al cierre;
      - ``reserve_addition``: monto que se decide separar manualmente;
      - ``net_liquidity``: respaldo estimado para días sin arqueo real.

    Política aplicada:
      1. El mes comienza con los fondos anteriores como reserva y con $0
         atribuible al mes.
      2. Una suba del saldo real se asigna a la liquidez del mes.
      3. Una baja del saldo real consume primero la reserva y luego la liquidez
         atribuible al mes.
      4. Una reserva consumida no se repone automáticamente.
      5. La reserva solo aumenta mediante una adición manual.
      6. Reserva + liquidez del mes siempre coincide con el saldo real contado.

    Devuelve un estado por movimiento. El campo principal para la curva verde
    es ``real_month_available``.
    """

    reserve_available = max(float(opening_reserved_funds or 0.0), 0.0)
    real_month_available = 0.0

    # Antes del primer cierre se asume que el total existente coincide con el
    # fondo proveniente del mes anterior.
    previous_total = reserve_available
    states = []

    for movement in movements:
        movement = movement or {}

        actual_balance = movement.get("actual_balance")
        reserve_addition = max(
            float(movement.get("reserve_addition") or 0.0),
            0.0,
        )
        net_liquidity = float(movement.get("net_liquidity") or 0.0)

        if actual_balance is not None:
            current_total = max(float(actual_balance or 0.0), 0.0)
            total_change = current_total - previous_total
            source = "actual"
        else:
            # Fallback conservador para un día sin arqueo real.
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

            # Según el criterio acordado, los egresos consumen primero los
            # fondos reservados. Una vez agotados, reducen la liquidez del mes.
            reserve_used = min(reserve_available, outflow)
            reserve_available -= reserve_used
            outflow -= reserve_used

            real_month_used = min(real_month_available, outflow)
            real_month_available -= real_month_used
            outflow -= real_month_used

            uncovered_deficit = max(outflow, 0.0)

        # Separar fondos no modifica el total: solo reclasifica parte de la
        # liquidez del mes como reserva. Nunca se puede reservar más de lo que
        # actualmente pertenece al mes.
        reserve_addition_applied = min(
            reserve_addition,
            real_month_available,
        )
        if reserve_addition_applied:
            real_month_available -= reserve_addition_applied
            reserve_available += reserve_addition_applied

        if actual_balance is not None:
            # Reconciliación exacta con el arqueo real. Evita desajustes por
            # redondeos y garantiza que ambas partes sumen el total contado.
            reserve_available = min(reserve_available, current_total)
            real_month_available = max(
                current_total - reserve_available,
                0.0,
            )

        previous_total = current_total

        states.append(
            {
                "reserve_available": round(reserve_available, 2),
                "real_month_available": round(real_month_available, 2),
                # Alias conservado para no romper consumidores anteriores.
                "operating_available": round(real_month_available, 2),
                "reserve_used": round(reserve_used, 2),
                "real_month_used": round(real_month_used, 2),
                "reserve_addition_applied": round(
                    reserve_addition_applied,
                    2,
                ),
                "uncovered_deficit": round(uncovered_deficit, 2),
                "total_allocated": round(
                    reserve_available + real_month_available,
                    2,
                ),
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
