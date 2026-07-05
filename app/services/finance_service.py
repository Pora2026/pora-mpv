# app/services/finance_service.py


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
    pending_net = 0.0

    if apps is not None:
        pending_net += float(apps)

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
    Calcula cuánto dinero debería haber físicamente.

    Fórmula:
    saldo inicial
    + ingresos efectivos
    - egresos pagados
    - transferencia a caja fuerte
    """

    return (
        float(opening_balance or 0.0)
        + float(cash_income or 0.0)
        - float(paid_expenses or 0.0)
        - float(safe_box_transfer or 0.0)
    )


def compute_cash_difference(expected_balance, actual_balance):
    """
    Diferencia entre caja esperada y arqueo real.
    """

    return (
        float(actual_balance or 0.0)
        - float(expected_balance or 0.0)
    )


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