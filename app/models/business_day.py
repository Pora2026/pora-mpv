from datetime import datetime
from app.extensions import db


class BusinessDay(db.Model):
    __tablename__ = "business_days"

    id = db.Column(db.Integer, primary_key=True)
    day = db.Column(db.Date, unique=True, nullable=False, index=True)
    note = db.Column(db.Text, default="")
    status = db.Column(db.String(20), default="draft")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    # =========================================================
    # LEGACY
    # =========================================================

    real_profit = db.Column(db.Float, nullable=True)

    # =========================================================
    # GANANCIA REAL POR CANAL
    # =========================================================

    real_cash_profit = db.Column(db.Float, nullable=True)
    real_digital_profit = db.Column(db.Float, nullable=True)

    # =========================================================
    # APPS
    # =========================================================

    real_apps_pending = db.Column(db.Float, nullable=True)      # vendido
    real_apps_collected = db.Column(db.Float, nullable=True)    # cobrado

    # =========================================================
    # CAJA / LIQUIDEZ
    # =========================================================

    # saldo inicial del día
    opening_cash_balance = db.Column(db.Float, nullable=True)

    # saldo esperado según sistema
    expected_cash_balance = db.Column(db.Float, nullable=True)

    # arqueo físico real
    actual_cash_balance = db.Column(db.Float, nullable=True)

    # diferencia entre esperado y real
    cash_difference = db.Column(db.Float, nullable=True)

    # transferencia a caja fuerte (legacy)
    # Se conserva para no reinterpretar movimientos históricos.
    safe_box_transfer = db.Column(db.Float, nullable=True)

    # saldo explícito de fondos reservados.
    # NULL = ese día no se modificó el fondo; se hereda el último valor.
    # 0 = el fondo fue liberado/agotado explícitamente.
    reserved_funds_balance = db.Column(db.Float, nullable=True)

    # momento en que se modificó explícitamente reserved_funds_balance
    reserved_funds_changed_at = db.Column(db.DateTime, nullable=True)
    
    # efectivo que queda inmovilizado como fondo de cambio al cierre del día.
    # No forma parte de la liquidez disponible de tesorería; se utiliza para
    # conciliar la ganancia calculada con la liquidez.
    operating_cash_balance = db.Column(db.Float, nullable=True)

    # liquidez diaria consolidada
    daily_mercadopago = db.Column(db.Float, nullable=True)

    # efectivo retirado desde CAJA
    daily_cash_withdrawn = db.Column(db.Float, nullable=True)

    # legacy anterior
    cash_balance = db.Column(db.Float, nullable=True)

    # =========================================================
    # RELACIONES
    # =========================================================

    shifts = db.relationship(
        "ShiftRecord",
        backref="business_day",
        cascade="all, delete-orphan"
    )

    expenses = db.relationship(
        "ExpenseEntry",
        backref="business_day",
        cascade="all, delete-orphan"
    )
