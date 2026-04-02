from datetime import datetime
from app.extensions import db


class BusinessDay(db.Model):
    __tablename__ = "business_days"

    id = db.Column(db.Integer, primary_key=True)
    day = db.Column(db.Date, unique=True, nullable=False, index=True)
    note = db.Column(db.Text, default="")
    status = db.Column(db.String(20), default="draft")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    # Legacy
    real_profit = db.Column(db.Float, nullable=True)

    # Nuevo esquema: separar ganancia real por canal
    real_cash_profit = db.Column(db.Float, nullable=True)
    real_digital_profit = db.Column(db.Float, nullable=True)

    shifts = db.relationship("ShiftRecord", backref="business_day", cascade="all, delete-orphan")
    expenses = db.relationship("ExpenseEntry", backref="business_day", cascade="all, delete-orphan")
