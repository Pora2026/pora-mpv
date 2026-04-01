from app.extensions import db


class ShiftRecord(db.Model):
    __tablename__ = "shift_records"

    id = db.Column(db.Integer, primary_key=True)
    business_day_id = db.Column(db.Integer, db.ForeignKey("business_days.id"), nullable=False)

    shift = db.Column(db.String(10), nullable=False)
    income = db.Column(db.Float, default=0.0)

    variable_expense_total = db.Column(db.Float, default=0.0)
    fixed_expense_total = db.Column(db.Float, default=0.0)

    note = db.Column(db.Text, default="")
    is_closed = db.Column(db.Boolean, default=False)

    __table_args__ = (db.UniqueConstraint("business_day_id", "shift", name="uq_day_shift"),)