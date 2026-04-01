from datetime import datetime
from app.extensions import db


class ExpenseEntry(db.Model):
    __tablename__ = "expense_entries"

    id = db.Column(db.Integer, primary_key=True)
    business_day_id = db.Column(db.Integer, db.ForeignKey("business_days.id"), nullable=False)
    kind = db.Column(db.String(10), nullable=False)
    category_id = db.Column(db.Integer, db.ForeignKey("expense_categories.id"), nullable=False)
    amount = db.Column(db.Float, default=0.0)
    note = db.Column(db.Text, default="")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    category = db.relationship("ExpenseCategory")