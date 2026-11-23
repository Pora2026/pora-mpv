import json
import sys
import types
import unittest
from io import BytesIO

from flask import Flask

from app.extensions import db
from app.models.user import User
from app.models.arandu import (
    AranduAuditEvent,
    AranduConversation,
    AranduMessage,
    AranduRule,
)
from app.services.arandu_access_service import user_can_access_arandu
from app.services import arandu_finance_service
from app.services.backup_migrations import migrate_payload_to_current
from app.services.backup_schema import CURRENT_FORMAT_VERSION
from app.services.arandu_document_service import DocumentValidationError, save_document
from app.services.arandu_orchestrator import ask_arandu
from werkzeug.datastructures import FileStorage


class AranduFoundationTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(
            TESTING=True,
            SQLALCHEMY_DATABASE_URI="sqlite:///:memory:",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
            ARANDU_ALLOWED_USERS="Bernardo,Ximena",
        )
        db.init_app(self.app)
        with self.app.app_context():
            db.create_all()

    def tearDown(self):
        with self.app.app_context():
            db.session.remove()
            db.drop_all()

    def test_access_is_explicit_and_case_insensitive(self):
        with self.app.app_context():
            self.assertTrue(user_can_access_arandu(User(username="bernardo")))
            self.assertTrue(user_can_access_arandu(User(username="XIMENA")))
            self.assertFalse(user_can_access_arandu(User(username="admin")))

    def test_auditable_shared_conversation_can_be_persisted(self):
        with self.app.app_context():
            bernardo = User(username="Bernardo", password_hash="x")
            db.session.add(bernardo)
            db.session.flush()
            conversation = AranduConversation(created_by_user_id=bernardo.id)
            db.session.add(conversation)
            db.session.flush()
            db.session.add_all(
                [
                    AranduMessage(
                        conversation_id=conversation.id,
                        user_id=bernardo.id,
                        role="user",
                        content="Analizá el mes actual.",
                    ),
                    AranduAuditEvent(
                        conversation_id=conversation.id,
                        user_id=bernardo.id,
                        event_type="conversation.created",
                        payload_json=json.dumps({"source": "web"}),
                    ),
                    AranduRule(
                        rule_key="cash_basis_expenses",
                        title="Gastos por caja",
                        content="Los gastos se registran cuando se pagan.",
                        status="approved",
                        proposed_by_user_id=bernardo.id,
                        approved_by_user_id=bernardo.id,
                    ),
                ]
            )
            db.session.commit()
            self.assertEqual(AranduMessage.query.count(), 1)
            self.assertEqual(AranduAuditEvent.query.count(), 1)
            self.assertEqual(AranduRule.query.count(), 1)

    def test_v3_backup_migrates_with_empty_arandu_tables(self):
        payload = {
            "format_version": 3,
            "manifest": {"tables": {}, "total_rows": 0},
            "tables": {},
        }
        migrated, applied = migrate_payload_to_current(payload)
        self.assertEqual(migrated["format_version"], CURRENT_FORMAT_VERSION)
        self.assertEqual(applied, ["v3 → v4", "v4 → v5"])
        self.assertEqual(migrated["tables"]["arandu_rules"], [])
        self.assertIn("content_sha256", migrated["manifest"])

    def test_financial_summary_preserves_current_pora_definitions(self):
        from datetime import date
        from app.models import BusinessDay

        with self.app.app_context():
            db.session.add_all(
                [
                    BusinessDay(
                        day=date(2026, 9, 1),
                        daily_cash_withdrawn=500.0,
                        daily_mercadopago=300.0,
                        real_apps_collected=100.0,
                        actual_cash_balance=5000.0,
                        reserved_funds_balance=700.0,
                    ),
                    BusinessDay(
                        day=date(2026, 9, 2),
                        daily_cash_withdrawn=200.0,
                        daily_mercadopago=100.0,
                        actual_cash_balance=5200.0,
                    ),
                ]
            )
            db.session.commit()

            original = arandu_finance_service.get_period_series
            arandu_finance_service.get_period_series = lambda *_: [
                {
                    "income": 2000.0,
                    "expense_total": 400.0,
                    "apps_gross": 500.0,
                    "apps_retention_estimate": 207.0,
                    "profit": 1600.0,
                    "profit_adjusted": 1393.0,
                }
            ]
            try:
                summary = arandu_finance_service.get_financial_period_summary(
                    date(2026, 9, 1),
                    date(2026, 9, 30),
                    today=date(2026, 9, 2),
                )
            finally:
                arandu_finance_service.get_period_series = original

            self.assertEqual(summary["calculated"]["profit"], 1393.0)
            self.assertEqual(summary["liquid"]["profit"], 800.0)
            self.assertFalse(summary["liquid"]["reserved_funds_deducted"])
            self.assertEqual(summary["real_accumulated"]["balance"], 5200.0)
            self.assertTrue(summary["real_accumulated"]["may_include_previous_periods"])

    def test_period_comparison_reports_amount_and_percentage(self):
        current = {
            "calculated": {"gross_sales": 120, "paid_expenses": 60, "profit": 60},
            "liquid": {"profit": 40},
        }
        previous = {
            "calculated": {"gross_sales": 100, "paid_expenses": 50, "profit": 50},
            "liquid": {"profit": 20},
        }
        original = arandu_finance_service.get_financial_period_summary
        arandu_finance_service.get_financial_period_summary = (
            lambda start, *_args, **_kwargs: current if start.day == 1 else previous
        )
        try:
            from datetime import date
            result = arandu_finance_service.compare_financial_periods(
                date(2026, 9, 1), date(2026, 9, 30),
                date(2026, 8, 2), date(2026, 8, 31),
            )
        finally:
            arandu_finance_service.get_financial_period_summary = original
        self.assertEqual(result["changes"]["gross_sales"], {"amount": 20.0, "pct": 20.0})
        self.assertEqual(result["changes"]["liquid_profit"], {"amount": 20.0, "pct": 100.0})
        self.assertIsNone(result["warning"])

    def test_expense_breakdown_reconciles_categories_and_unclassified_amount(self):
        from datetime import date
        from app.models import BusinessDay, ExpenseCategory, ExpenseEntry

        with self.app.app_context():
            day = BusinessDay(day=date(2026, 9, 3))
            category = ExpenseCategory(kind="fixed", name="Alquiler")
            db.session.add_all([day, category])
            db.session.flush()
            db.session.add(
                ExpenseEntry(
                    business_day_id=day.id,
                    category_id=category.id,
                    kind="fixed",
                    amount=300.0,
                )
            )
            db.session.commit()
            result = arandu_finance_service.get_expense_breakdown(
                date(2026, 9, 1),
                date(2026, 9, 30),
                period_summary={"calculated": {"paid_expenses": 350.0}},
            )
            self.assertEqual(result["categorized_total"], 300.0)
            self.assertEqual(result["unclassified_difference"], 50.0)
            self.assertFalse(result["reconciled"])

    def test_document_is_stored_in_database_with_hash_and_text(self):
        with self.app.app_context():
            user = User(username="Bernardo", password_hash="x")
            db.session.add(user)
            db.session.commit()
            document = save_document(
                FileStorage(
                    stream=BytesIO(b'{"ventas": 123}'),
                    filename="datos.json",
                    content_type="application/json",
                ),
                user.id,
            )
            self.assertEqual(document.file_content, b'{"ventas": 123}')
            self.assertIn('"ventas": 123', document.extracted_text)
            self.assertEqual(len(document.sha256), 64)

    def test_document_rejects_executable_content_type_by_extension(self):
        with self.app.app_context():
            user = User(username="Bernardo", password_hash="x")
            db.session.add(user)
            db.session.commit()
            with self.assertRaises(DocumentValidationError):
                save_document(
                    FileStorage(stream=BytesIO(b"danger"), filename="run.exe"),
                    user.id,
                )

    def test_orchestrator_persists_shared_auditable_chat(self):
        from unittest.mock import patch

        class FakeResponses:
            def create(self, **_kwargs):
                return types.SimpleNamespace(output=[], output_text="Conclusión verificable.")

        fake_module = types.SimpleNamespace(
            OpenAI=lambda **_kwargs: types.SimpleNamespace(responses=FakeResponses())
        )
        with self.app.app_context(), patch.dict(
            "os.environ", {"OPENAI_API_KEY": "test-key"}
        ), patch.dict(sys.modules, {"openai": fake_module}):
            self.app.config["ARANDU_OPENAI_MODEL"] = "test-model"
            self.app.config["ARANDU_WEB_SEARCH_ENABLED"] = False
            user = User(username="Bernardo", password_hash="x")
            db.session.add(user)
            db.session.commit()
            message, evidence = ask_arandu(user, "¿Cómo está el negocio?")
            self.assertEqual(message.content, "Conclusión verificable.")
            self.assertEqual(evidence, [])
            self.assertEqual(AranduMessage.query.count(), 2)
            self.assertGreaterEqual(AranduAuditEvent.query.count(), 2)


if __name__ == "__main__":
    unittest.main()
