import unittest

from flask import Flask

from app.extensions import db
from app.models import BusinessDay, ShiftRecord
from app.routes.caja_api import caja_api_bp


TOKEN = "test-caja-sync-token"


def payload(day="2026-08-20", morning=344667, afternoon=432306):
    return {
        "contract_version": 1,
        "source": "caja",
        "day": day,
        "shifts": {
            "MORNING": {"income": morning},
            "AFTERNOON": {"income": afternoon},
        },
        "real_apps_pending": 57319,
        "daily_mercadopago": 498354,
        "daily_cash_withdrawn": 225000,
        "operating_cash_balance": 15100,
    }


class CajaApiSyncTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(
            TESTING=True,
            SQLALCHEMY_DATABASE_URI="sqlite:///:memory:",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
            CAJA_SYNC_TOKEN=TOKEN,
            CAJA_SYNC_START_DATE="2026-08-13",
        )
        db.init_app(self.app)
        self.app.register_blueprint(caja_api_bp)

        with self.app.app_context():
            db.create_all()

        self.client = self.app.test_client()
        self.headers = {"Authorization": f"Bearer {TOKEN}"}

    def tearDown(self):
        with self.app.app_context():
            db.session.remove()
            db.drop_all()

    def test_health_requires_valid_token(self):
        self.assertEqual(self.client.get("/api/v1/caja/health").status_code, 401)
        response = self.client.get("/api/v1/caja/health", headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["contract_version"], 1)

    def test_creates_day_and_maps_only_expected_fields(self):
        response = self.client.put(
            "/api/v1/caja/days/2026-08-20",
            json=payload(),
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 201)

        with self.app.app_context():
            bday = BusinessDay.query.one()
            self.assertEqual(bday.real_apps_pending, 57319)
            self.assertEqual(bday.daily_mercadopago, 498354)
            self.assertEqual(bday.daily_cash_withdrawn, 225000)
            self.assertEqual(bday.operating_cash_balance, 15100)
            self.assertEqual(bday.status, "complete")

            shifts = {s.shift: s for s in bday.shifts}
            self.assertEqual(shifts["Mañana"].income, 344667)
            self.assertEqual(shifts["Tarde"].income, 432306)
            self.assertTrue(shifts["Mañana"].is_closed)
            self.assertTrue(shifts["Tarde"].is_closed)

    def test_repeat_is_idempotent_and_updates_existing_day(self):
        first = self.client.put(
            "/api/v1/caja/days/2026-08-20",
            json=payload(),
            headers=self.headers,
        )
        self.assertEqual(first.status_code, 201)

        second_payload = payload(morning=350000, afternoon=440000)
        second = self.client.put(
            "/api/v1/caja/days/2026-08-20",
            json=second_payload,
            headers=self.headers,
        )
        self.assertEqual(second.status_code, 200)
        self.assertEqual(second.get_json()["action"], "updated")

        with self.app.app_context():
            self.assertEqual(BusinessDay.query.count(), 1)
            self.assertEqual(ShiftRecord.query.count(), 2)
            bday = BusinessDay.query.one()
            shifts = {s.shift: s for s in bday.shifts}
            self.assertEqual(shifts["Mañana"].income, 350000)
            self.assertEqual(shifts["Tarde"].income, 440000)

    def test_sync_preserves_manual_pora_fields(self):
        with self.app.app_context():
            bday = BusinessDay(
                day=__import__("datetime").date(2026, 8, 20),
                note="nota manual",
                status="complete",
                real_cash_profit=123,
                real_digital_profit=456,
                real_apps_collected=789,
                opening_cash_balance=1000,
                actual_cash_balance=2000,
                safe_box_transfer=3000,
                reserved_funds_balance=4000,
            )
            db.session.add(bday)
            db.session.flush()
            db.session.add(ShiftRecord(business_day=bday, shift="Mañana", income=1, note="m"))
            db.session.add(ShiftRecord(business_day=bday, shift="Tarde", income=2, note="t"))
            db.session.commit()

        response = self.client.put(
            "/api/v1/caja/days/2026-08-20",
            json=payload(),
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            bday = BusinessDay.query.one()
            self.assertEqual(bday.note, "nota manual")
            self.assertEqual(bday.real_cash_profit, 123)
            self.assertEqual(bday.real_digital_profit, 456)
            self.assertEqual(bday.real_apps_collected, 789)
            self.assertEqual(bday.opening_cash_balance, 1000)
            self.assertEqual(bday.actual_cash_balance, 2000)
            self.assertEqual(bday.safe_box_transfer, 3000)
            self.assertEqual(bday.reserved_funds_balance, 4000)
            shifts = {s.shift: s for s in bday.shifts}
            self.assertEqual(shifts["Mañana"].note, "m")
            self.assertEqual(shifts["Tarde"].note, "t")


    def test_rejects_dates_before_sync_start_without_writing(self):
        old_day = "2026-08-12"
        response = self.client.put(
            f"/api/v1/caja/days/{old_day}",
            json=payload(day=old_day),
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("2026-08-13", response.get_json()["detail"])

        with self.app.app_context():
            self.assertEqual(BusinessDay.query.count(), 0)
            self.assertEqual(ShiftRecord.query.count(), 0)

    def test_rejects_date_mismatch_without_writing(self):
        response = self.client.put(
            "/api/v1/caja/days/2026-08-20",
            json=payload(day="2026-08-21"),
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 400)
        with self.app.app_context():
            self.assertEqual(BusinessDay.query.count(), 0)

    def test_rejects_unsupported_contract(self):
        data = payload()
        data["contract_version"] = 99
        response = self.client.put(
            "/api/v1/caja/days/2026-08-20",
            json=data,
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 400)


if __name__ == "__main__":
    unittest.main()
