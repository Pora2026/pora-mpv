import unittest
from sqlalchemy import create_engine, text
from app.services.bna_schema import ensure_daily_bna_column, ensure_bna_accreditation_column
from app.services.finance_service import compute_calc_liquid_reconciliation

class BnaFullIntegrationTests(unittest.TestCase):
    def test_schema_migration_idempotent_and_preserves_rows(self):
        engine = create_engine('sqlite:///:memory:')
        with engine.begin() as c:
            c.execute(text('CREATE TABLE business_days (id INTEGER PRIMARY KEY, income REAL)'))
            c.execute(text('INSERT INTO business_days VALUES (1, 123.45)'))
        self.assertTrue(ensure_daily_bna_column(engine))
        self.assertTrue(ensure_bna_accreditation_column(engine))
        self.assertFalse(ensure_bna_accreditation_column(engine))
        with engine.connect() as c:
            self.assertEqual(c.execute(text('SELECT income, daily_bna, daily_bna_accredited FROM business_days')).one(), (123.45, None, None))

    def test_partial_credit_reduces_bna_effect(self):
        args = dict(calculated_profit=500, liquid_profit=300, apps_gross=0,
                    apps_collected=0, previous_operating_cash=0,
                    current_operating_cash=0, bna_sales=200)
        self.assertEqual(compute_calc_liquid_reconciliation(**args, bna_accredited=80)['bna_effect'], 120)
        self.assertEqual(compute_calc_liquid_reconciliation(**args, bna_accredited=200)['bna_effect'], 0)
        self.assertEqual(compute_calc_liquid_reconciliation(**args, bna_accredited=0)['bna_effect'], 200)

    def test_old_reconciliation_unchanged(self):
        r = compute_calc_liquid_reconciliation(100, 80, 0, 0, 0, 0)
        self.assertEqual(r['explained_gap'], 0)
        self.assertIsNone(r['bna_effect'])

if __name__ == '__main__': unittest.main()
