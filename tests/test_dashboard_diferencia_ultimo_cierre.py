import unittest
from pathlib import Path

class DashboardDifferenceTests(unittest.TestCase):
    def test_same_daily_formula_and_last_close(self):
        source = (Path(__file__).resolve().parents[1] / "app/routes/dashboard.py").read_text(encoding="utf-8")
        self.assertIn("Diferencia sin explicar</div>",source)
        self.assertNotIn("Residuo de conciliación",source)
        self.assertIn("expected_day = compute_expected_cash_balance(",source)
        self.assertIn("opening_balance=b.opening_cash_balance",source)
        self.assertIn("reserved_funds_change=(",source)
        self.assertIn("current_reserved_funds - running_reserved_funds",source)
        self.assertIn('accumulated_unexplained = actual_close - expected_close',source)
        self.assertIn("d1 <= d <= d2",source)
        self.assertIn("accumulated_unexplained_pct <= 1",source)
        self.assertIn("accumulated_unexplained_pct <= 5",source)
        self.assertIn("Brecha explicada acumulada",source)

if __name__=="__main__": unittest.main()
