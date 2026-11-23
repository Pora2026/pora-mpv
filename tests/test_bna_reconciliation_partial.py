import unittest
from app.services.finance_service import compute_calc_liquid_reconciliation

class BnaReconciliationPartialTests(unittest.TestCase):
    def test_existing_calculation_unchanged_without_bna(self):
        result = compute_calc_liquid_reconciliation(500, 200, 0, 0, 0, 0)
        self.assertEqual(result['explained_gap'], 0)
        self.assertIsNone(result['bna_effect'])
        self.assertEqual(result['unexplained_gap'], 300)

    def test_bna_partial_accreditation(self):
        result = compute_calc_liquid_reconciliation(500, 200, 0, 0, 0, 0, bna_sales=200, bna_accredited=75)
        self.assertEqual(result['bna_effect'], 125)
        self.assertEqual(result['explained_gap'], 125)
        self.assertEqual(result['unexplained_gap'], 175)

    def test_bna_fully_accredited(self):
        result = compute_calc_liquid_reconciliation(500, 200, 0, 0, 0, 0, bna_sales=200, bna_accredited=200)
        self.assertEqual(result['bna_effect'], 0)

if __name__ == '__main__': unittest.main()
