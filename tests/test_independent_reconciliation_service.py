import unittest
from app.services.independent_reconciliation_service import reconcile_treasury, reconcile_calc_liquid

class IndependentReconciliationTests(unittest.TestCase):
    def test_detects_unexplained_shortage(self):
        r = reconcile_treasury(1000000, 600000, 100000, 1420000)
        self.assertEqual(r['expected_balance'], 1500000)
        self.assertEqual(r['unexplained_financial_difference'], -80000)

    def test_verified_adjustments_leave_residual(self):
        r = reconcile_treasury(1000000, 600000, 100000, 1420000, [-50000, -20000])
        self.assertEqual(r['explained_financial_difference'], -70000)
        self.assertEqual(r['unexplained_financial_difference'], -10000)

    def test_internal_transfer_excluded(self):
        r = reconcile_treasury(100, 20, 10, 110)
        self.assertEqual(r['unexplained_financial_difference'], 0)

    def test_bna_pending_does_not_count_as_cash(self):
        r = reconcile_calc_liquid(100, 0, 0, 100, 0, 0, 0)
        self.assertEqual(r['bna_effect'], 100)
        self.assertEqual(r['calc_liquid_residual'], 0)

    def test_bna_partial_accreditation(self):
        r = reconcile_calc_liquid(100, 40, 0, 100, 40, 0, 0)
        self.assertEqual(r['bna_effect'], 60)

    def test_invalid_input_rejected(self):
        with self.assertRaises(ValueError):
            reconcile_treasury(0, -1, 0, 0)

if __name__ == '__main__': unittest.main()
