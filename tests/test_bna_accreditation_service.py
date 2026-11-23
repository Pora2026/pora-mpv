from datetime import date
import unittest
from app.services.bna_accreditation_service import (
    eighth_banking_day, summarize_bna_receivables,
)


class BnaAccreditationTests(unittest.TestCase):
    def test_eight_banking_days_with_holiday(self):
        self.assertEqual(eighth_banking_day(date(2026, 11, 19),
            {date(2026, 11, 23)}), date(2026, 12, 2))

    def test_pending_and_overdue_with_partial_confirmation(self):
        result = summarize_bna_receivables(
            [(date(2026, 11, 19), 100000)],
            [(date(2026, 11, 19), 40000)],
            date(2026, 12, 3), {date(2026, 11, 23)})
        self.assertEqual(result, {"pending": 60000, "overdue": 60000,
                                  "not_due": 0})

    def test_not_due_on_maturity(self):
        result = summarize_bna_receivables(
            [(date(2026, 11, 19), 100000)], [],
            date(2026, 12, 2), {date(2026, 11, 23)})
        self.assertEqual(result["overdue"], 0)

    def test_cannot_overconfirm(self):
        with self.assertRaises(ValueError):
            summarize_bna_receivables([(date(2026, 11, 19), 100)],
                [(date(2026, 11, 19), 101)], date(2026, 12, 3))

if __name__ == '__main__':
    unittest.main()
