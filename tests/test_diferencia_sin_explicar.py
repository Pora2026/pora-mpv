import unittest
from pathlib import Path
from app.services.finance_service import compute_expected_cash_balance, compute_cash_difference

class DiferenciaSinExplicarTests(unittest.TestCase):
    def test_diferencia_real_menos_esperado(self):
        expected = compute_expected_cash_balance(1000, 400, 200, 100)
        self.assertEqual(expected, 1100)
        self.assertEqual(compute_cash_difference(expected, 900), -200)
        self.assertEqual(compute_cash_difference(expected, 1300), 200)

    def test_vista_reutiliza_tarjeta_sin_recuadro_extra(self):
        source = (Path(__file__).resolve().parents[1] / 'app/routes/days.py').read_text(encoding='utf-8')
        self.assertIn('Diferencia sin explicar', source)
        self.assertIn('has_independent_balance', source)
        self.assertNotIn('Control independiente de saldo disponible (día seleccionado)', source)
        self.assertNotIn('Residuo de conciliación</div>', source)
        self.assertIn('Brecha explicada</div>', source)
        self.assertIn('BNA:', source)

if __name__ == '__main__':
    unittest.main()
