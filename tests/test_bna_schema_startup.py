import unittest
from sqlalchemy import create_engine, inspect, text
from app.services.bna_schema import ensure_daily_bna_column


class BnaSchemaStartupTests(unittest.TestCase):
    def test_add_column_preserves_data_and_is_idempotent(self):
        engine = create_engine('sqlite:///:memory:')
        with engine.begin() as connection:
            connection.execute(text('CREATE TABLE business_days (id INTEGER PRIMARY KEY, income REAL)'))
            connection.execute(text('INSERT INTO business_days (id, income) VALUES (1, 123.45)'))
        self.assertTrue(ensure_daily_bna_column(engine))
        self.assertFalse(ensure_daily_bna_column(engine))
        with engine.connect() as connection:
            self.assertEqual(connection.execute(text('SELECT id, income, daily_bna FROM business_days')).one(), (1, 123.45, None))

    def test_missing_table_fails_without_creating_it(self):
        engine = create_engine('sqlite:///:memory:')
        with self.assertRaises(RuntimeError):
            ensure_daily_bna_column(engine)
        self.assertFalse(inspect(engine).has_table('business_days'))
