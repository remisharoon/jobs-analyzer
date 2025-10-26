import sys
import types
import unittest
from datetime import date, timedelta

PROJECT_ROOT = __import__("pathlib").Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from plombery.dld_open_data import (  # noqa: E402
    BASE_SETTINGS,
    DatasetConfig,
    _build_sql,
    _compute_start_date,
    _extract_max_date,
)


class DLDOpenDataHelpersTest(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = DatasetConfig(
            key="transactions",
            label="Transactions",
            resource_id="abcd-1234",
            date_field="registration_date",
            es_index="dld_transactions",
            buffer_days=2,
        )

    def test_build_sql(self) -> None:
        start = date(2024, 1, 15)
        sql = _build_sql(self.cfg, start, offset=100, limit=BASE_SETTINGS.page_size)
        self.assertIn('"abcd-1234"', sql)
        self.assertIn('"registration_date" >= \'2024-01-15\'', sql)
        self.assertTrue(sql.strip().startswith("SELECT"))
        self.assertIn("LIMIT", sql)
        self.assertIn("OFFSET 100", sql)

    def test_compute_start_date_uses_buffer(self) -> None:
        today = date.today()
        state = {"transactions": {"max_date": (today - timedelta(days=5)).isoformat()}}
        computed = _compute_start_date(self.cfg, state)
        self.assertEqual(computed, today - timedelta(days=7))

    def test_compute_start_date_without_history(self) -> None:
        state: dict[str, dict[str, str]] = {}
        computed = _compute_start_date(self.cfg, state)
        expected = date.today() - timedelta(days=BASE_SETTINGS.lookback_days)
        self.assertEqual(computed, expected)

    def test_extract_max_date(self) -> None:
        records = [
            {"registration_date": "2024-01-10"},
            {"registration_date": "2024-02-01T00:00:00"},
            {"registration_date": "2023-12-30"},
        ]
        max_dt = _extract_max_date(records, "registration_date")
        self.assertEqual(max_dt, date(2024, 2, 1))


if __name__ == "__main__":
    unittest.main()
