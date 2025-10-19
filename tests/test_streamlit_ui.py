import sys
import types
import unittest
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


def _ensure_streamlit_stub() -> None:
    if "streamlit" in sys.modules:
        return

    stub = types.ModuleType("streamlit")

    def cache_data(**_kwargs):
        def decorator(func):
            return func

        return decorator

    stub.cache_data = cache_data
    stub.secrets = types.SimpleNamespace(
        PostgresDB=types.SimpleNamespace(connection_string=""),
    )
    stub.__path__ = [str(SRC_PATH / "streamlit")]
    sys.modules["streamlit"] = stub


_ensure_streamlit_stub()

import importlib

jobs_data = importlib.import_module("streamlit.jobs_data")


class JobsDataUITest(unittest.TestCase):
    def test_filter_invalid_records_drops_missing_and_other_titles(self) -> None:
        df = pd.DataFrame(
            [
                {"country_inferred": "United Arab Emirates", "job_title_inferred": "Engineer", "listing_category": "sales"},
                {"country_inferred": None, "job_title_inferred": "Engineer", "listing_category": "sales"},
                {"country_inferred": "United Arab Emirates", "job_title_inferred": "Other", "listing_category": "lettings"},
                {"country_inferred": "None", "job_title_inferred": "Engineer", "listing_category": "sales"},
            ]
        )

        filtered = jobs_data.filter_invalid_records(df)

        self.assertEqual(len(filtered), 1)
        self.assertListEqual(filtered["job_title_inferred"].tolist(), ["Engineer"])
        self.assertIn("listing_category", filtered.columns)
        self.assertListEqual(filtered["listing_category"].tolist(), ["sales"])


if __name__ == "__main__":
    unittest.main()
