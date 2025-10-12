import sys
import unittest
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from plombery.allsopp_crs import (  # noqa: E402 - module path tweaked above
    parse_allsopp_detail_page,
    parse_allsopp_listing_page,
)


def read_fixture(name: str) -> str:
    path = PROJECT_ROOT / "tests" / "fixtures" / "allsopp" / name
    return path.read_text(encoding="utf-8")


class AllsoppParserTest(unittest.TestCase):
    def test_parse_listing_page_extracts_expected_columns(self) -> None:
        html = read_fixture("listing_page_sample.html")
        df = parse_allsopp_listing_page(html)

        self.assertFalse(df.empty)
        self.assertEqual(len(df), 2)

        expected_cols = {
            "id",
            "reference_number",
            "price",
            "bedrooms",
            "bathrooms",
            "detail_url",
            "name",
        }
        self.assertTrue(expected_cols.issubset(set(df.columns)))

        row = df.set_index("id").loc["a0EdK000000FWfVUAW"]
        self.assertEqual(row["reference_number"], "L-264834")
        self.assertEqual(row["price"], 4900000)
        self.assertEqual(row["bedrooms"], 3)
        self.assertEqual(
            row["detail_url"],
            "https://www.allsoppandallsopp.com/dubai/property/sales/L-264834",
        )
        self.assertEqual(row["listing_area"], "Al Fattan Marine Towers, Jumeirah Beach Residence.")

        second_row = df.set_index("id").loc["a0EdK000000FWfWUAW"]
        self.assertTrue(pd.isna(second_row["property_video"]))

    def test_parse_detail_page_structured_data(self) -> None:
        html = read_fixture("detail_page_sample.html")
        detail = parse_allsopp_detail_page(html)

        self.assertTrue(detail)
        self.assertEqual(detail["detail_name"], "Highly Upgraded | Must See | Sea View")
        self.assertEqual(detail["detail_price"], 4900000)
        self.assertEqual(detail["detail_bedrooms"], 3)
        self.assertEqual(detail["detail_listing_area"], "Al Fattan Marine Towers, Jumeirah Beach Residence.")
        self.assertEqual(detail["detail_agent_mobile"], "+971585003435")
        self.assertEqual(detail["detail_private_amenities"], ["Balcony", "View of Water", "Vacant on Transfer"])
        self.assertIsNotNone(detail.get("detail_transferred_date_epoch"))
        self.assertIn("detail_transferred_date_iso", detail)


if __name__ == "__main__":
    unittest.main()
