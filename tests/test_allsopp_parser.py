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


INLINE_FIXTURES = {
    "listing_page_lettings_sample.html": (
        '<html><body><script id="__NEXT_DATA__" type="application/json">'
        '{"props": {"pageProps": {"data": {"data": {"hits": [{"_id": "a0EdK000000RZbBUAW", "fields": {"id": ["a0EdK000000RZbBUAW"], "pba__broker_s_listing_id__c": ["L-278275"], "pba__listingprice_pb__c": [80000], "pba__bedrooms_pb__c": [1], "pba__fullbathrooms_pb__c": [1], "pba__totalarea_pb__c": [798], "listing_area": [", Belgravia Heights 1, Jumeirah Village Circle."], "property_type_website__c": ["Apartment"], "pba__status__c": ["To Let - Live"], "listing_agent_name": ["Nashon Mwamba"], "listing_agent_mobile": ["+971558190942"], "listing_agent_Email": ["nashon@allsoppandallsopp.com"], "listing_agent_Whatsapp": ["+971558190942"], "pba__latitude_pb__c": [25.053812], "pba__longitude_pb__c": [55.220251], "images": ["20250902_153107.jpg", "20250902_153144.jpg"], "property_video": ["NULL"], "name": ["Unfurnished | 1 Bed | Available September"], "pba__listingtype__c": ["Rent"], "business_type_aa__c": ["Residential"], "pba__property__c": ["a0OdK000001FBVdUAO"]}}]}}}}}'
        "</script></body></html>"
    )
}


def read_fixture(name: str) -> str:
    if name in INLINE_FIXTURES:
        return INLINE_FIXTURES[name]
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
        self.assertEqual(row["listing_category"], "sales")

        second_row = df.set_index("id").loc["a0EdK000000FWfWUAW"]
        self.assertTrue(pd.isna(second_row["property_video"]))
        self.assertEqual(second_row["listing_category"], "sales")

    def test_parse_listing_page_lettings_builds_lettings_urls(self) -> None:
        html = read_fixture("listing_page_lettings_sample.html")
        df = parse_allsopp_listing_page(html, detail_segment="lettings")

        self.assertFalse(df.empty)
        self.assertEqual(len(df), 1)

        row = df.iloc[0]
        self.assertEqual(row["listing_type"], "Rent")
        self.assertEqual(row["listing_category"], "lettings")
        self.assertEqual(
            row["detail_url"],
            "https://www.allsoppandallsopp.com/dubai/property/lettings/L-278275",
        )
        self.assertEqual(row["reference_number"], "L-278275")
        self.assertTrue(pd.isna(row["property_video"]))
        self.assertEqual(row["price"], 80000)

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
