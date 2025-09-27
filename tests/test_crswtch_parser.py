import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / 'src'
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from plombery.carswitch_crs import (  # noqa: E402 - imported after sys.path tweak
    parse_crswtch_detail_page,
    parse_crswtch_listing_page,
)


def read_fixture(name: str) -> str:
    path = PROJECT_ROOT / 'tests' / 'fixtures' / 'crswtch' / name
    return path.read_text(encoding='utf-8')


class CrswtchParserTest(unittest.TestCase):
    def test_parse_listing_page_extracts_expected_columns(self) -> None:
        html = read_fixture('listing_page_sample.html')
        df = parse_crswtch_listing_page(html)

        self.assertFalse(df.empty)
        self.assertEqual(len(df), 12)
        expected_cols = {'id', 'price', 'mileage', 'detail_url', 'detail_name'}
        self.assertTrue(expected_cols.issubset(df.columns))

        row = df.set_index('id').loc['715957']
        self.assertEqual(row['make'], 'infiniti')
        self.assertEqual(row['price'], 30500)
        self.assertEqual(
            row['detail_url'],
            'https://carswitch.com/dubai/used-car/infiniti/qx50/2015/715957',
        )
        self.assertEqual(row['detail_name'], '2015 Infiniti QX50')

    def test_parse_detail_page_structured_data(self) -> None:
        html = read_fixture('detail_page_sample.html')
        detail = parse_crswtch_detail_page(html)

        self.assertTrue(detail)
        self.assertEqual(detail['detail_name'], 'Infiniti QX50 2015 3.7')
        self.assertEqual(detail['detail_vehicle_identification_number'], 'BUYFROMCS00715957')
        self.assertEqual(detail['detail_offer_price'], 30500)
        self.assertEqual(detail['detail_color'], 'White')
        self.assertEqual(detail['detail_engine_fuel_type'], 'Petrol')
        self.assertEqual(
            detail['detail_item_url'],
            'https://carswitch.com/dubai/used-car/infiniti/qx50/2015/715957',
        )


if __name__ == '__main__':
    unittest.main()
