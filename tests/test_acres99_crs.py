from unittest.mock import patch

import pandas as pd

from acres99_crs import (
    BASE_SETTINGS,
    ES_INDEX_MAPPING,
    Acres99BaseSettings,
    Acres99JobSettings,
    _build_listing_page_url,
    _fetch,
    _is_generic_title,
    _is_99acres_listing_url,
    _looks_blocked,
    _name_from_url,
    _normalize_possession_date,
    _parse_area,
    _parse_price,
    _validate_record,
    df_to_actions,
    ensure_index,
    es_doc_exists,
    parse_99acres_detail_page,
    parse_99acres_listing_page,
)


def test_parse_acres99_listing_fixture(acres99_html):
    df = parse_99acres_listing_page(acres99_html, category="buy", city_label="Kochi", city_slug="kochi")
    assert not df.empty
    row = df.iloc[0].to_dict()
    assert row["title"] == "Skyline Habitat"
    assert row["city"] == "Kochi"
    assert row["locality"] == "Kakkanad"
    assert row["listing_category"] == "buy"
    assert row["source"] == "99acres.com"
    assert row["price_min"] == 12000000


def test_parse_acres99_listing_empty(empty_html):
    df = parse_99acres_listing_page(empty_html, category="buy", city_label="Kochi", city_slug="kochi")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_parse_acres99_listing_link_fallback():
    html = """
    <html><body>
      <a href="/property-in-kochi-ffid-page-2">Next</a>
      <a href="/skyline-habitat-kakkanad-kochi-npxid-r467655">Skyline Habitat</a>
      <a href="/contact">Contact</a>
    </body></html>
    """
    df = parse_99acres_listing_page(html, category="buy", city_label="Kochi", city_slug="kochi")
    assert len(df) == 1
    row = df.iloc[0].to_dict()
    assert row["title"] == "Skyline Habitat"
    assert row["detail_url"].endswith("npxid-r467655")


def test_parse_acres99_detail_page(acres99_detail_html):
    detail = parse_99acres_detail_page(acres99_detail_html)
    assert detail["detail_title"] == "Skyline Habitat"
    assert detail["detail_rera_number"] == "K-RERA/987/2026"
    assert detail["detail_possession_date"] == "2027-12"
    assert detail["detail_price_min"] == 12000000
    assert detail["detail_area_sqft"] == 1350.0


def test_parse_acres99_detail_from_meta_when_missing_data():
    html = """
    <html>
      <head>
        <meta property="og:title" content="Riverfront Homes" />
        <meta name="description" content="RERA: K-RERA/111/2026. Possession by Jun 2028" />
      </head>
      <body>Riverfront Homes</body>
    </html>
    """
    detail = parse_99acres_detail_page(html)
    assert detail["detail_title"] == "Riverfront Homes"
    assert detail["detail_rera_number"] == "K-RERA/111/2026"
    assert detail["detail_possession_date"] == "2028-06"


def test_looks_blocked_positive():
    text = '<body data-label="CAPTCHA">blocked</body>'
    assert _looks_blocked(text) is True


def test_looks_blocked_with_allow_marker():
    text = "captcha __initialData__ 99acres"
    assert _looks_blocked(text) is False


def test_parse_price_variants():
    assert _parse_price("55 Lac - 85 Lac")[0] == 5500000
    assert _parse_price("55 Lac - 85 Lac")[1] == 8500000
    assert _parse_price("INR 1.2 Cr")[0] == 12000000
    assert _parse_price("Price on request")[0] is None


def test_parse_area_variants():
    assert _parse_area("1200 sq.ft")[0] == 1200.0
    assert round(_parse_area("100 sqm")[0], 2) == 1076.39
    assert round(_parse_area("100 sq yd")[0], 2) == 900.0
    assert round(_parse_area("1 acre")[0], 2) == 43560.0


def test_normalize_possession_date_variants():
    assert _normalize_possession_date("Jun 2026") == "2026-06"
    assert _normalize_possession_date("Q4 2027") == "2027-12"
    assert _normalize_possession_date("2028") == "2028-01"
    assert _normalize_possession_date("Ready to move") is None


def test_name_from_url_strips_ffid_suffix():
    url = "https://www.99acres.com/property-in-kochi-ffid-1234"
    assert _name_from_url(url) == "Property In Kochi"


def test_is_generic_title():
    assert _is_generic_title("projects in kochi") is True
    assert _is_generic_title("Skyline Habitat") is False


def test_is_99acres_listing_url_filters_assets_and_accepts_detail_links():
    assert _is_99acres_listing_url("https://www.99acres.com/sophia-melody-kakkanad-kochi-npxid-r467655") is True
    assert _is_99acres_listing_url("https://www.99acres.com/2-bhk-bedroom-apartment-flat-for-sale-in-x-spid-a91554138") is True
    assert _is_99acres_listing_url("https://newprojects.99acres.com/projects/foo/bar/images/sample.jpg") is False
    assert _is_99acres_listing_url("https://www.99acres.com/property-in-kochi-ffid") is False


def test_validate_record_mandatory_fields():
    record = {
        "id": "abc",
        "title": "Skyline Habitat",
        "detail_url": "https://www.99acres.com/skyline-habitat-kakkanad-kochi-npxid-r467655",
        "city": "Kochi",
        "source": "99acres.com",
        "price_min": 9000000,
        "price_max": 7000000,
    }
    validated = _validate_record(record)
    assert validated["price_min"] == 7000000
    assert validated["price_max"] == 9000000


def test_validate_record_rejects_invalid_url():
    record = {
        "id": "abc",
        "title": "Skyline Habitat",
        "detail_url": "https://example.com/abc",
        "city": "Kochi",
        "source": "99acres.com",
    }
    try:
        _validate_record(record)
        assert False, "Expected ValueError"
    except ValueError:
        assert True


def test_build_listing_page_url_with_placeholders():
    job = Acres99JobSettings(
        name="buy_kochi",
        category="buy",
        city_slug="kochi",
        city_label="Kochi",
        listing_url_template="https://www.99acres.com/property-in-{city}-ffid-page-{page}",
        pages=2,
        es_index="acres99_properties",
    )
    assert _build_listing_page_url(job, 2) == "https://www.99acres.com/property-in-kochi-ffid-page-2"


def test_fetch_retries_and_succeeds():
    class FakeResponse:
        def __init__(self, text, status_code=200):
            self.text = text
            self.status_code = status_code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError("http error")

    class FakeSession:
        def __init__(self):
            self.calls = 0

        def get(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                return FakeResponse('data-label="CAPTCHA"', 200)
            return FakeResponse("<html>ok __initialData__</html>", 200)

    settings = Acres99BaseSettings(
        enabled=True,
        min_delay_seconds=0.01,
        max_delay_seconds=0.02,
        detail_retry_count=1,
        request_timeout=10,
        stop_on_existing=False,
        data_dir=BASE_SETTINGS.data_dir,
        schedule_hour="5",
        schedule_minute="30",
        schedule_timezone="Asia/Kolkata",
    )
    client = type("C", (), {"session": FakeSession(), "use_curl_cffi": False, "api_token": None})()
    with patch("acres99_crs.time.sleep"):
        html = _fetch(client, "https://www.99acres.com", retries=2, base_settings=settings)
    assert "__initialData__" in html


def test_ensure_index_creates_when_missing(mock_es_client):
    mock_es_client.indices.exists.return_value = False
    ensure_index(mock_es_client, "acres99_properties")
    mock_es_client.indices.create.assert_called_once()
    args = mock_es_client.indices.create.call_args
    assert args[1]["index"] == "acres99_properties"
    assert args[1]["body"] == ES_INDEX_MAPPING


def test_es_doc_exists_behaviour(mock_es_client):
    mock_es_client.exists.return_value = True
    assert es_doc_exists(mock_es_client, "acres99_properties", "abc") is True
    mock_es_client.exists.return_value = False
    assert es_doc_exists(mock_es_client, "acres99_properties", "abc") is False


def test_df_to_actions_uses_target_index():
    df = pd.DataFrame(
        [
            {
                "id": "a1",
                "title": "T1",
                "_target_index": "acres99_properties",
            },
            {
                "id": "a2",
                "title": "T2",
                "_target_index": "acres99_rent_properties",
            },
        ]
    )
    actions = list(df_to_actions(df, default_index=None))
    assert len(actions) == 2
    assert actions[0]["_index"] == "acres99_properties"
    assert actions[1]["_index"] == "acres99_rent_properties"
    assert actions[0]["_op_type"] == "index"


def test_df_to_actions_raises_without_index():
    df = pd.DataFrame([{"id": "a1", "title": "T1"}])
    try:
        list(df_to_actions(df, default_index=None))
        assert False, "Expected ValueError"
    except ValueError:
        assert True
