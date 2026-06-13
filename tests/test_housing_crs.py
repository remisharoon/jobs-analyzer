from unittest.mock import patch

import pandas as pd

from housing_crs import (
    BASE_SETTINGS,
    ES_INDEX_MAPPING,
    HousingBaseSettings,
    HousingJobSettings,
    _build_listing_page_url,
    _fetch,
    _is_generic_title,
    _looks_blocked,
    _name_from_url,
    _parse_area,
    _parse_price,
    _normalize_configurations,
    _normalize_possession_date,
    _validate_record,
    df_to_actions,
    ensure_index,
    es_doc_exists,
    parse_housing_detail_page,
    parse_housing_listing_page,
)


def test_parse_housing_listing_fixture(housing_html):
    df = parse_housing_listing_page(housing_html, category="buy", city_label="Kochi", city_slug="kochi")
    assert not df.empty
    row = df.iloc[0].to_dict()
    assert row["title"] == "Godrej Kochi Riverside"
    assert row["builder_name"] == "Godrej Properties"
    assert row["price_currency"] == "INR"
    assert row["listing_category"] == "buy"
    assert row["city"] == "Kochi"
    assert row["source"] == "housing.com"


def test_parse_housing_listing_empty(empty_html):
    df = parse_housing_listing_page(empty_html, category="buy", city_label="Kochi", city_slug="kochi")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_parse_housing_listing_link_fallback():
    html = """
    <html><body>
      <a href="/in/buy/resale/page-1">Next</a>
      <a href="/in/projects/kochi/skyline-habitat-prjid-1234">Skyline Habitat</a>
      <a href="/contact">Contact</a>
    </body></html>
    """
    df = parse_housing_listing_page(html, category="buy", city_label="Kochi", city_slug="kochi")
    assert len(df) == 1
    row = df.iloc[0].to_dict()
    assert row["title"] == "Skyline Habitat"
    assert row["detail_url"].endswith("skyline-habitat-prjid-1234")


def test_parse_housing_detail_page(housing_detail_html):
    detail = parse_housing_detail_page(housing_detail_html)
    assert detail["detail_title"] == "Skyline Habitat"
    assert detail["detail_builder_name"] == "Skyline Builders"
    assert detail["detail_rera_number"] == "K-RERA/987/2026"
    assert detail["detail_possession_date"] == "2027-12"
    assert detail["detail_price_min"] == 12000000
    assert detail["detail_area_sqft"] == 1350.0


def test_parse_housing_detail_from_meta_when_missing_next_data():
    html = """
    <html>
      <head>
        <meta property="og:title" content="Riverfront Homes" />
        <meta name="description" content="RERA: K-RERA/111/2026. Possession by Jun 2028" />
      </head>
      <body>Riverfront Homes</body>
    </html>
    """
    detail = parse_housing_detail_page(html)
    assert detail["detail_title"] == "Riverfront Homes"
    assert detail["detail_rera_number"] == "K-RERA/111/2026"
    assert detail["detail_possession_date"] == "2028-06"


def test_looks_blocked_positive():
    text = "Please solve captcha to continue"
    assert _looks_blocked(text) is True


def test_looks_blocked_with_allow_marker():
    text = "captcha __NEXT_DATA__ housing.com"
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


def test_normalize_configurations_variants():
    assert _normalize_configurations("2,3 BHK") == ["2BHK", "3BHK"]
    assert _normalize_configurations("studio, 1 rk") == ["Studio", "1RK"]


def test_normalize_possession_date_variants():
    assert _normalize_possession_date("Jun 2026") == "2026-06"
    assert _normalize_possession_date("Q4 2027") == "2027-12"
    assert _normalize_possession_date("2028") == "2028-01"
    assert _normalize_possession_date("Ready to move") is None


def test_name_from_url_strips_prjid_suffix():
    url = "https://housing.com/in/projects/kochi/skyline-habitat-prjid-1234"
    assert _name_from_url(url) == "Skyline Habitat"


def test_is_generic_title():
    assert _is_generic_title("projects in kochi") is True
    assert _is_generic_title("Skyline Habitat") is False


def test_validate_record_mandatory_fields():
    record = {
        "id": "abc",
        "title": "Skyline Habitat",
        "detail_url": "https://housing.com/in/projects/kochi/skyline-habitat-prjid-1234",
        "city": "Kochi",
        "source": "housing.com",
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
        "source": "housing.com",
    }
    try:
        _validate_record(record)
        assert False, "Expected ValueError"
    except ValueError:
        assert True


def test_build_listing_page_url_with_placeholders():
    job = HousingJobSettings(
        name="buy_kochi",
        category="buy",
        city_slug="kochi",
        city_label="Kochi",
        listing_url_template="https://housing.com/in/buy/{city}?page={page}",
        pages=2,
        es_index="housing_properties",
    )
    assert _build_listing_page_url(job, 2) == "https://housing.com/in/buy/kochi?page=2"


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
                return FakeResponse("captcha", 200)
            return FakeResponse("<html>ok __NEXT_DATA__</html>", 200)

    settings = HousingBaseSettings(
        enabled=True,
        min_delay_seconds=0.01,
        max_delay_seconds=0.02,
        detail_retry_count=1,
        request_timeout=10,
        stop_on_existing=False,
        data_dir=BASE_SETTINGS.data_dir,
        schedule_hour="4",
        schedule_minute="15",
        schedule_timezone="Asia/Kolkata",
    )
    with patch("housing_crs.time.sleep"):
        html = _fetch(type("C", (), {"session": FakeSession(), "use_curl_cffi": False})(), "https://housing.com", retries=2, base_settings=settings)
    assert "__NEXT_DATA__" in html


def test_ensure_index_creates_when_missing(mock_es_client):
    mock_es_client.indices.exists.return_value = False
    ensure_index(mock_es_client, "housing_properties")
    mock_es_client.indices.create.assert_called_once()
    args = mock_es_client.indices.create.call_args
    assert args[1]["index"] == "housing_properties"
    assert args[1]["body"] == ES_INDEX_MAPPING


def test_es_doc_exists_behaviour(mock_es_client):
    mock_es_client.exists.return_value = True
    assert es_doc_exists(mock_es_client, "housing_properties", "abc") is True
    mock_es_client.exists.return_value = False
    assert es_doc_exists(mock_es_client, "housing_properties", "abc") is False


def test_df_to_actions_uses_target_index():
    df = pd.DataFrame(
        [
            {
                "id": "a1",
                "title": "T1",
                "_target_index": "housing_properties",
            },
            {
                "id": "a2",
                "title": "T2",
                "_target_index": "housing_rent_properties",
            },
        ]
    )
    actions = list(df_to_actions(df, default_index=None))
    assert len(actions) == 2
    assert actions[0]["_index"] == "housing_properties"
    assert actions[1]["_index"] == "housing_rent_properties"
    assert actions[0]["_op_type"] == "index"


def test_df_to_actions_raises_without_index():
    df = pd.DataFrame([{"id": "a1", "title": "T1"}])
    try:
        list(df_to_actions(df, default_index=None))
        assert False, "Expected ValueError"
    except ValueError:
        assert True
