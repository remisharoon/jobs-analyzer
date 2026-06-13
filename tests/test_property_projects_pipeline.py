from urllib.parse import unquote

from property_projects_pipeline import (
    _looks_blocked,
    _normalize_builder_name,
    _activate_settings,
    _is_duckduckgo_listing_url_allowed,
    _name_from_project_url,
    _select_listing_parser,
    parse_aqar_listing,
    parse_housing_listing,
    parse_commonfloor_listing,
    parse_project_detail_page,
    parse_dld_projects_csv,
    normalize_project_record,
)


def test_parse_housing_listing_fixture(housing_html):
    projects = parse_housing_listing(housing_html)
    assert len(projects) == 1
    project = projects[0]
    assert project["project_name"] == "Godrej Kochi Riverside"
    assert project["builder_name"] == "Godrej Properties"
    assert project["price_currency"] == "INR"
    assert "apartment" in project["property_types"]


def test_parse_commonfloor_listing_fixture(commonfloor_html):
    projects = parse_commonfloor_listing(commonfloor_html)
    assert len(projects) == 1
    project = projects[0]
    assert project["project_name"] == "Asset Grandeur"
    assert project["builder_name"] == "Asset Homes"
    assert project["price_min"] == 4500000


def test_parse_project_detail_uses_next_data(project_detail_html):
    detail = parse_project_detail_page(project_detail_html, "housing")
    assert detail["project_name"] == "Prestige Dolphins Court"
    assert detail["price_min"] == 7500000
    assert detail["price_max"] == 12000000
    assert detail["price_currency"] == "INR"
    assert detail["rera_number"] == "K-RERA/123/2025"


def test_parse_dld_projects_csv_and_normalize():
    csv_text = (
        "Project Name,Developer Name,Project Type,Project Status,Completed %,Completion Date,Project Value\n"
        "Harbor Heights,Emaar,Commercial,Active,72,2027-12-31,AED 1.2 Bn\n"
    )
    rows = parse_dld_projects_csv(csv_text)
    assert len(rows) == 1
    row = rows[0]
    assert row["project_name"] == "Harbor Heights"
    assert row["price_currency"] == "AED"
    assert row["completion_percentage"] == 72.0

    normalized = normalize_project_record(row)
    assert normalized["project_name"] == "Harbor Heights"
    assert normalized["country"] == "UAE"
    assert normalized["property_category"] == "commercial"
    assert normalized["id"]


def test_name_from_project_url_decodes_percent_encoded_slug():
    encoded_slug = "%D8%A8%D9%8A%D8%A7%D8%AA-%D9%87%D9%8A%D9%84%D8%B2-%D8%A7%D9%84%D8%B5%D9%81%D8%A7-453"
    url = (
        "https://aqar.fm/"
        "%D8%A7%D9%84%D9%85%D8%B4%D8%A7%D8%B1%D9%8A%D8%B9-%D8%A7%D9%84%D8%B9%D9%82%D8%A7%D8%B1%D9%8A%D8%A9/"
        "%D8%A7%D9%84%D8%B1%D9%8A%D8%A7%D8%B6/"
        f"{encoded_slug}"
    )

    expected = unquote(encoded_slug).replace("-", " ")
    assert _name_from_project_url(url) == expected


def test_parse_aqar_listing_uses_slug_for_noise_anchor_text():
    encoded_prefix = "%D8%A7%D9%84%D9%85%D8%B4%D8%A7%D8%B1%D9%8A%D8%B9-%D8%A7%D9%84%D8%B9%D9%82%D8%A7%D8%B1%D9%8A%D8%A9"
    encoded_city = "%D8%A7%D9%84%D8%B1%D9%8A%D8%A7%D8%B6"
    encoded_slug = "%D8%A8%D9%8A%D8%A7%D8%AA-%D9%87%D9%8A%D9%84%D8%B2-%D8%A7%D9%84%D8%B5%D9%81%D8%A7-453"
    html = (
        "<html><body>"
        "<div>riyadh projects</div>"
        f"<a href=\"/{encoded_prefix}/{encoded_city}/{encoded_slug}\">Copy Link Get More Info</a>"
        "</body></html>"
    )

    _activate_settings("riyadh")
    try:
        projects = parse_aqar_listing(html)
    finally:
        _activate_settings("kochi")

    assert len(projects) == 1
    assert projects[0]["project_name"] == unquote(encoded_slug).replace("-", " ")


def test_extra_source_uaeprojects_filters_cross_city_links():
    html = (
        "<html><body>"
        "<h1>Dubai projects</h1>"
        "<a href=\"/projects/dubai/business-bay/avarra-by-palace\">Avarra by Palace</a>"
        "<a href=\"/projects/abu-dhabi/al-reem-island/joud-residence\">Joud Residence</a>"
        "</body></html>"
    )

    _activate_settings("dubai")
    try:
        parser = _select_listing_parser("https://uaeprojects.com/state/dubai", source_override="extra_sources")
        projects = parser(html)
    finally:
        _activate_settings("kochi")

    assert len(projects) == 1
    assert projects[0]["project_name"] == "Avarra by Palace"
    assert projects[0]["project_url"] == "https://uaeprojects.com/projects/dubai/business-bay/avarra-by-palace"
    assert projects[0]["source"] == "extra_sources"


def test_extra_source_uaeprojects_accepts_rak_numbered_slug_variant():
    html = (
        "<html><body>"
        "<h1>Ras Al Khaimah projects</h1>"
        "<a href=\"/projects/ras-al-khaimah-1/al-hamra-waterfront/al-hamra-waterfront\">Al Hamra Waterfront</a>"
        "</body></html>"
    )

    _activate_settings("ras_al_khaimah")
    try:
        parser = _select_listing_parser("https://uaeprojects.com/state/ras-al-khaimah", source_override="extra_sources")
        projects = parser(html)
    finally:
        _activate_settings("kochi")

    assert len(projects) == 1
    assert projects[0]["project_name"] == "Al Hamra Waterfront"
    assert projects[0]["project_url"] == "https://uaeprojects.com/projects/ras-al-khaimah-1/al-hamra-waterfront/al-hamra-waterfront"


def test_extra_source_mada_uses_anchor_title_for_clean_name():
    html = (
        "<html><body>"
        "<a href=\"/ar/projects/al-adwan-tower\" title=\"برج العدوان\">"
        "على الخارطةتجاريمتاحموعد التسليم: 2027 - الربع 4برج العدوان الرياض - النخيلمكتب السعر المبدئي:"
        "</a>"
        "</body></html>"
    )

    _activate_settings("riyadh")
    try:
        parser = _select_listing_parser("https://madaproperties.sa/projects", source_override="extra_sources")
        projects = parser(html)
    finally:
        _activate_settings("kochi")

    assert len(projects) == 1
    assert projects[0]["project_name"] == "برج العدوان"
    assert projects[0]["project_url"] == "https://madaproperties.sa/ar/projects/al-adwan-tower"
    assert projects[0]["source"] == "extra_sources"


def test_duckduckgo_host_allowlist_blocks_non_allowed_hosts_and_allows_subdomains():
    _activate_settings("dubai")
    try:
        assert _is_duckduckgo_listing_url_allowed("https://www.propertyfinder.ae/en/new-projects/lp/dubai") is True
        assert _is_duckduckgo_listing_url_allowed("https://sub.uaeprojects.com/projects/dubai/test-project") is True
        assert _is_duckduckgo_listing_url_allowed("https://example.com/projects/dubai/test-project") is False
    finally:
        _activate_settings("kochi")


def test_normalize_builder_name_strips_realestateindia_suffix_noise():
    raw = "Artech Realtors Pvt. Ltd. - RealEstateIndia.Com Thiruvananthapuram Search from Ov"
    assert _normalize_builder_name(raw) == "Artech Realtors Pvt. Ltd."


def test_parse_commonfloor_detail_builder_name_rejects_noise_phrase():
    html = """
    <html>
      <head><title>Artech Marvel</title></head>
      <body>
        <p>This project, spread over 0.58 acres, is developed by and Lift fascia.</p>
      </body>
    </html>
    """
    detail = parse_project_detail_page(html, "commonfloor")
    assert "builder_name" not in detail


def test_name_from_project_url_strips_housing_prjid_suffix():
    url = "https://housing.com/in/projects/kochi/skyline-habitat-prjid-12345"
    assert _name_from_project_url(url) == "Skyline Habitat"


def test_looks_blocked_allows_housing_marker_with_soft_marker():
    html = "captcha protection housing __NEXT_DATA__"
    assert _looks_blocked(html) is False
