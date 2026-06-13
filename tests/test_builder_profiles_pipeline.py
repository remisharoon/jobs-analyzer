from builder_profiles_pipeline import (
    _apply_domain_guess_enrichment,
    _city_query_variants,
    _dedupe_discovery_results,
    _builder_name_from_domain,
    _domain_guess_urls_for_builder,
    _discover_city_web_results,
    _extract_builder_names_from_text,
    _prioritize_city_discovery_results,
    _score_city_discovery_result,
    _seed_priority_score,
    _looks_like_builder_name,
    _normalize_builder_name,
    _extract_realestateindia_load_more_payload,
    _is_realestateindia_builder_city_match,
    _realestateindia_city_slug,
    _realestateindia_city_slug_candidates,
    parse_duckduckgo_results,
    parse_realestateindia_builder_results,
)
from unittest.mock import Mock


class TestRealEstateIndiaBuilderParsing:
    def test_parse_builder_cards(self):
        html = """
        <ul id="service_classified_results">
          <li>
            <div class="agent_item pr_list" data-url="https://www.realestateindia.com/profile/pvu-developers-in-kochi-3654255/">
              <div class="ag_info">
                <div class="ag_name">
                  <a href="/profile/pvu-developers-in-kochi-3654255/"><h2>PVU Developers</h2></a>
                </div>
                <div class="ag_location"><div class="location-dc">Kochi</div></div>
              </div>
              <div class="cssc-about">Trusted developers in Kochi with premium projects.</div>
            </div>
          </li>
          <li>
            <div class="agent_item pr_list" data-url="https://www.realestateindia.com/profile/classic-homes-in-kakkanad-kochi-3461408/">
              <div class="ag_info">
                <div class="ag_name">
                  <a href="https://www.realestateindia.com/profile/classic-homes-in-kakkanad-kochi-3461408/"><h2>CLASSIC HOMES</h2></a>
                </div>
                <div class="ag_location"><div class="location-dc">Kakkanad Kochi</div></div>
              </div>
              <div class="cssc-about">29-year legacy of crafting exceptional living spaces.</div>
            </div>
          </li>
        </ul>
        """
        results = parse_realestateindia_builder_results(html)
        assert len(results) == 2
        assert results[0]["builder_name"] == "PVU Developers"
        assert results[0]["url"] == "https://www.realestateindia.com/profile/pvu-developers-in-kochi-3654255/"
        assert "Kochi" in results[0]["snippet"]
        assert results[1]["builder_name"] == "CLASSIC HOMES"
        assert results[1]["source"] == "realestateindia_builders"

    def test_extract_load_more_payload(self):
        listing_url = "https://www.realestateindia.com/builders-developers-in-kochi.htm"
        html = """
        <script>
        $.post('/Functions/fetch_service_classified_results.php',
          {'pageno': track_click,'city_level':'2','cat_id':'109','city_id':'6655',
           'file_name':'kochi','ref_state_file_name':'kerala','ref_city_file_name':'kochi',
           'solr_rand_no':'876925778','people_also_search_for':'','people_also_search_cat':''},
          function(data){
            $('#service_classified_results').append(data);
          }
        );
        </script>
        """
        payload = _extract_realestateindia_load_more_payload(html, listing_url)
        assert payload is not None
        assert payload["city_level"] == "2"
        assert payload["city_id"] == "6655"
        assert payload["location"] == listing_url
        assert payload["people_also_search_for"] == ""
        assert payload["people_also_search_cat"] == ""

    def test_city_slug_override(self):
        assert _realestateindia_city_slug("Bengaluru") == "bangalore"
        assert _realestateindia_city_slug("Kochi") == "kochi"
        assert _realestateindia_city_slug("Thiruvananthapuram") == "thiruvananthapuram"
        assert _realestateindia_city_slug("Kozhikode") == "calicut"

    def test_city_slug_candidates_include_fallback_aliases(self):
        assert _realestateindia_city_slug_candidates("Thiruvananthapuram") == ["thiruvananthapuram", "trivandrum"]
        assert _realestateindia_city_slug_candidates("Kozhikode") == ["calicut", "kozhikode"]

    def test_city_match_filter(self):
        kochi_result = {
            "builder_name": "PVU Developers",
            "url": "https://www.realestateindia.com/profile/pvu-developers-in-kochi-3654255/",
            "snippet": "Kochi trusted developers",
        }
        out_of_city_result = {
            "builder_name": "Jain Housing",
            "url": "https://www.realestateindia.com/profile/jain-housing-constructions-ltd-in-t-nagar-chennai-3719443/",
            "snippet": "T Nagar Chennai",
        }
        assert _is_realestateindia_builder_city_match(kochi_result, "Kochi") is True
        assert _is_realestateindia_builder_city_match(out_of_city_result, "Kochi") is False

    def test_city_match_rejects_also_deals_in_entries(self):
        result = {
            "builder_name": "External Builder",
            "url": "https://www.realestateindia.com/profile/external-builder-in-chennai-111/",
            "location": "Chennai (also deals in Kochi)",
        }
        assert _is_realestateindia_builder_city_match(result, "Kochi") is False

    def test_parse_builder_cards_extracts_website(self):
        html = """
        <ul id="service_classified_results">
          <li>
            <div class="agent_item pr_list" data-url="https://www.realestateindia.com/profile/classic-homes-in-kakkanad-kochi-3461408/">
              <div class="ag_info">
                <div class="ag_name">
                  <a href="https://www.realestateindia.com/profile/classic-homes-in-kakkanad-kochi-3461408/"><h2>CLASSIC HOMES</h2></a>
                </div>
                <div class="ag_location"><div class="location-dc">Kakkanad Kochi</div></div>
              </div>
              <div class="agent_footer">
                <a onclick="window.open('https://www.classichomes.in/','_blank');" class="web_link"><span>https://www.classichomes.in/</span></a>
              </div>
            </div>
          </li>
        </ul>
        """
        results = parse_realestateindia_builder_results(html)
        assert len(results) == 1
        assert results[0]["website"] == "https://www.classichomes.in/"


class TestDiscoveryPrioritization:
    def test_score_city_discovery_prefers_builder_signals(self):
        strong = {
            "title": "Classic Homes Official Website",
            "snippet": "Top real estate builder in Kochi",
            "url": "https://www.classichomes.in/",
            "source": "duckduckgo",
        }
        noisy = {
            "title": "BEST definition and meaning",
            "snippet": "Dictionary result",
            "url": "https://dictionary.cambridge.org/dictionary/english/best",
            "source": "bing_city",
        }
        assert _score_city_discovery_result(strong, "Kochi") > _score_city_discovery_result(noisy, "Kochi")

    def test_prioritize_city_discovery_orders_by_score(self):
        results = [
            {
                "title": "BEST definition and meaning",
                "snippet": "Dictionary",
                "url": "https://dictionary.cambridge.org/dictionary/english/best",
                "source": "bing_city",
            },
            {
                "title": "Classic Homes Official Website",
                "snippet": "Real estate developers in Kochi",
                "url": "https://www.classichomes.in/",
                "source": "duckduckgo",
            },
        ]
        prioritized = _prioritize_city_discovery_results(results, "Kochi", limit=5)
        assert prioritized
        assert prioritized[0]["url"] == "https://www.classichomes.in/"

    def test_seed_priority_prefers_richer_profile(self):
        weak = {
            "builder_name": "Weak Builder",
            "total_projects": 0,
            "source_names": ["duckduckgo"],
            "known_websites": [],
            "sources": ["https://example.com/listing"],
        }
        strong = {
            "builder_name": "Strong Builder",
            "total_projects": 3,
            "source_names": ["property_projects", "realestateindia_builders"],
            "known_websites": ["https://strongbuilder.com/"],
            "rera_numbers": ["K-RERA/123/2025"],
            "operating_cities": ["Kochi"],
            "sources": ["https://strongbuilder.com/", "https://www.realestateindia.com/profile/strong-1/"],
            "data_quality": "medium",
        }
        assert _seed_priority_score(strong) > _seed_priority_score(weak)

    def test_city_query_variants_include_aliases(self):
        assert _city_query_variants("Thiruvananthapuram") == ["Thiruvananthapuram", "Trivandrum"]
        assert _city_query_variants("Kozhikode") == ["Kozhikode", "Calicut"]

    def test_domain_guess_urls_for_builder(self):
        guesses = _domain_guess_urls_for_builder("CLASSIC HOMES")
        lowered = {url.lower() for url in guesses}
        assert "https://www.classichomes.in/" in lowered
        assert any(url.endswith(".com/") for url in lowered)


class TestCitySearchFallback:
    def test_discover_city_web_results_uses_fallback_when_ddg_is_weak(self):
        session = Mock()
        html_ddg = """
        <a class="result__a" href="https://dictionary.cambridge.org/dictionary/english/best">Best</a>
        """
        html_brave = """
        <a href="https://www.classichomes.in/" class="l1">Classic Homes Official Website</a>
        """
        rss = """<?xml version=\"1.0\"?><rss><channel>
            <item><title>Classic Homes</title><link>https://www.classichomes.in/</link><description>builder in kochi</description></item>
        </channel></rss>"""

        from builder_profiles_pipeline import _fetch

        def fake_fetch(_session, url, **kwargs):
            if "duckduckgo" in url:
                return html_ddg
            if "format=rss" in url:
                return rss
            if "search.brave.com" in url:
                return html_brave
            return ""

        import builder_profiles_pipeline as bp

        orig_fetch = bp._fetch
        bp._fetch = fake_fetch
        try:
            results = _discover_city_web_results(session, "Kochi", "top real estate builders in Kochi", 0)
        finally:
            bp._fetch = orig_fetch

        assert results
        assert any("classichomes.in" in (item.get("url") or "") for item in results)

    def test_dedupe_discovery_results_prefers_unique_urls(self):
        results = [
            {"title": "A", "url": "https://www.classichomes.in", "source": "duckduckgo"},
            {"title": "B", "url": "https://www.classichomes.in/", "source": "bing_city"},
        ]
        deduped = _dedupe_discovery_results(results)
        assert len(deduped) == 1

    def test_apply_domain_guess_enrichment_adds_guess_for_rei_candidates(self):
        import builder_profiles_pipeline as bp

        class FakeResponse:
            def __init__(self, status_code: int, url: str):
                self.status_code = status_code
                self.url = url

        original_session_get = bp._session_get
        original_max_lookups = bp.DOMAIN_GUESS_MAX_LOOKUPS_PER_RUN
        bp.DOMAIN_GUESS_MAX_LOOKUPS_PER_RUN = 5

        def fake_session_get(_session, url, timeout):
            if "classichomes.in" in url:
                return FakeResponse(200, "https://www.classichomes.in/")
            return FakeResponse(404, url)

        bp._session_get = fake_session_get
        try:
            discovered = [
                {
                    "builder_name": "CLASSIC HOMES",
                    "source_names": ["realestateindia_builders"],
                    "known_websites": [],
                    "sources": ["https://www.realestateindia.com/profile/classic-homes-in-kakkanad-kochi-3461408/"],
                    "operating_cities": ["Kochi"],
                    "main_operating_city": "Kochi",
                    "_discovery_order": 1,
                },
                {
                    "builder_name": "Already Known",
                    "source_names": ["realestateindia_builders"],
                    "known_websites": ["https://alreadyknown.example/"],
                    "sources": [],
                    "operating_cities": ["Kochi"],
                    "main_operating_city": "Kochi",
                    "_discovery_order": 2,
                },
            ]

            _apply_domain_guess_enrichment(Mock(), discovered)
        finally:
            bp._session_get = original_session_get
            bp.DOMAIN_GUESS_MAX_LOOKUPS_PER_RUN = original_max_lookups

        assert "domain_guess" in (discovered[0].get("source_names") or [])
        assert "https://www.classichomes.in/" in (discovered[0].get("known_websites") or [])
        assert "domain_guess" not in (discovered[1].get("source_names") or [])


class TestBuilderNameHeuristics:
    def test_extract_builder_names_supports_realtors_suffix(self):
        text = "Artech Marvel by Artech Realtors Pvt. Ltd. in Trivandrum"
        names = _extract_builder_names_from_text(text)
        assert any("Artech Realtors" in name for name in names)

    def test_builder_name_from_domain_supports_realtors(self):
        assert _builder_name_from_domain("https://artechrealtors.com/") == "Artech Realtors"

    def test_normalize_builder_name_strips_portal_tail_text(self):
        raw = "Artech Realtors Pvt. Ltd. - RealEstateIndia.Com Thiruvananthapuram Search from Ov"
        assert _normalize_builder_name(raw) == "Artech Realtors Pvt. Ltd."

    def test_normalize_builder_name_strips_leading_renowned_phrase(self):
        raw = "the renowned Artech Realtors Pvt. Ltd."
        assert _normalize_builder_name(raw) == "Artech Realtors Pvt. Ltd."

    def test_normalize_builder_name_rejects_commonfloor_spec_text(self):
        assert _normalize_builder_name("and Lift fascia") is None
        assert _looks_like_builder_name("ELCB and MCB", strict=True) is False
        assert _normalize_builder_name("ELCB and MCBs with independent KSEB meters. Lifts") is None

    def test_looks_like_builder_name_still_accepts_valid_names(self):
        assert _looks_like_builder_name("Artech Realtors Pvt. Ltd.", strict=True) is True
