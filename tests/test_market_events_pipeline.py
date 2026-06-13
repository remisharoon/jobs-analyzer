import io
import zipfile
from datetime import date
from unittest.mock import patch

from market_events_pipeline import (
    MarketEventsSettings,
    _collect_gdelt_daily_exports,
    _canonical_city,
    _canonical_country,
    _canonical_state,
    _impact_direction_and_score,
    _infer_scope,
    _normalize_event_record,
    _quality_score,
    _source_weight,
    _passes_quality_thresholds,
    index_events_to_es,
)


def _settings() -> MarketEventsSettings:
    return MarketEventsSettings(
        region_key="india_kerala",
        region_name="India-Kerala",
        countries=["India"],
        states=["Kerala"],
        cities=["Kochi", "Thiruvananthapuram", "Kozhikode"],
        localities=[],
        queries=["kochi metro"],
        lookback_days=7,
        max_events_per_source=100,
        max_events_per_run=200,
        request_timeout=20,
        retries=1,
        min_delay=0.0,
        max_delay=0.0,
        es_index="market_events",
        data_dir=None,  # unused by tested helpers
        schedule_hour="6",
        schedule_minute="0",
        schedule_timezone="Asia/Kolkata",
        source_google_news=True,
        source_guardian=True,
        source_worldbank=True,
        source_gdacs=True,
        source_usgs=True,
        source_eonet=False,
        source_gdelt=True,
        source_un_news=True,
        source_federal_reserve=True,
        source_ecb=True,
        source_aljazeera=True,
        source_arabnews=True,
        source_spa=True,
        min_source_credibility="medium",
        min_quality_score=55,
        source_weight_high=1.0,
        source_weight_medium=0.75,
        source_weight_low=0.5,
        guardian_api_key="test",
        worldbank_country_codes=["IN"],
        backfill_start=None,
        backfill_end=None,
        max_backfill_days_per_run=30,
    )


def test_canonical_geo_aliases():
    assert _canonical_city("tvm") == "Thiruvananthapuram"
    assert _canonical_city("calicut") == "Kozhikode"
    assert _canonical_state("eastern province") == "Eastern Province"
    assert _canonical_country("ksa") == "Saudi Arabia"


def test_infer_scope_priority_order():
    assert _infer_scope([], [], [], ["Kakkanad"]) == "locality"
    assert _infer_scope([], [], ["Kochi"], []) == "city"
    assert _infer_scope([], ["Kerala"], [], []) == "state"
    assert _infer_scope(["India"], [], [], []) == "country"
    assert _infer_scope([], [], [], []) == "global"


def test_impact_direction_and_score_classic_cases():
    direction_conflict, score_conflict = _impact_direction_and_score("conflict", "war and missile attack")
    assert direction_conflict == "negative"
    assert score_conflict >= 75

    direction_infra, score_infra = _impact_direction_and_score(
        "infrastructure",
        "new port expansion and metro line approved with strong investment",
    )
    assert direction_infra == "positive"
    assert score_infra >= 70


def test_normalize_event_record_focus_city_match():
    settings = _settings()
    raw = {
        "event_title": "Kochi metro expansion approved",
        "event_summary": "Major infrastructure investment for Kochi in Kerala, India",
        "source_name": "Test Source",
        "source_url": "https://example.com/kochi-metro",
        "source_type": "news",
        "published_at": "2025-01-10T00:00:00Z",
    }

    normalized = _normalize_event_record(raw, settings)
    assert normalized is not None
    assert normalized["event_scope"] == "city"
    assert normalized["event_category"] == "infrastructure"
    assert "Kochi" in normalized["cities"]
    assert normalized["focus_region_match"] is True
    assert normalized["id"]


def test_normalize_event_record_discards_non_focus_general_event():
    settings = _settings()
    raw = {
        "event_title": "Community festival and food fair",
        "event_summary": "A local celebration with music and stalls",
        "source_name": "Local",
        "source_url": "https://example.com/festival",
        "published_at": "2025-02-05T00:00:00Z",
    }

    normalized = _normalize_event_record(raw, settings)
    assert normalized is None


def test_index_events_to_es_indexes_by_id(mock_es_client):
    event = {
        "id": "evt-1",
        "event_title": "Kochi Port Expansion",
        "event_summary": "Major investment",
    }

    with patch("market_events_pipeline.helpers.bulk", return_value=(1, [])) as bulk_mock:
        count = index_events_to_es([event], es=mock_es_client, index="market_events_test")

    assert count == 1
    assert bulk_mock.called


def test_normalize_event_record_accepts_city_alias_tvm():
    settings = _settings()
    raw = {
        "event_title": "TVM metro corridor approved",
        "event_summary": "Large transport investment in Kerala, India",
        "source_name": "Test Source",
        "source_url": "https://example.com/tvm-metro",
        "published_at": "2025-01-11T00:00:00Z",
    }

    normalized = _normalize_event_record(raw, settings)
    assert normalized is not None
    assert "Thiruvananthapuram" in normalized["cities"]
    assert normalized["focus_region_match"] is True


def test_normalize_event_record_id_is_region_scoped():
    kerala = _settings()
    uae = MarketEventsSettings(
        region_key="uae",
        region_name="UAE",
        countries=["UAE"],
        states=["Dubai"],
        cities=["Dubai"],
        localities=[],
        queries=["dubai property"],
        lookback_days=7,
        max_events_per_source=100,
        max_events_per_run=200,
        request_timeout=20,
        retries=1,
        min_delay=0.0,
        max_delay=0.0,
        es_index="market_events",
        data_dir=None,
        schedule_hour="6",
        schedule_minute="0",
        schedule_timezone="Asia/Dubai",
        source_google_news=True,
        source_guardian=True,
        source_worldbank=True,
        source_gdacs=True,
        source_usgs=True,
        source_eonet=False,
        source_gdelt=True,
        source_un_news=True,
        source_federal_reserve=True,
        source_ecb=True,
        source_aljazeera=True,
        source_arabnews=True,
        source_spa=True,
        min_source_credibility="medium",
        min_quality_score=55,
        source_weight_high=1.0,
        source_weight_medium=0.75,
        source_weight_low=0.5,
        guardian_api_key="test",
        worldbank_country_codes=["AE"],
        backfill_start=None,
        backfill_end=None,
        max_backfill_days_per_run=30,
    )

    raw = {
        "event_title": "Global inflation outlook worsens",
        "event_summary": "Global macro-economic stress is rising",
        "source_name": "Test Source",
        "source_url": "https://example.com/global-inflation",
        "published_at": "2025-01-12T00:00:00Z",
    }

    normalized_kerala = _normalize_event_record(raw, kerala)
    normalized_uae = _normalize_event_record(raw, uae)
    assert normalized_kerala is not None
    assert normalized_uae is not None
    assert normalized_kerala["id"] != normalized_uae["id"]


def test_gdelt_collector_uses_url_column_57_for_58_column_rows():
    settings = _settings()

    row = ["" for _ in range(58)]
    row[6] = "Actor 1"
    row[16] = "Actor 2"
    row[29] = "18"
    row[51] = "Kochi"
    row[57] = "https://example.com/gdelt-event"
    tsv = "\t".join(row) + "\n"

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("sample.export.CSV", tsv)

    class _Resp:
        status_code = 200
        content = buf.getvalue()

    class _Session:
        def get(self, *_args, **_kwargs):
            return _Resp()

    events = _collect_gdelt_daily_exports(
        _Session(),
        settings,
        date(2025, 1, 10),
        date(2025, 1, 10),
    )

    assert len(events) == 1
    assert events[0]["source_url"] == "https://example.com/gdelt-event"
    assert events[0]["event_category"] == "conflict"


def test_source_weight_and_quality_score_thresholds():
    settings = _settings()

    high_weight = _source_weight("high", settings)
    medium_weight = _source_weight("medium", settings)
    low_weight = _source_weight("low", settings)
    assert high_weight > medium_weight > low_weight

    quality = _quality_score(80, high_weight)
    assert quality >= 80

    good_record = {"source_credibility": "high", "quality_score": 80}
    weak_record = {"source_credibility": "medium", "quality_score": 20}
    assert _passes_quality_thresholds(good_record, settings) is True
    assert _passes_quality_thresholds(weak_record, settings) is False
