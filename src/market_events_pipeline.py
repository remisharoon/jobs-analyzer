"""Market events pipeline for real-estate impact monitoring.

Collects major global/country/state/city/locality events that may influence
property markets across focus regions:
- UAE (all cities)
- KSA (major cities)
- India (Kochi, Thiruvananthapuram, Kozhikode)

Data is normalized and indexed into a dedicated Elasticsearch index.
"""

from __future__ import annotations

import csv
import hashlib
import html
import io
import json
import logging
import random
import re
import time
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote_plus, urlparse

import numpy as np
import pandas as pd
import requests
from apscheduler.triggers.cron import CronTrigger
from elasticsearch import Elasticsearch, helpers
from pydantic import BaseModel
from plombery import Trigger, register_pipeline, task

from config import read_config


logger = logging.getLogger(__name__)


DEFAULT_EVENTS_INDEX = "market_events"
CONFIG_SECTION_GLOBAL = "market_events"
CONFIG_SECTION_PREFIX = "market_events."

DEFAULT_UAE_CITIES = [
    "Dubai",
    "Abu Dhabi",
    "Sharjah",
    "Ajman",
    "Ras Al Khaimah",
    "Fujairah",
    "Umm Al Quwain",
    "Al Ain",
]

DEFAULT_KSA_CITIES = ["Riyadh", "Jeddah", "Dammam", "Mecca", "Medina"]

DEFAULT_KERALA_CITIES = ["Kochi", "Thiruvananthapuram", "Kozhikode"]

MAINSTREAM_SOURCE_HIGH_CRED = {
    "reuters.com",
    "apnews.com",
    "bloomberg.com",
    "ft.com",
    "wsj.com",
    "thehindu.com",
    "economictimes.indiatimes.com",
    "thenationalnews.com",
    "khaleejtimes.com",
    "gulfnews.com",
    "zawya.com",
    "aljazeera.com",
    "arabnews.com",
    "news.un.org",
    "federalreserve.gov",
    "ecb.europa.eu",
}

CITY_ALIASES = {
    "tvm": "Thiruvananthapuram",
    "trivandrum": "Thiruvananthapuram",
    "thiruvananthapuram": "Thiruvananthapuram",
    "calicut": "Kozhikode",
    "kozhikode": "Kozhikode",
    "kochi": "Kochi",
    "cochin": "Kochi",
    "riyadh": "Riyadh",
    "jeddah": "Jeddah",
    "dammam": "Dammam",
    "makkah": "Mecca",
    "mecca": "Mecca",
    "madinah": "Medina",
    "medina": "Medina",
    "abu dhabi": "Abu Dhabi",
    "dubai": "Dubai",
    "sharjah": "Sharjah",
    "ajman": "Ajman",
    "ras al khaimah": "Ras Al Khaimah",
    "fujairah": "Fujairah",
    "umm al quwain": "Umm Al Quwain",
    "al ain": "Al Ain",
}

STATE_ALIASES = {
    "kerala": "Kerala",
    "makkah": "Makkah",
    "riyadh": "Riyadh",
    "eastern province": "Eastern Province",
    "abu dhabi": "Abu Dhabi",
    "dubai": "Dubai",
}

COUNTRY_ALIASES = {
    "uae": "UAE",
    "u.a.e": "UAE",
    "united arab emirates": "UAE",
    "ksa": "Saudi Arabia",
    "saudi": "Saudi Arabia",
    "saudi arabia": "Saudi Arabia",
    "india": "India",
}

EVENT_CATEGORY_RULES: dict[str, tuple[str, ...]] = {
    "conflict": (
        "war",
        "conflict",
        "military",
        "invasion",
        "missile",
        "attack",
        "sanction",
        "geopolitical tension",
        "ceasefire",
    ),
    "mega_event": (
        "fifa",
        "world cup",
        "expo",
        "olympics",
        "asian games",
        "summit",
        "cop28",
    ),
    "infrastructure": (
        "port",
        "airport",
        "metro",
        "rail",
        "highway",
        "bridge",
        "corridor",
        "logistics hub",
        "free zone",
        "industrial city",
    ),
    "investment": (
        "investment",
        "fdi",
        "funding",
        "capital",
        "project financing",
        "megaproject",
        "construction contract",
        "real estate development",
    ),
    "policy": (
        "policy",
        "regulation",
        "visa",
        "residency",
        "tax",
        "rate cut",
        "interest rate",
        "zoning",
        "land law",
    ),
    "disaster": (
        "earthquake",
        "flood",
        "cyclone",
        "hurricane",
        "wildfire",
        "drought",
        "tsunami",
    ),
    "macro_economic": (
        "inflation",
        "recession",
        "gdp",
        "unemployment",
        "currency",
        "oil prices",
        "central bank",
    ),
}

NEGATIVE_TOKENS = {
    "war",
    "conflict",
    "attack",
    "sanction",
    "flood",
    "earthquake",
    "disaster",
    "recession",
    "inflation",
    "protest",
    "strike",
    "crisis",
}

POSITIVE_TOKENS = {
    "investment",
    "expansion",
    "approval",
    "launch",
    "inaugurated",
    "new project",
    "funding",
    "growth",
    "expo",
    "world cup",
    "new port",
    "metro line",
}

REQUEST_HEADERS = {
    "Accept": "application/json,text/plain,text/html,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.6422.78 Safari/537.36"
    ),
}

ES_INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "5s",
    },
    "mappings": {
        "dynamic": True,
        "dynamic_templates": [
            {
                "dates_iso": {
                    "match": "*_at",
                    "mapping": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
                }
            },
            {"dates": {"match": "*_date", "mapping": {"type": "date", "format": "strict_date_optional_time||yyyy-MM-dd"}}},
            {"strings": {"match_mapping_type": "string", "mapping": {"type": "keyword", "ignore_above": 512}}},
            {"doubleNums": {"match_mapping_type": "double", "mapping": {"type": "double"}}},
            {"longNums": {"match_mapping_type": "long", "mapping": {"type": "long"}}},
        ],
        "properties": {
            "id": {"type": "keyword"},
            "event_title": {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}},
            "event_summary": {"type": "text"},
            "event_scope": {"type": "keyword"},
            "event_category": {"type": "keyword"},
            "impact_direction": {"type": "keyword"},
            "impact_score": {"type": "long"},
            "countries": {"type": "keyword"},
            "states": {"type": "keyword"},
            "cities": {"type": "keyword"},
            "localities": {"type": "keyword"},
            "focus_region_match": {"type": "boolean"},
            "focus_market": {"type": "keyword"},
            "event_start_date": {"type": "date", "format": "strict_date_optional_time||yyyy-MM-dd"},
            "event_end_date": {"type": "date", "format": "strict_date_optional_time||yyyy-MM-dd"},
            "published_at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
            "source_name": {"type": "keyword"},
            "source_url": {"type": "keyword"},
            "source_type": {"type": "keyword"},
            "source_credibility": {"type": "keyword"},
            "source_weight": {"type": "double"},
            "quality_score": {"type": "long"},
            "discovered_at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
            "updated_at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
            "source_tags": {"type": "keyword"},
        },
    },
}


@dataclass(slots=True)
class MarketEventsSettings:
    region_key: str
    region_name: str
    countries: list[str]
    states: list[str]
    cities: list[str]
    localities: list[str]
    queries: list[str]
    lookback_days: int
    max_events_per_source: int
    max_events_per_run: int
    request_timeout: int
    retries: int
    min_delay: float
    max_delay: float
    es_index: str
    data_dir: Path
    schedule_hour: str
    schedule_minute: str
    schedule_timezone: str
    source_google_news: bool
    source_guardian: bool
    source_worldbank: bool
    source_gdacs: bool
    source_usgs: bool
    source_eonet: bool
    source_gdelt: bool
    source_un_news: bool
    source_federal_reserve: bool
    source_ecb: bool
    source_aljazeera: bool
    source_arabnews: bool
    source_spa: bool
    min_source_credibility: str
    min_quality_score: int
    source_weight_high: float
    source_weight_medium: float
    source_weight_low: float
    guardian_api_key: str
    worldbank_country_codes: list[str]
    backfill_start: date
    backfill_end: date
    max_backfill_days_per_run: int


DEFAULT_REGION_SETTINGS: dict[str, dict[str, Any]] = {
    "uae": {
        "region_name": "UAE",
        "countries": ["UAE"],
        "states": ["Dubai", "Abu Dhabi", "Sharjah", "Ajman", "Ras Al Khaimah", "Fujairah", "Umm Al Quwain"],
        "cities": DEFAULT_UAE_CITIES,
        "localities": [],
        "queries": [
            "UAE real estate market investment",
            "Dubai new port project",
            "Abu Dhabi infrastructure investment",
            "UAE mega event expo fifa bid",
            "UAE visa policy property market",
        ],
        "worldbank_country_codes": ["AE"],
        "schedule_timezone": "Asia/Dubai",
        "schedule_hour": "6",
        "schedule_minute": "0",
    },
    "ksa": {
        "region_name": "KSA",
        "countries": ["Saudi Arabia"],
        "states": ["Riyadh", "Makkah", "Eastern Province", "Medina"],
        "cities": DEFAULT_KSA_CITIES,
        "localities": [],
        "queries": [
            "Riyadh real estate investment",
            "Saudi Arabia mega project housing",
            "Riyadh Expo development",
            "Jeddah port expansion project",
            "Saudi policy mortgage real estate",
        ],
        "worldbank_country_codes": ["SA"],
        "schedule_timezone": "Asia/Riyadh",
        "schedule_hour": "6",
        "schedule_minute": "25",
    },
    "india_kerala": {
        "region_name": "India-Kerala",
        "countries": ["India"],
        "states": ["Kerala"],
        "cities": DEFAULT_KERALA_CITIES,
        "localities": [],
        "queries": [
            "Kochi major investment project",
            "Thiruvananthapuram infrastructure project",
            "Calicut Kozhikode development project",
            "Kerala real estate policy",
            "Kochi port airport expansion",
        ],
        "worldbank_country_codes": ["IN"],
        "schedule_timezone": "Asia/Kolkata",
        "schedule_hour": "6",
        "schedule_minute": "50",
    },
}


def _slug(value: str | None) -> str:
    text = (value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text or "default"


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        txt = re.sub(r"\s+", " ", value).strip()
        return txt or None
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        if not value:
            return None
        return _as_text(value[0])
    if isinstance(value, dict):
        for key in ("name", "title", "value", "label", "text"):
            if key in value and value[key] not in (None, "", []):
                return _as_text(value[key])
    return re.sub(r"\s+", " ", str(value)).strip() or None


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _parse_date(value: str | None, default: date) -> date:
    text = _as_text(value)
    if not text:
        return default
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            continue
    return default


def _parse_datetime(value: Any) -> datetime | None:
    text = _as_text(value)
    if not text:
        return None
    text = text.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    try:
        dt = parsedate_to_datetime(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _canonical_country(text: str | None) -> str | None:
    raw = _as_text(text)
    if not raw:
        return None
    key = raw.lower()
    if key in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[key]
    return raw.title()


def _canonical_state(text: str | None) -> str | None:
    raw = _as_text(text)
    if not raw:
        return None
    key = raw.lower()
    if key in STATE_ALIASES:
        return STATE_ALIASES[key]
    return raw.title()


def _canonical_city(text: str | None) -> str | None:
    raw = _as_text(text)
    if not raw:
        return None
    key = raw.lower()
    if key in CITY_ALIASES:
        return CITY_ALIASES[key]
    return raw.title()


def _uniq(values: Iterable[str | None]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        txt = _as_text(value)
        if not txt:
            continue
        key = txt.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(txt)
    return out


def _event_category_from_text(text: str) -> str:
    lowered = text.lower()
    for category, markers in EVENT_CATEGORY_RULES.items():
        if any(marker in lowered for marker in markers):
            return category
    return "general"


def _impact_direction_and_score(category: str, text: str) -> tuple[str, int]:
    lowered = text.lower()
    pos_hits = sum(1 for token in POSITIVE_TOKENS if token in lowered)
    neg_hits = sum(1 for token in NEGATIVE_TOKENS if token in lowered)

    if category in {"conflict", "disaster"}:
        direction = "negative"
        base = 78
    elif category in {"investment", "infrastructure", "mega_event"}:
        direction = "positive"
        base = 68
    elif category in {"policy", "macro_economic"}:
        direction = "mixed"
        base = 58
    else:
        direction = "unknown"
        base = 45

    if pos_hits > neg_hits + 1:
        direction = "positive"
    elif neg_hits > pos_hits + 1:
        direction = "negative"
    elif pos_hits or neg_hits:
        direction = "mixed"

    score = base + (pos_hits * 4) + (neg_hits * 4)
    score = max(15, min(95, score))
    return direction, score


def _source_credibility(source_type: str, source_url: str) -> str:
    if source_type in {"gov", "open_data"}:
        return "high"
    host = (urlparse(source_url).netloc or "").lower()
    host = host[4:] if host.startswith("www.") else host
    if host in MAINSTREAM_SOURCE_HIGH_CRED:
        return "high"
    return "medium"


def _source_weight(credibility: str, settings: MarketEventsSettings) -> float:
    level = (credibility or "").strip().lower()
    if level == "high":
        return settings.source_weight_high
    if level == "low":
        return settings.source_weight_low
    return settings.source_weight_medium


def _quality_score(impact_score: int, source_weight: float) -> int:
    # Weighted quality score keeps impact relevance while preferring stronger feeds.
    score = int(round((impact_score * 0.75) + (source_weight * 100.0 * 0.25)))
    return max(0, min(100, score))


def _passes_quality_thresholds(record: dict[str, Any], settings: MarketEventsSettings) -> bool:
    credibility_rank = {"low": 0, "medium": 1, "high": 2}
    min_level = (settings.min_source_credibility or "medium").strip().lower()
    min_rank = credibility_rank.get(min_level, 1)

    source_rank = credibility_rank.get((_as_text(record.get("source_credibility")) or "").lower(), 1)
    if source_rank < min_rank:
        return False

    quality_score = int(record.get("quality_score") or 0)
    return quality_score >= settings.min_quality_score


def _extract_geo_from_text(text: str, settings: MarketEventsSettings) -> tuple[list[str], list[str], list[str], list[str]]:
    lowered = text.lower()
    countries: list[str] = []
    states: list[str] = []
    cities: list[str] = []
    localities: list[str] = []

    for country in settings.countries:
        if country.lower() in lowered:
            countries.append(country)

    for state in settings.states:
        if state.lower() in lowered:
            states.append(state)

    for city in settings.cities:
        if city.lower() in lowered:
            cities.append(city)

    # Alias-aware extraction for common geo variants.
    for alias, canonical in CITY_ALIASES.items():
        if canonical in settings.cities and alias in lowered:
            cities.append(canonical)

    for alias, canonical in STATE_ALIASES.items():
        if canonical in settings.states and alias in lowered:
            states.append(canonical)

    for alias, canonical in COUNTRY_ALIASES.items():
        if canonical in settings.countries and alias in lowered:
            countries.append(canonical)

    # Very light locality inference from "in <place>" pattern.
    for m in re.finditer(r"\bin\s+([a-zA-Z][a-zA-Z\-\s]{2,40})", text):
        candidate = _as_text(m.group(1))
        if not candidate:
            continue
        cand_low = candidate.lower()
        if any(cand_low == c.lower() for c in cities):
            continue
        if any(cand_low == s.lower() for s in states):
            continue
        if any(cand_low == c.lower() for c in countries):
            continue
        localities.append(candidate.title())

    return _uniq(countries), _uniq(states), _uniq(cities), _uniq(localities)


def _infer_scope(countries: list[str], states: list[str], cities: list[str], localities: list[str]) -> str:
    if localities:
        return "locality"
    if cities:
        return "city"
    if states:
        return "state"
    if countries:
        return "country"
    return "global"


def _normalize_event_record(raw: dict[str, Any], settings: MarketEventsSettings) -> dict[str, Any] | None:
    title = _as_text(raw.get("event_title") or raw.get("title"))
    if not title:
        return None

    summary = _as_text(raw.get("event_summary") or raw.get("summary") or "") or ""
    source_url = _as_text(raw.get("source_url") or raw.get("url")) or ""
    source_name = _as_text(raw.get("source_name") or raw.get("source") or "unknown") or "unknown"
    source_type = (_as_text(raw.get("source_type")) or "news").lower()

    published_dt = _parse_datetime(raw.get("published_at") or raw.get("published") or raw.get("pub_date"))
    if published_dt is None:
        published_dt = datetime.now(timezone.utc)

    event_start = _parse_date(_as_text(raw.get("event_start_date")), published_dt.date())
    event_end = _parse_date(_as_text(raw.get("event_end_date")), event_start)

    text_blob = f"{title}\n{summary}"
    countries, states, cities, localities = _extract_geo_from_text(text_blob, settings)

    countries = _uniq([_canonical_country(c) for c in countries])
    states = _uniq([_canonical_state(s) for s in states])
    cities = _uniq([_canonical_city(c) for c in cities])
    localities = _uniq(localities)

    category = (_as_text(raw.get("event_category")) or "").lower() or _event_category_from_text(text_blob)
    scope = (_as_text(raw.get("event_scope")) or "").lower() or _infer_scope(countries, states, cities, localities)
    impact_direction, impact_score = _impact_direction_and_score(category, text_blob)
    source_credibility = _source_credibility(source_type, source_url)
    source_weight = _source_weight(source_credibility, settings)
    quality_score = _quality_score(impact_score, source_weight)

    # Keep major global events even without explicit location. Otherwise require
    # overlap with focus geography.
    if scope == "global":
        focus_match = category in {"conflict", "macro_economic", "mega_event", "policy", "disaster"}
    else:
        focus_match = bool(
            set(countries).intersection(settings.countries)
            or set(states).intersection(settings.states)
            or set(cities).intersection(settings.cities)
            or (settings.localities and set(localities).intersection(settings.localities))
        )

    if not focus_match:
        return None

    key_payload = "|".join(
        [
            settings.region_key,
            title.strip().lower(),
            event_start.isoformat(),
            (cities[0] if cities else (states[0] if states else (countries[0] if countries else "global"))).lower(),
            source_url.strip().lower(),
        ]
    )
    doc_id = hashlib.sha1(key_payload.encode("utf-8")).hexdigest()
    now_iso = datetime.now(timezone.utc).isoformat()

    record = {
        "id": doc_id,
        "event_title": title,
        "event_summary": summary,
        "event_scope": scope,
        "event_category": category,
        "impact_direction": impact_direction,
        "impact_score": impact_score,
        "countries": countries,
        "states": states,
        "cities": cities,
        "localities": localities,
        "focus_region_match": True,
        "focus_market": settings.region_name,
        "event_start_date": event_start.isoformat(),
        "event_end_date": event_end.isoformat(),
        "published_at": published_dt.isoformat(),
        "source_name": source_name,
        "source_url": source_url,
        "source_type": source_type,
        "source_credibility": source_credibility,
        "source_weight": source_weight,
        "quality_score": quality_score,
        "discovered_at": _as_text(raw.get("discovered_at")) or now_iso,
        "updated_at": now_iso,
        "source_tags": _uniq([_as_text(raw.get("source_tag")), _as_text(raw.get("source_name")), category]),
        "_target_region": settings.region_key,
    }
    if not _passes_quality_thresholds(record, settings):
        return None
    return record


def _rss_local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _strip_html(value: str) -> str:
    text = html.unescape(value or "")
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _fetch_text(session: requests.Session, url: str, *, timeout: int, retries: int, min_delay: float, max_delay: float) -> str | None:
    for attempt in range(retries):
        try:
            response = session.get(url, headers=REQUEST_HEADERS, timeout=timeout)
            if response.status_code == 200:
                return response.text
            logger.warning("Fetch status=%s url=%s", response.status_code, url)
        except Exception as exc:
            logger.warning("Fetch error attempt=%d url=%s err=%s", attempt + 1, url, exc)
        time.sleep(random.uniform(min_delay, max_delay))
    return None


def _collect_google_news(
    session: requests.Session,
    settings: MarketEventsSettings,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not settings.source_google_news:
        return events

    # Google expects before date as exclusive upper bound.
    before = (end_date + timedelta(days=1)).isoformat()

    for query in settings.queries:
        q = f"{query} after:{start_date.isoformat()} before:{before}"
        rss_url = (
            "https://news.google.com/rss/search?"
            f"q={quote_plus(q)}&hl=en-US&gl=US&ceid=US:en"
        )
        xml_text = _fetch_text(
            session,
            rss_url,
            timeout=settings.request_timeout,
            retries=settings.retries,
            min_delay=settings.min_delay,
            max_delay=settings.max_delay,
        )
        if not xml_text:
            continue

        try:
            root = ET.fromstring(xml_text)
        except Exception as exc:
            logger.warning("Failed to parse Google News RSS for query=%s: %s", query, exc)
            continue

        for item in root.findall("./channel/item"):
            title = _as_text(item.findtext("title"))
            link = _as_text(item.findtext("link"))
            pub_date = _as_text(item.findtext("pubDate"))
            description = _strip_html(_as_text(item.findtext("description")) or "")
            source_node = item.find("source")
            source_name = _as_text(source_node.text) if source_node is not None else "Google News"

            if not title:
                continue

            events.append(
                {
                    "event_title": title,
                    "event_summary": description,
                    "source_name": source_name or "Google News",
                    "source_url": link or "",
                    "source_type": "news",
                    "published_at": pub_date,
                    "source_tag": "google_news",
                }
            )
            if len(events) >= settings.max_events_per_source:
                return events
    return events


def _collect_guardian(
    session: requests.Session,
    settings: MarketEventsSettings,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not settings.source_guardian:
        return events

    api_key = settings.guardian_api_key or "test"
    for query in settings.queries:
        page = 1
        while page <= 3:
            url = (
                "https://content.guardianapis.com/search?"
                f"api-key={quote_plus(api_key)}"
                f"&q={quote_plus(query)}"
                f"&from-date={start_date.isoformat()}"
                f"&to-date={end_date.isoformat()}"
                "&page-size=50&order-by=newest"
                f"&page={page}"
            )
            text = _fetch_text(
                session,
                url,
                timeout=settings.request_timeout,
                retries=settings.retries,
                min_delay=settings.min_delay,
                max_delay=settings.max_delay,
            )
            if not text:
                break
            try:
                payload = json.loads(text)
            except Exception:
                break

            response = payload.get("response") if isinstance(payload, dict) else None
            if not isinstance(response, dict):
                break

            results = response.get("results") if isinstance(response.get("results"), list) else []
            if not results:
                break

            for item in results:
                events.append(
                    {
                        "event_title": _as_text(item.get("webTitle")) or "",
                        "event_summary": _as_text(item.get("sectionName")) or "",
                        "source_name": "The Guardian",
                        "source_url": _as_text(item.get("webUrl")) or "",
                        "source_type": "news",
                        "published_at": _as_text(item.get("webPublicationDate")),
                        "source_tag": "guardian",
                    }
                )
                if len(events) >= settings.max_events_per_source:
                    return events
            page += 1
    return events


def _collect_worldbank_projects(
    session: requests.Session,
    settings: MarketEventsSettings,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not settings.source_worldbank:
        return events

    for cc in settings.worldbank_country_codes:
        url = f"https://search.worldbank.org/api/v2/projects?format=json&countrycode={quote_plus(cc)}&rows=200"
        text = _fetch_text(
            session,
            url,
            timeout=settings.request_timeout,
            retries=settings.retries,
            min_delay=settings.min_delay,
            max_delay=settings.max_delay,
        )
        if not text:
            continue
        try:
            payload = json.loads(text)
        except Exception:
            continue

        projects = payload.get("projects") if isinstance(payload, dict) else None
        if not isinstance(projects, dict):
            continue

        for project in projects.values():
            if not isinstance(project, dict):
                continue
            approval_raw = _as_text(project.get("boardapprovaldate"))
            approval_dt = _parse_datetime(approval_raw)
            if approval_dt is None:
                continue
            if not (start_date <= approval_dt.date() <= end_date):
                continue

            project_name = _as_text(project.get("project_name"))
            if not project_name:
                continue

            country_names = project.get("countryname")
            if isinstance(country_names, list):
                country_text = ", ".join(_uniq(country_names))
            else:
                country_text = _as_text(country_names) or ""

            summary = _as_text(project.get("projectstatusdisplay")) or ""
            if country_text:
                summary = f"{summary}. Country: {country_text}".strip(". ")

            events.append(
                {
                    "event_title": project_name,
                    "event_summary": summary,
                    "source_name": "World Bank Projects",
                    "source_url": _as_text(project.get("url")) or "",
                    "source_type": "open_data",
                    "published_at": approval_dt.isoformat(),
                    "event_category": "investment",
                    "source_tag": "worldbank",
                }
            )
            if len(events) >= settings.max_events_per_source:
                return events
    return events


def _collect_gdacs(
    session: requests.Session,
    settings: MarketEventsSettings,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not settings.source_gdacs:
        return events

    url = "https://www.gdacs.org/xml/rss.xml"
    xml_text = _fetch_text(
        session,
        url,
        timeout=settings.request_timeout,
        retries=settings.retries,
        min_delay=settings.min_delay,
        max_delay=settings.max_delay,
    )
    if not xml_text:
        return events

    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return events

    for item in root.findall("./channel/item"):
        title = _as_text(item.findtext("title"))
        if not title:
            continue
        pub_date = _as_text(item.findtext("pubDate"))
        pub_dt = _parse_datetime(pub_date)
        if pub_dt is None or not (start_date <= pub_dt.date() <= end_date):
            continue
        description = _as_text(item.findtext("description")) or ""
        link = _as_text(item.findtext("link")) or ""
        country = ""
        for child in item:
            if _rss_local_name(child.tag).lower() == "country":
                country = _as_text(child.text) or ""
                break

        if country:
            description = f"{description} Country: {country}".strip()

        events.append(
            {
                "event_title": title,
                "event_summary": description,
                "source_name": "GDACS",
                "source_url": link,
                "source_type": "open_data",
                "published_at": pub_dt.isoformat(),
                "event_category": "disaster",
                "source_tag": "gdacs",
            }
        )
        if len(events) >= settings.max_events_per_source:
            break
    return events


def _collect_usgs(
    session: requests.Session,
    settings: MarketEventsSettings,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not settings.source_usgs:
        return events

    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_month.geojson"
    text = _fetch_text(
        session,
        url,
        timeout=settings.request_timeout,
        retries=settings.retries,
        min_delay=settings.min_delay,
        max_delay=settings.max_delay,
    )
    if not text:
        return events
    try:
        payload = json.loads(text)
    except Exception:
        return events

    features = payload.get("features") if isinstance(payload, dict) else None
    if not isinstance(features, list):
        return events

    for feature in features:
        if not isinstance(feature, dict):
            continue
        props = feature.get("properties") if isinstance(feature.get("properties"), dict) else {}
        title = _as_text(props.get("title"))
        if not title:
            continue
        ts = props.get("time")
        try:
            pub_dt = datetime.fromtimestamp(float(ts) / 1000.0, tz=timezone.utc)
        except Exception:
            continue
        if not (start_date <= pub_dt.date() <= end_date):
            continue

        events.append(
            {
                "event_title": title,
                "event_summary": _as_text(props.get("place")) or "",
                "source_name": "USGS",
                "source_url": _as_text(props.get("url")) or "",
                "source_type": "open_data",
                "published_at": pub_dt.isoformat(),
                "event_category": "disaster",
                "source_tag": "usgs",
            }
        )
        if len(events) >= settings.max_events_per_source:
            break
    return events


def _collect_eonet(
    session: requests.Session,
    settings: MarketEventsSettings,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not settings.source_eonet:
        return events

    url = "https://eonet.gsfc.nasa.gov/api/v3/events?status=open&limit=200"
    text = _fetch_text(
        session,
        url,
        timeout=settings.request_timeout,
        retries=settings.retries,
        min_delay=settings.min_delay,
        max_delay=settings.max_delay,
    )
    if not text:
        return events
    try:
        payload = json.loads(text)
    except Exception:
        return events

    for item in payload.get("events", []) if isinstance(payload, dict) else []:
        if not isinstance(item, dict):
            continue
        title = _as_text(item.get("title"))
        if not title:
            continue
        geometries = item.get("geometry") if isinstance(item.get("geometry"), list) else []
        if not geometries:
            continue
        published_at = _as_text(geometries[-1].get("date"))
        pub_dt = _parse_datetime(published_at)
        if pub_dt is None or not (start_date <= pub_dt.date() <= end_date):
            continue

        categories = item.get("categories") if isinstance(item.get("categories"), list) else []
        category_titles = [
            _as_text(cat.get("title")) for cat in categories if isinstance(cat, dict)
        ]
        summary = ", ".join(v for v in category_titles if v)

        events.append(
            {
                "event_title": title,
                "event_summary": summary,
                "source_name": "NASA EONET",
                "source_url": _as_text(item.get("link")) or "",
                "source_type": "open_data",
                "published_at": pub_dt.isoformat(),
                "event_category": "disaster",
                "source_tag": "eonet",
            }
        )
        if len(events) >= settings.max_events_per_source:
            break
    return events


def _collect_un_news(
    session: requests.Session,
    settings: MarketEventsSettings,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not settings.source_un_news:
        return events

    rss_url = "https://news.un.org/feed/subscribe/en/news/all/rss.xml"
    xml_text = _fetch_text(
        session,
        rss_url,
        timeout=settings.request_timeout,
        retries=settings.retries,
        min_delay=settings.min_delay,
        max_delay=settings.max_delay,
    )
    if not xml_text:
        return events

    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return events

    for item in root.findall("./channel/item"):
        title = _as_text(item.findtext("title"))
        if not title:
            continue
        pub_date = _as_text(item.findtext("pubDate"))
        pub_dt = _parse_datetime(pub_date)
        if pub_dt is None or not (start_date <= pub_dt.date() <= end_date):
            continue

        description = _strip_html(_as_text(item.findtext("description")) or "")
        link = _as_text(item.findtext("link")) or ""

        events.append(
            {
                "event_title": title,
                "event_summary": description,
                "source_name": "UN News",
                "source_url": link,
                "source_type": "news",
                "published_at": pub_dt.isoformat(),
                "source_tag": "un_news",
            }
        )
        if len(events) >= settings.max_events_per_source:
            break
    return events


def _collect_federal_reserve(
    session: requests.Session,
    settings: MarketEventsSettings,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not settings.source_federal_reserve:
        return events

    rss_url = "https://www.federalreserve.gov/feeds/press_all.xml"
    xml_text = _fetch_text(
        session,
        rss_url,
        timeout=settings.request_timeout,
        retries=settings.retries,
        min_delay=settings.min_delay,
        max_delay=settings.max_delay,
    )
    if not xml_text:
        return events

    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return events

    for item in root.findall("./channel/item"):
        title = _as_text(item.findtext("title"))
        if not title:
            continue
        pub_date = _as_text(item.findtext("pubDate"))
        pub_dt = _parse_datetime(pub_date)
        if pub_dt is None or not (start_date <= pub_dt.date() <= end_date):
            continue

        summary = _strip_html(_as_text(item.findtext("description")) or "")
        link = _as_text(item.findtext("link")) or ""

        events.append(
            {
                "event_title": title,
                "event_summary": summary,
                "source_name": "US Federal Reserve",
                "source_url": link,
                "source_type": "gov",
                "published_at": pub_dt.isoformat(),
                "event_category": "macro_economic",
                "source_tag": "fed",
            }
        )
        if len(events) >= settings.max_events_per_source:
            break
    return events


def _collect_ecb(
    session: requests.Session,
    settings: MarketEventsSettings,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not settings.source_ecb:
        return events

    rss_url = "https://www.ecb.europa.eu/rss/press.html"
    xml_text = _fetch_text(
        session,
        rss_url,
        timeout=settings.request_timeout,
        retries=settings.retries,
        min_delay=settings.min_delay,
        max_delay=settings.max_delay,
    )
    if not xml_text:
        return events

    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return events

    for item in root.findall("./channel/item"):
        title = _as_text(item.findtext("title"))
        if not title:
            continue
        pub_date = _as_text(item.findtext("pubDate"))
        pub_dt = _parse_datetime(pub_date)
        if pub_dt is None or not (start_date <= pub_dt.date() <= end_date):
            continue

        summary = _strip_html(_as_text(item.findtext("description")) or "")
        link = _as_text(item.findtext("link")) or ""

        events.append(
            {
                "event_title": title,
                "event_summary": summary,
                "source_name": "European Central Bank",
                "source_url": link,
                "source_type": "gov",
                "published_at": pub_dt.isoformat(),
                "event_category": "macro_economic",
                "source_tag": "ecb",
            }
        )
        if len(events) >= settings.max_events_per_source:
            break
    return events


def _collect_aljazeera(
    session: requests.Session,
    settings: MarketEventsSettings,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not settings.source_aljazeera:
        return events

    rss_url = "https://www.aljazeera.com/xml/rss/all.xml"
    xml_text = _fetch_text(
        session,
        rss_url,
        timeout=settings.request_timeout,
        retries=settings.retries,
        min_delay=settings.min_delay,
        max_delay=settings.max_delay,
    )
    if not xml_text:
        return events

    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return events

    for item in root.findall("./channel/item"):
        title = _as_text(item.findtext("title"))
        if not title:
            continue
        pub_date = _as_text(item.findtext("pubDate"))
        pub_dt = _parse_datetime(pub_date)
        if pub_dt is None or not (start_date <= pub_dt.date() <= end_date):
            continue

        description = _strip_html(_as_text(item.findtext("description")) or "")
        link = _as_text(item.findtext("link")) or ""

        events.append(
            {
                "event_title": title,
                "event_summary": description,
                "source_name": "Al Jazeera",
                "source_url": link,
                "source_type": "news",
                "published_at": pub_dt.isoformat(),
                "source_tag": "aljazeera",
            }
        )
        if len(events) >= settings.max_events_per_source:
            break
    return events


def _collect_arabnews(
    session: requests.Session,
    settings: MarketEventsSettings,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not settings.source_arabnews:
        return events

    rss_url = "https://www.arabnews.com/rss.xml"
    xml_text = _fetch_text(
        session,
        rss_url,
        timeout=settings.request_timeout,
        retries=settings.retries,
        min_delay=settings.min_delay,
        max_delay=settings.max_delay,
    )
    if not xml_text:
        return events

    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return events

    for item in root.findall("./channel/item"):
        title = _as_text(item.findtext("title"))
        if not title:
            continue
        pub_date = _as_text(item.findtext("pubDate"))
        pub_dt = _parse_datetime(pub_date)
        if pub_dt is None or not (start_date <= pub_dt.date() <= end_date):
            continue

        description = _strip_html(_as_text(item.findtext("description")) or "")
        link = _as_text(item.findtext("link")) or ""

        events.append(
            {
                "event_title": title,
                "event_summary": description,
                "source_name": "Arab News",
                "source_url": link,
                "source_type": "news",
                "published_at": pub_dt.isoformat(),
                "source_tag": "arabnews",
            }
        )
        if len(events) >= settings.max_events_per_source:
            break
    return events


def _collect_spa_news(
    session: requests.Session,
    settings: MarketEventsSettings,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not settings.source_spa:
        return events

    url = "https://www.spa.gov.sa/en"
    html_text = _fetch_text(
        session,
        url,
        timeout=settings.request_timeout,
        retries=settings.retries,
        min_delay=settings.min_delay,
        max_delay=settings.max_delay,
    )
    if not html_text:
        return events

    try:
        match = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', html_text, re.S | re.I)
        if not match:
            return events
        payload = json.loads(match.group(1))
    except Exception:
        return events

    page_props = ((payload.get("props") or {}).get("pageProps") or {}) if isinstance(payload, dict) else {}
    rows = page_props.get("mainNews") if isinstance(page_props.get("mainNews"), list) else []
    if not rows:
        return events

    for row in rows:
        if not isinstance(row, dict):
            continue
        title = _as_text(row.get("title"))
        if not title:
            continue

        published_at = row.get("published_at")
        pub_dt = None
        if isinstance(published_at, (int, float)):
            try:
                pub_dt = datetime.fromtimestamp(float(published_at), tz=timezone.utc)
            except Exception:
                pub_dt = None
        if pub_dt is None:
            pub_dt = _parse_datetime(published_at)
        if pub_dt is None or not (start_date <= pub_dt.date() <= end_date):
            continue

        link_raw = _as_text(row.get("sharable_link")) or ""
        if link_raw and not link_raw.startswith("http"):
            link_raw = f"https://{link_raw.lstrip('/')}"

        summary = _as_text(row.get("subtitle")) or _strip_html(_as_text(row.get("content")) or "")
        if len(summary) > 500:
            summary = summary[:500].rstrip() + "..."

        events.append(
            {
                "event_title": title,
                "event_summary": summary,
                "source_name": "Saudi Press Agency",
                "source_url": link_raw,
                "source_type": "gov",
                "published_at": pub_dt.isoformat(),
                "source_tag": "spa",
            }
        )
        if len(events) >= settings.max_events_per_source:
            break
    return events


def _gdelt_code_to_label(event_root_code: str | None) -> str:
    code = (_as_text(event_root_code) or "").strip()
    labels = {
        "1": "statement",
        "2": "appeal",
        "3": "intent",
        "4": "consult",
        "5": "diplomatic cooperation",
        "6": "material cooperation",
        "7": "provide aid",
        "8": "yield",
        "9": "investigate",
        "10": "demand",
        "11": "disapprove",
        "12": "reject",
        "13": "threaten",
        "14": "protest",
        "15": "exhibit force",
        "16": "reduce relations",
        "17": "coerce",
        "18": "assault",
        "19": "fight",
        "20": "mass violence",
    }
    return labels.get(code, "event")


def _collect_gdelt_daily_exports(
    session: requests.Session,
    settings: MarketEventsSettings,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not settings.source_gdelt:
        return events

    day_count = max(1, (end_date - start_date).days + 1)
    if day_count > settings.max_backfill_days_per_run:
        start_date = end_date - timedelta(days=settings.max_backfill_days_per_run - 1)

    for offset in range((end_date - start_date).days + 1):
        current_day = start_date + timedelta(days=offset)
        ymd = current_day.strftime("%Y%m%d")
        url = f"http://data.gdeltproject.org/events/{ymd}.export.CSV.zip"
        data = None
        for attempt in range(settings.retries):
            try:
                resp = session.get(url, headers=REQUEST_HEADERS, timeout=settings.request_timeout)
                if resp.status_code == 200:
                    data = resp.content
                    break
                if resp.status_code == 404:
                    break
                logger.warning("GDELT day file status=%s day=%s", resp.status_code, ymd)
            except Exception as exc:
                logger.warning("GDELT fetch error day=%s attempt=%d err=%s", ymd, attempt + 1, exc)
            time.sleep(random.uniform(settings.min_delay, settings.max_delay))
        if not data:
            continue

        try:
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                names = zf.namelist()
                if not names:
                    continue
                with zf.open(names[0]) as fh:
                    text_stream = io.TextIOWrapper(fh, encoding="utf-8", errors="replace")
                    reader = csv.reader(text_stream, delimiter="\t")
                    for row in reader:
                        if len(row) < 30:
                            continue
                        actor1 = _as_text(row[6]) or ""
                        actor2 = _as_text(row[16]) or ""
                        event_root = _as_text(row[29]) or ""
                        action_geo = _as_text(row[51]) or _as_text(row[37]) or ""
                        source_url = _as_text(row[57]) if len(row) > 57 else ""
                        if not source_url:
                            continue

                        label = _gdelt_code_to_label(event_root)
                        title = f"{actor1 or 'Actor'} {label} {actor2 or 'counterparty'}"
                        summary = action_geo or "GDELT coded event"

                        category = "conflict" if event_root in {"14", "15", "16", "17", "18", "19", "20"} else "policy"

                        events.append(
                            {
                                "event_title": title.strip(),
                                "event_summary": summary,
                                "source_name": "GDELT",
                                "source_url": source_url,
                                "source_type": "open_data",
                                "published_at": current_day.isoformat(),
                                "event_category": category,
                                "source_tag": "gdelt_daily",
                            }
                        )
                        if len(events) >= settings.max_events_per_source:
                            return events
        except Exception as exc:
            logger.warning("Failed to parse GDELT day file day=%s err=%s", ymd, exc)
    return events


def _saved_data_dir(settings: MarketEventsSettings) -> Path:
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    return settings.data_dir


def _available_region_sections(config_obj) -> dict[str, str]:
    out: dict[str, str] = {}
    prefix = CONFIG_SECTION_PREFIX
    for section_name in config_obj.sections():
        if not section_name.startswith(prefix):
            continue
        suffix = section_name[len(prefix):].strip().lower()
        if suffix:
            out[suffix] = section_name
    return out


def _split_csv_values(value: str | None) -> list[str]:
    text = _as_text(value)
    if not text:
        return []
    return [v.strip() for v in text.split(",") if v.strip()]


def _get_opt(section: Any, key: str, default: str) -> str:
    if hasattr(section, "get"):
        try:
            return section.get(key, fallback=default)  # configparser.SectionProxy
        except TypeError:
            return section.get(key, default)
    return default


config = read_config()
_region_sections = _available_region_sections(config)


def _load_settings_for_region(region_key: str) -> MarketEventsSettings:
    region_slug = _slug(region_key)
    defaults = DEFAULT_REGION_SETTINGS.get(region_slug) or DEFAULT_REGION_SETTINGS["india_kerala"]

    global_section = config[CONFIG_SECTION_GLOBAL] if config.has_section(CONFIG_SECTION_GLOBAL) else {}
    section_name = _region_sections.get(region_slug, f"{CONFIG_SECTION_PREFIX}{region_slug}")
    region_section = config[section_name] if config.has_section(section_name) else {}

    today = datetime.now(timezone.utc).date()

    region_name = _get_opt(region_section, "region_name", defaults["region_name"])
    countries = _split_csv_values(_get_opt(region_section, "countries", ",".join(defaults["countries"]))) or defaults["countries"]
    states = _split_csv_values(_get_opt(region_section, "states", ",".join(defaults["states"]))) or defaults["states"]
    cities = _split_csv_values(_get_opt(region_section, "cities", ",".join(defaults["cities"]))) or defaults["cities"]
    localities = _split_csv_values(_get_opt(region_section, "localities", ""))
    queries = _split_csv_values(_get_opt(region_section, "queries", ",".join(defaults["queries"]))) or defaults["queries"]

    es_index = _get_opt(global_section, "es_index", DEFAULT_EVENTS_INDEX)
    es_index = _get_opt(region_section, "es_index", es_index)
    data_dir_value = _get_opt(region_section, "data_dir", f"saved_data/market_events/{region_slug}")

    lookback_days = max(1, int(_get_opt(global_section, "lookback_days", "7")))
    lookback_days = max(1, int(_get_opt(region_section, "lookback_days", str(lookback_days))))

    max_events_per_source = max(10, int(_get_opt(global_section, "max_events_per_source", "400")))
    max_events_per_source = max(10, int(_get_opt(region_section, "max_events_per_source", str(max_events_per_source))))

    max_events_per_run = max(20, int(_get_opt(global_section, "max_events_per_run", "1500")))
    max_events_per_run = max(20, int(_get_opt(region_section, "max_events_per_run", str(max_events_per_run))))

    request_timeout = max(5, int(_get_opt(global_section, "request_timeout_seconds", "30")))
    request_timeout = max(5, int(_get_opt(region_section, "request_timeout_seconds", str(request_timeout))))

    retries = max(1, int(_get_opt(global_section, "request_retries", "2")))
    retries = max(1, int(_get_opt(region_section, "request_retries", str(retries))))

    min_delay = float(_get_opt(global_section, "min_delay_seconds", "1.2"))
    min_delay = float(_get_opt(region_section, "min_delay_seconds", str(min_delay)))
    max_delay = float(_get_opt(global_section, "max_delay_seconds", "2.8"))
    max_delay = float(_get_opt(region_section, "max_delay_seconds", str(max_delay)))
    if max_delay < min_delay:
        max_delay = min_delay

    schedule_hour = _get_opt(region_section, "schedule_hour", defaults["schedule_hour"])
    schedule_minute = _get_opt(region_section, "schedule_minute", defaults["schedule_minute"])
    schedule_timezone = _get_opt(region_section, "schedule_timezone", defaults["schedule_timezone"])

    guardian_api_key = _get_opt(global_section, "guardian_api_key", "test")
    guardian_api_key = _get_opt(region_section, "guardian_api_key", guardian_api_key)

    wb_codes = _split_csv_values(_get_opt(region_section, "worldbank_country_codes", ",".join(defaults["worldbank_country_codes"])))
    if not wb_codes:
        wb_codes = defaults["worldbank_country_codes"]

    backfill_start_default = date(today.year - 10, today.month, min(today.day, 28))
    backfill_end_default = today
    backfill_start = _parse_date(
        _get_opt(global_section, "backfill_start", backfill_start_default.isoformat()),
        backfill_start_default,
    )
    backfill_start = _parse_date(
        _get_opt(region_section, "backfill_start", backfill_start.isoformat()),
        backfill_start,
    )
    backfill_end = _parse_date(
        _get_opt(global_section, "backfill_end", backfill_end_default.isoformat()),
        backfill_end_default,
    )
    backfill_end = _parse_date(
        _get_opt(region_section, "backfill_end", backfill_end.isoformat()),
        backfill_end,
    )

    max_backfill_days_per_run = max(7, int(_get_opt(global_section, "max_backfill_days_per_run", "30")))
    max_backfill_days_per_run = max(
        7,
        int(_get_opt(region_section, "max_backfill_days_per_run", str(max_backfill_days_per_run))),
    )

    source_google_news = _parse_bool(_get_opt(global_section, "source_google_news_enabled", "true"), True)
    source_guardian = _parse_bool(_get_opt(global_section, "source_guardian_enabled", "true"), True)
    source_worldbank = _parse_bool(_get_opt(global_section, "source_worldbank_enabled", "true"), True)
    source_gdacs = _parse_bool(_get_opt(global_section, "source_gdacs_enabled", "true"), True)
    source_usgs = _parse_bool(_get_opt(global_section, "source_usgs_enabled", "true"), True)
    source_eonet = _parse_bool(_get_opt(global_section, "source_eonet_enabled", "false"), False)
    source_gdelt = _parse_bool(_get_opt(global_section, "source_gdelt_enabled", "true"), True)
    source_un_news = _parse_bool(_get_opt(global_section, "source_un_news_enabled", "true"), True)
    source_federal_reserve = _parse_bool(
        _get_opt(global_section, "source_federal_reserve_enabled", "true"),
        True,
    )
    source_ecb = _parse_bool(_get_opt(global_section, "source_ecb_enabled", "true"), True)
    source_aljazeera = _parse_bool(_get_opt(global_section, "source_aljazeera_enabled", "true"), True)
    source_arabnews = _parse_bool(_get_opt(global_section, "source_arabnews_enabled", "true"), True)
    source_spa = _parse_bool(_get_opt(global_section, "source_spa_enabled", "true"), True)

    min_source_credibility = (_get_opt(global_section, "min_source_credibility", "medium") or "medium").strip().lower()
    if min_source_credibility not in {"low", "medium", "high"}:
        min_source_credibility = "medium"
    min_quality_score = max(0, min(100, int(_get_opt(global_section, "min_quality_score", "55"))))
    source_weight_high = float(_get_opt(global_section, "source_weight_high", "1.0"))
    source_weight_medium = float(_get_opt(global_section, "source_weight_medium", "0.75"))
    source_weight_low = float(_get_opt(global_section, "source_weight_low", "0.5"))

    source_google_news = _parse_bool(_get_opt(region_section, "source_google_news_enabled", str(source_google_news).lower()), source_google_news)
    source_guardian = _parse_bool(_get_opt(region_section, "source_guardian_enabled", str(source_guardian).lower()), source_guardian)
    source_worldbank = _parse_bool(_get_opt(region_section, "source_worldbank_enabled", str(source_worldbank).lower()), source_worldbank)
    source_gdacs = _parse_bool(_get_opt(region_section, "source_gdacs_enabled", str(source_gdacs).lower()), source_gdacs)
    source_usgs = _parse_bool(_get_opt(region_section, "source_usgs_enabled", str(source_usgs).lower()), source_usgs)
    source_eonet = _parse_bool(_get_opt(region_section, "source_eonet_enabled", str(source_eonet).lower()), source_eonet)
    source_gdelt = _parse_bool(_get_opt(region_section, "source_gdelt_enabled", str(source_gdelt).lower()), source_gdelt)
    source_un_news = _parse_bool(_get_opt(region_section, "source_un_news_enabled", str(source_un_news).lower()), source_un_news)
    source_federal_reserve = _parse_bool(
        _get_opt(region_section, "source_federal_reserve_enabled", str(source_federal_reserve).lower()),
        source_federal_reserve,
    )
    source_ecb = _parse_bool(_get_opt(region_section, "source_ecb_enabled", str(source_ecb).lower()), source_ecb)
    source_aljazeera = _parse_bool(_get_opt(region_section, "source_aljazeera_enabled", str(source_aljazeera).lower()), source_aljazeera)
    source_arabnews = _parse_bool(_get_opt(region_section, "source_arabnews_enabled", str(source_arabnews).lower()), source_arabnews)
    source_spa = _parse_bool(_get_opt(region_section, "source_spa_enabled", str(source_spa).lower()), source_spa)

    min_source_credibility = (
        _get_opt(region_section, "min_source_credibility", min_source_credibility) or min_source_credibility
    ).strip().lower()
    if min_source_credibility not in {"low", "medium", "high"}:
        min_source_credibility = "medium"
    min_quality_score = max(
        0,
        min(100, int(_get_opt(region_section, "min_quality_score", str(min_quality_score)))),
    )
    source_weight_high = float(_get_opt(region_section, "source_weight_high", str(source_weight_high)))
    source_weight_medium = float(_get_opt(region_section, "source_weight_medium", str(source_weight_medium)))
    source_weight_low = float(_get_opt(region_section, "source_weight_low", str(source_weight_low)))

    countries = _uniq([_canonical_country(v) for v in countries])
    states = _uniq([_canonical_state(v) for v in states])
    cities = _uniq([_canonical_city(v) for v in cities])
    localities = _uniq(localities)

    return MarketEventsSettings(
        region_key=region_slug,
        region_name=region_name,
        countries=countries,
        states=states,
        cities=cities,
        localities=localities,
        queries=queries,
        lookback_days=lookback_days,
        max_events_per_source=max_events_per_source,
        max_events_per_run=max_events_per_run,
        request_timeout=request_timeout,
        retries=retries,
        min_delay=min_delay,
        max_delay=max_delay,
        es_index=es_index,
        data_dir=Path(data_dir_value),
        schedule_hour=schedule_hour,
        schedule_minute=schedule_minute,
        schedule_timezone=schedule_timezone,
        source_google_news=source_google_news,
        source_guardian=source_guardian,
        source_worldbank=source_worldbank,
        source_gdacs=source_gdacs,
        source_usgs=source_usgs,
        source_eonet=source_eonet,
        source_gdelt=source_gdelt,
        source_un_news=source_un_news,
        source_federal_reserve=source_federal_reserve,
        source_ecb=source_ecb,
        source_aljazeera=source_aljazeera,
        source_arabnews=source_arabnews,
        source_spa=source_spa,
        min_source_credibility=min_source_credibility,
        min_quality_score=min_quality_score,
        source_weight_high=source_weight_high,
        source_weight_medium=source_weight_medium,
        source_weight_low=source_weight_low,
        guardian_api_key=guardian_api_key,
        worldbank_country_codes=wb_codes,
        backfill_start=backfill_start,
        backfill_end=backfill_end,
        max_backfill_days_per_run=max_backfill_days_per_run,
    )


DEFAULT_REGION = "uae"
SETTINGS = _load_settings_for_region(DEFAULT_REGION)


_es_config = config["elasticsearch"] if config.has_section("elasticsearch") else {}
es_hosts: list[str] = []
hosts_raw = _es_config.get("host", "localhost:9200") if hasattr(_es_config, "get") else "localhost:9200"
for host in str(hosts_raw).split(","):
    h = host.strip()
    if not h:
        continue
    if not h.startswith("http"):
        h = f"http://{h}"
    if ":" not in h.split("//")[-1]:
        h += ":9200"
    es_hosts.append(h)

es_user = _es_config.get("username", "elastic") if hasattr(_es_config, "get") else "elastic"
es_password = _es_config.get("password", "changeme") if hasattr(_es_config, "get") else "changeme"


def _activate_settings(region: str | None) -> MarketEventsSettings:
    global SETTINGS
    if region:
        SETTINGS = _load_settings_for_region(region)
    return SETTINGS


def _activate_settings_from_records(records: list[dict[str, Any]]) -> MarketEventsSettings:
    region = ""
    for rec in records:
        region = (_as_text(rec.get("_target_region")) or "").strip().lower()
        if region:
            break
    return _activate_settings(region or SETTINGS.region_key)


def es_client() -> Elasticsearch:
    es = Elasticsearch(
        hosts=es_hosts,
        http_auth=(es_user, es_password),
        timeout=30,
        max_retries=3,
        retry_on_timeout=True,
    )
    try:
        info = es.info()
        logger.info("Connected to ES cluster=%s", info.get("cluster_name", "unknown"))
    except Exception as exc:
        logger.warning("Failed to fetch ES info: %s", exc)
    return es


def ensure_index(es: Elasticsearch, index: str) -> None:
    if not es.indices.exists(index=index):
        es.indices.create(index=index, body=ES_INDEX_MAPPING)


def df_to_actions(df: pd.DataFrame, index: str) -> Iterable[dict[str, Any]]:
    clean = df.replace({np.nan: None})
    for record in clean.to_dict(orient="records"):
        doc_id = record.get("id")
        if not doc_id:
            continue
        yield {
            "_index": index,
            "_id": doc_id,
            "_op_type": "index",
            "_source": record,
        }


def index_events_to_es(events: list[dict[str, Any]], es: Elasticsearch | None = None, index: str | None = None) -> int:
    target_index = index or SETTINGS.es_index
    if not events:
        logger.info("No market events to index (%s)", target_index)
        return 0

    if es is None:
        es = es_client()
    ensure_index(es, target_index)

    df = pd.DataFrame(events)
    if df.empty:
        return 0
    df = df.drop_duplicates(subset=["id"], keep="last").reset_index(drop=True)

    actions = list(df_to_actions(df, target_index))
    if not actions:
        return 0

    indexed, errors = helpers.bulk(
        es,
        actions,
        chunk_size=500,
        request_timeout=120,
        raise_on_error=False,
        raise_on_exception=False,
    )
    if errors:
        logger.warning("ES bulk indexing completed with %d errors (index=%s)", len(errors), target_index)
    logger.info("Indexed %d market-event records into ES index %s", indexed, target_index)
    return indexed


class InputParams(BaseModel):
    region: str = DEFAULT_REGION
    mode: str = "incremental"  # incremental|backfill
    start_date: str | None = None
    end_date: str | None = None


def _resolve_date_window(params: InputParams) -> tuple[date, date]:
    today = datetime.now(timezone.utc).date()

    if params.mode.lower() == "backfill":
        start = _parse_date(params.start_date, SETTINGS.backfill_start)
        end = _parse_date(params.end_date, SETTINGS.backfill_end)
    else:
        end = _parse_date(params.end_date, today)
        fallback_start = end - timedelta(days=max(1, SETTINGS.lookback_days) - 1)
        start = _parse_date(params.start_date, fallback_start)

    if end < start:
        start, end = end, start
    return start, end


@task
async def discover_events(params: InputParams = None) -> list[dict[str, Any]]:
    selected = params or InputParams()
    settings = _activate_settings(selected.region)
    start_date, end_date = _resolve_date_window(selected)

    logger.info(
        "Discover market events region=%s mode=%s window=%s..%s",
        settings.region_key,
        selected.mode,
        start_date,
        end_date,
    )

    session = requests.Session()
    discovered: list[dict[str, Any]] = []

    collectors = [
        _collect_google_news,
        _collect_guardian,
        _collect_un_news,
        _collect_federal_reserve,
        _collect_ecb,
        _collect_aljazeera,
        _collect_arabnews,
        _collect_spa_news,
        _collect_worldbank_projects,
        _collect_gdacs,
        _collect_usgs,
        _collect_eonet,
        _collect_gdelt_daily_exports,
    ]

    for collector in collectors:
        try:
            rows = collector(session, settings, start_date, end_date)
            for row in rows:
                row["discovered_at"] = datetime.now(timezone.utc).isoformat()
                row["_target_region"] = settings.region_key
            discovered.extend(rows)
            logger.info("Collector %s discovered %d rows", collector.__name__, len(rows))
            if len(discovered) >= settings.max_events_per_run:
                discovered = discovered[: settings.max_events_per_run]
                break
        except Exception:
            logger.exception("Collector failed: %s", collector.__name__)

    # De-duplicate by source URL + title before enrichment.
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in discovered:
        key = "|".join(
            [
                (_as_text(row.get("event_title")) or "").lower(),
                (_as_text(row.get("source_url")) or "").lower(),
            ]
        )
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    out_dir = _saved_data_dir(settings)
    pd.DataFrame(deduped).to_json(out_dir / "discovered_raw.json", orient="records", force_ascii=False, indent=2)
    logger.info("Total discovered market-event rows: %d", len(deduped))
    return deduped


@task
async def enrich_and_classify(discovered: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not discovered:
        logger.info("No discovered market events to enrich")
        return []

    settings = _activate_settings_from_records(discovered)
    enriched: list[dict[str, Any]] = []

    for row in discovered:
        normalized = _normalize_event_record(row, settings)
        if not normalized:
            continue
        enriched.append(normalized)
        if len(enriched) >= settings.max_events_per_run:
            break

    # Deduplicate by ID post-normalization.
    deduped: dict[str, dict[str, Any]] = {}
    for row in enriched:
        deduped[row["id"]] = row
    final_rows = list(deduped.values())

    out_dir = _saved_data_dir(settings)
    pd.DataFrame(final_rows).to_json(out_dir / "enriched_events.json", orient="records", force_ascii=False, indent=2)
    logger.info("Enriched market-event rows: %d", len(final_rows))
    return final_rows


@task
async def standardize_and_index(enriched: list[dict[str, Any]]) -> int:
    if not enriched:
        logger.info("No enriched market events to index")
        return 0

    settings = _activate_settings_from_records(enriched)
    deduped = list({row["id"]: row for row in enriched if row.get("id")}.values())
    indexed = index_events_to_es(deduped, index=settings.es_index)

    out_dir = _saved_data_dir(settings)
    pd.DataFrame(deduped).to_json(out_dir / "standardized_events.json", orient="records", force_ascii=False, indent=2)
    return indexed


def _build_triggers() -> list[Trigger]:
    triggers: list[Trigger] = []

    region_keys = set(DEFAULT_REGION_SETTINGS.keys())
    region_keys.update(_region_sections.keys())

    for region_key in sorted(region_keys):
        settings = _load_settings_for_region(region_key)
        triggers.append(
            Trigger(
                id=f"market_events_daily_{settings.region_key}",
                name=f"Market Events Daily ({settings.region_name})",
                description=(
                    f"Track major global/country/city events impacting real estate for {settings.region_name}"
                ),
                params=InputParams(region=settings.region_key),
                schedule=CronTrigger(
                    hour=settings.schedule_hour,
                    minute=settings.schedule_minute,
                    timezone=settings.schedule_timezone,
                ),
            )
        )
    return triggers


register_pipeline(
    id="market_events_pipeline",
    description=(
        "Collect, classify and index major global/local events that can impact "
        "property and real-estate markets across UAE, KSA and Kerala focus cities."
    ),
    tasks=[discover_events, enrich_and_classify, standardize_and_index],
    triggers=_build_triggers(),
    params=InputParams,
)
