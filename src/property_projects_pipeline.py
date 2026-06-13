"""Unified multi-region property projects scraper pipeline.

Discovers, enriches, standardizes and indexes property projects across
configured cities in Kerala, UAE and KSA. The pipeline is source-agnostic and
supports apartments, villas and commercial project inventory.

City onboarding is config-driven via `[property_projects.<city>]` sections.
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import html
import io
import json
import logging
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote_plus, unquote, urljoin, urlparse

import numpy as np
import pandas as pd
import requests

try:  # pragma: no cover - optional dependency
    from curl_cffi import requests as curl_requests
except Exception:  # pragma: no cover
    curl_requests = None

from apscheduler.triggers.cron import CronTrigger
from elasticsearch import Elasticsearch, helpers
from pydantic import BaseModel
from plombery import Trigger, register_pipeline, task

from config import read_config
from utils.llm_client import call_llm

try:  # pragma: no cover - optional dependency
    import json_repair
except Exception:  # pragma: no cover
    json_repair = None

try:  # pragma: no cover - optional dependency
    import dirtyjson
except Exception:  # pragma: no cover
    dirtyjson = None


logger = logging.getLogger(__name__)


DEFAULT_PROJECTS_INDEX = "property_projects"
CONFIG_SECTION_PREFIX = "property_projects"

SOURCE_KEYS = (
    "realestateindia",
    "commonfloor",
    "housing",
    "propertyfinder",
    "aqar",
    "bayut",
    "offplan_dubai",
    "extra_sources",
)

PRICE_MULTIPLIERS = {
    "cr": 10_000_000,
    "crore": 10_000_000,
    "lac": 100_000,
    "lakh": 100_000,
    "l": 100_000,
    "k": 1_000,
    "m": 1_000_000,
    "mn": 1_000_000,
    "million": 1_000_000,
    "b": 1_000_000_000,
    "bn": 1_000_000_000,
    "billion": 1_000_000_000,
}

HARD_BLOCK_MARKERS = (
    "verify you are a human",
    "access denied",
    "unusual traffic",
    "are you a robot",
    "pardon our interruption",
    "our systems have detected unusual traffic",
    "security challenge",
    "temporarily blocked",
    "cloudflare ray id",
    "incapsula incident id",
)

SOFT_BLOCK_MARKERS = (
    "captcha",
    "blocked",
)

REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
}

MODERN_USER_AGENTS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.126 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.78 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.207 Safari/537.36",
)

IMPERSONATE_IDS = ("chrome110", "chrome120", "chrome124", "chrome131")

COMMERCIAL_TYPE_TOKENS = {
    "commercial",
    "office",
    "retail",
    "shop",
    "showroom",
    "warehouse",
    "industrial",
    "labour-camp",
    "co-working",
    "coworking",
    "full-floor",
    "building",
}

RESIDENTIAL_TYPE_TOKENS = {
    "apartment",
    "villa",
    "townhouse",
    "duplex",
    "studio",
    "penthouse",
    "plot",
    "plotted-development",
    "residential-land",
    "floor",
}

NAME_KEYS = (
    "projectName",
    "project_name",
    "projectTitle",
    "project_title",
    "title",
    "name",
    "displayName",
    "developmentName",
)
URL_KEYS = (
    "projectURL",
    "projectUrl",
    "project_url",
    "url",
    "href",
    "permalink",
    "seoUrl",
    "detailUrl",
    "link",
)
BUILDER_KEYS = (
    "builderName",
    "builder_name",
    "builder",
    "developerName",
    "developer_name",
    "developer",
    "developerTitle",
)
PRICE_KEYS = (
    "price",
    "priceRange",
    "price_range",
    "startingPrice",
    "launchPrice",
    "projectValue",
    "minPrice",
    "maxPrice",
    "amount",
    "priceText",
    "priceLabel",
)
TYPE_KEYS = (
    "propertyType",
    "property_type",
    "propertyTypes",
    "category",
    "type",
)
CONFIG_KEYS = (
    "configuration",
    "configurations",
    "bhk",
    "bedroomRange",
    "beds",
)
LOCALITY_KEYS = ("locality", "localityName", "area", "areaName", "district")
CITY_KEYS = ("city", "cityName", "city_name")
STATE_KEYS = ("state", "stateName", "province")
COUNTRY_KEYS = ("country", "countryName")
POSSESSION_KEYS = (
    "possessionDate",
    "possession_date",
    "handoverDate",
    "completionDate",
    "deliveryDate",
)
STATUS_KEYS = (
    "launchStatus",
    "launch_status",
    "projectStatus",
    "project_status",
    "constructionStatus",
    "status",
)
RERA_KEYS = ("reraNumber", "rera", "reraId", "reraNo")
LAT_KEYS = ("lat", "latitude")
LNG_KEYS = ("lng", "lon", "longitude")
DESCRIPTION_KEYS = (
    "description",
    "shortDescription",
    "projectDescription",
    "summary",
)
AMENITY_KEYS = ("amenities", "features", "projectAmenities")
IMAGE_KEYS = ("images", "image", "gallery", "projectImages")


@dataclass(slots=True)
class SourceTuning:
    enabled: bool
    pages: int
    retries: int
    max_projects: int


DEFAULT_SOURCE_TUNING: dict[str, SourceTuning] = {
    "realestateindia": SourceTuning(enabled=True, pages=4, retries=2, max_projects=100),
    "commonfloor": SourceTuning(enabled=True, pages=3, retries=1, max_projects=80),
    "housing": SourceTuning(enabled=True, pages=1, retries=1, max_projects=40),
    "propertyfinder": SourceTuning(enabled=True, pages=3, retries=2, max_projects=100),
    "aqar": SourceTuning(enabled=True, pages=2, retries=1, max_projects=120),
    "bayut": SourceTuning(enabled=True, pages=2, retries=1, max_projects=60),
    "offplan_dubai": SourceTuning(enabled=True, pages=2, retries=1, max_projects=80),
    "extra_sources": SourceTuning(enabled=True, pages=1, retries=1, max_projects=80),
}


CITY_SOURCE_TUNING_OVERRIDES: dict[str, dict[str, SourceTuning]] = {
    "kochi": {
        "realestateindia": SourceTuning(enabled=True, pages=4, retries=2, max_projects=90),
        "commonfloor": SourceTuning(enabled=True, pages=2, retries=1, max_projects=60),
        "housing": SourceTuning(enabled=True, pages=1, retries=1, max_projects=20),
    },
    "thiruvananthapuram": {
        "realestateindia": SourceTuning(enabled=True, pages=4, retries=2, max_projects=90),
        "commonfloor": SourceTuning(enabled=True, pages=2, retries=1, max_projects=60),
        "housing": SourceTuning(enabled=True, pages=1, retries=1, max_projects=20),
    },
    "kozhikode": {
        "realestateindia": SourceTuning(enabled=True, pages=4, retries=2, max_projects=90),
        "commonfloor": SourceTuning(enabled=True, pages=2, retries=1, max_projects=40),
        "housing": SourceTuning(enabled=True, pages=1, retries=1, max_projects=20),
    },
    "dubai": {
        "propertyfinder": SourceTuning(enabled=True, pages=4, retries=2, max_projects=120),
        "bayut": SourceTuning(enabled=True, pages=1, retries=1, max_projects=25),
        "offplan_dubai": SourceTuning(enabled=True, pages=2, retries=1, max_projects=80),
        "extra_sources": SourceTuning(enabled=True, pages=1, retries=1, max_projects=120),
    },
    "abu-dhabi": {
        "propertyfinder": SourceTuning(enabled=True, pages=3, retries=2, max_projects=100),
        "bayut": SourceTuning(enabled=True, pages=1, retries=1, max_projects=25),
        "extra_sources": SourceTuning(enabled=True, pages=1, retries=1, max_projects=80),
    },
    "sharjah": {
        "propertyfinder": SourceTuning(enabled=True, pages=3, retries=2, max_projects=100),
        "bayut": SourceTuning(enabled=True, pages=1, retries=1, max_projects=25),
    },
    "ajman": {
        "propertyfinder": SourceTuning(enabled=True, pages=2, retries=2, max_projects=80),
        "bayut": SourceTuning(enabled=True, pages=1, retries=1, max_projects=25),
    },
    "ras-al-khaimah": {
        "propertyfinder": SourceTuning(enabled=True, pages=2, retries=2, max_projects=80),
        "bayut": SourceTuning(enabled=True, pages=1, retries=1, max_projects=25),
        "extra_sources": SourceTuning(enabled=True, pages=1, retries=1, max_projects=60),
    },
    "fujairah": {
        "propertyfinder": SourceTuning(enabled=True, pages=2, retries=2, max_projects=50),
        "bayut": SourceTuning(enabled=True, pages=1, retries=1, max_projects=25),
    },
    "umm-al-quwain": {
        "propertyfinder": SourceTuning(enabled=True, pages=2, retries=2, max_projects=50),
        "bayut": SourceTuning(enabled=True, pages=1, retries=1, max_projects=25),
    },
    "al-ain": {
        "propertyfinder": SourceTuning(enabled=True, pages=2, retries=2, max_projects=50),
        "bayut": SourceTuning(enabled=True, pages=1, retries=1, max_projects=25),
    },
    "riyadh": {
        "propertyfinder": SourceTuning(enabled=True, pages=2, retries=2, max_projects=60),
        "aqar": SourceTuning(enabled=True, pages=2, retries=1, max_projects=130),
        "bayut": SourceTuning(enabled=True, pages=2, retries=1, max_projects=45),
        "extra_sources": SourceTuning(enabled=True, pages=1, retries=1, max_projects=35),
    },
    "jeddah": {
        "propertyfinder": SourceTuning(enabled=True, pages=2, retries=2, max_projects=50),
        "aqar": SourceTuning(enabled=True, pages=2, retries=1, max_projects=90),
        "bayut": SourceTuning(enabled=True, pages=2, retries=1, max_projects=40),
    },
    "dammam": {
        "propertyfinder": SourceTuning(enabled=True, pages=2, retries=2, max_projects=40),
        "aqar": SourceTuning(enabled=True, pages=2, retries=1, max_projects=80),
        "bayut": SourceTuning(enabled=True, pages=2, retries=1, max_projects=35),
    },
    "mecca": {
        "propertyfinder": SourceTuning(enabled=True, pages=2, retries=2, max_projects=40),
        "aqar": SourceTuning(enabled=True, pages=2, retries=1, max_projects=70),
        "bayut": SourceTuning(enabled=True, pages=2, retries=1, max_projects=30),
    },
    "medina": {
        "propertyfinder": SourceTuning(enabled=True, pages=1, retries=1, max_projects=20),
        "aqar": SourceTuning(enabled=True, pages=2, retries=1, max_projects=50),
        "bayut": SourceTuning(enabled=True, pages=2, retries=1, max_projects=25),
    },
}

MANDATORY_FIELDS = [
    "id",
    "project_name",
    "builder_name",
    "city",
    "country",
    "target_city_key",
    "target_city",
    "source",
    "data_quality",
    "discovered_at",
    "updated_at",
]

STANDARD_SCHEMA_FIELDS = [
    "id",
    "project_name",
    "builder_name",
    "developer_name",
    "property_types",
    "property_category",
    "launch_status",
    "project_status",
    "price_min",
    "price_max",
    "price_currency",
    "price_per_sqft",
    "configurations",
    "total_units",
    "total_towers",
    "total_floors",
    "project_size_acres",
    "carpet_area_min_sqft",
    "carpet_area_max_sqft",
    "super_area_min_sqft",
    "super_area_max_sqft",
    "locality",
    "city",
    "district",
    "state",
    "country",
    "region",
    "target_city_key",
    "target_city",
    "latitude",
    "longitude",
    "possession_date",
    "possession_quarter",
    "completion_date",
    "completion_percentage",
    "rera_number",
    "rera_status",
    "amenities",
    "floor_plans",
    "project_description",
    "project_highlights",
    "payment_plan",
    "images",
    "brochure_url",
    "project_url",
    "builder_url",
    "source_url",
    "source",
    "data_quality",
    "discovered_at",
    "updated_at",
]

PROJECT_EXTRACT_PROMPT = """You are a real-estate extraction engine.
Extract one JSON object with exactly these keys. Use null for missing values.
No markdown, no code fences.

Keys:
- project_name (string)
- builder_name (string)
- property_types (array of strings)
- launch_status (string)
- project_status (string)
- price_min (integer)
- price_max (integer)
- price_currency (string, INR|AED|SAR|USD)
- configurations (array of strings)
- locality (string)
- city (string)
- state (string)
- country (string)
- possession_date (string YYYY-MM or YYYY-MM-DD)
- completion_date (string YYYY-MM or YYYY-MM-DD)
- rera_number (string)
- amenities (array of strings)
- payment_plan (string)
- project_description (string)
- project_highlights (array of strings)
- builder_url (string)
- brochure_url (string)
"""


def _city_slug(value: str | None) -> str:
    text = (value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text or "city"


def _available_city_sections(config_obj) -> dict[str, str]:
    out: dict[str, str] = {}
    prefix = f"{CONFIG_SECTION_PREFIX}."
    for section_name in config_obj.sections():
        if not section_name.startswith(prefix):
            continue
        city_key = section_name[len(prefix):].strip().lower()
        if city_key:
            out[city_key] = section_name
    return out


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


@dataclass(slots=True)
class PropertyProjectSettings:
    city_key: str
    city: str
    state: str
    district: str
    country: str
    region: str
    currency: str
    duckduckgo_queries: list[str]
    duckduckgo_allowed_hosts: list[str]
    realestateindia_url: str
    commonfloor_url: str
    housing_url: str
    propertyfinder_url: str
    aqar_url: str
    bayut_url: str
    offplan_dubai_url: str
    dld_project_csv_url: str
    extra_source_urls: list[str]
    pages: int
    duckduckgo_pages: int
    min_delay: float
    max_delay: float
    detail_retry: int
    request_timeout: int
    es_index: str
    data_dir: Path
    schedule_hour: str
    schedule_minute: str
    schedule_timezone: str
    source_tuning: dict[str, SourceTuning]


def _default_settings() -> PropertyProjectSettings:
    return PropertyProjectSettings(
        city_key="default",
        city="Kochi",
        state="Kerala",
        district="Ernakulam",
        country="India",
        region="Kerala",
        currency="INR",
        duckduckgo_queries=["property projects kochi"],
        duckduckgo_allowed_hosts=[],
        realestateindia_url="",
        commonfloor_url="",
        housing_url="",
        propertyfinder_url="",
        aqar_url="",
        bayut_url="",
        offplan_dubai_url="",
        dld_project_csv_url="",
        extra_source_urls=[],
        pages=2,
        duckduckgo_pages=1,
        min_delay=2.0,
        max_delay=4.0,
        detail_retry=2,
        request_timeout=30,
        es_index=DEFAULT_PROJECTS_INDEX,
        data_dir=Path("saved_data/property_projects/default"),
        schedule_hour="3",
        schedule_minute="0",
        schedule_timezone="Asia/Kolkata",
        source_tuning={k: SourceTuning(v.enabled, v.pages, v.retries, v.max_projects) for k, v in DEFAULT_SOURCE_TUNING.items()},
    )


config = read_config()
_city_sections = _available_city_sections(config)


def _load_settings_for_city(city: str) -> PropertyProjectSettings:
    raw_city = (city or "").strip().lower()
    candidate_suffixes = [raw_city, _city_slug(raw_city), raw_city.replace("-", "_"), raw_city.replace("_", "-")]
    section_name = ""
    section_suffix = ""
    for suffix in candidate_suffixes:
        if not suffix:
            continue
        cand = f"{CONFIG_SECTION_PREFIX}.{suffix}"
        if config.has_section(cand):
            section_name = cand
            section_suffix = suffix
            break
    if not section_name:
        available = ", ".join(sorted(_city_sections)) or "none"
        requested = _city_slug(raw_city) if raw_city else raw_city
        raise KeyError(f"Missing [{CONFIG_SECTION_PREFIX}.{requested}] in config.ini. Available cities: {available}")

    city_key = _city_slug(section_suffix)

    section = config[section_name]

    def _get_raw(key: str, fallback: str = "") -> str:
        try:
            return section.get(key, fallback=fallback, raw=True)
        except TypeError:
            value = section.get(key, fallback)
            return value if value is not None else fallback

    city_name = (_get_raw("city", city_key.title()) or city_key.title()).strip()
    country = (_get_raw("country", "India") or "India").strip()
    state = (_get_raw("state", "") or "").strip()
    district = (_get_raw("district", "") or "").strip()
    region = (_get_raw("region", state or country) or state or country).strip()
    currency = (_get_raw("currency", "INR") or "INR").strip().upper()
    es_index = (_get_raw("es_index", DEFAULT_PROJECTS_INDEX) or DEFAULT_PROJECTS_INDEX).strip()

    data_dir_value = (_get_raw("data_dir", "") or "").strip() or f"saved_data/property_projects/{city_key.replace('-', '_')}"
    queries = [q.strip() for q in _get_raw("duckduckgo_queries", "").split(",") if q.strip()]
    if not queries:
        queries = [f"property projects {city_name}"]

    duckduckgo_allowed_hosts = [host.strip() for host in _get_raw("duckduckgo_allowed_hosts", "").split(",") if host.strip()]
    extra_source_urls = [u.strip() for u in _get_raw("extra_source_urls", "").split(",") if u.strip()]

    city_source_defaults = CITY_SOURCE_TUNING_OVERRIDES.get(city_key, {})
    source_tuning: dict[str, SourceTuning] = {}
    for source_name in SOURCE_KEYS:
        base = city_source_defaults.get(source_name) or DEFAULT_SOURCE_TUNING.get(source_name)
        if base is None:
            base = SourceTuning(enabled=True, pages=2, retries=1, max_projects=80)

        enabled_raw = _get_raw(f"source_{source_name}_enabled", str(base.enabled).lower())
        pages_raw = _get_raw(f"source_{source_name}_pages", str(base.pages))
        retries_raw = _get_raw(f"source_{source_name}_retries", str(base.retries))
        cap_raw = _get_raw(f"source_{source_name}_max_projects", str(base.max_projects))

        try:
            pages_value = int(pages_raw)
        except Exception:
            pages_value = base.pages
        try:
            retries_value = int(retries_raw)
        except Exception:
            retries_value = base.retries
        try:
            cap_value = int(cap_raw)
        except Exception:
            cap_value = base.max_projects

        source_tuning[source_name] = SourceTuning(
            enabled=_parse_bool(enabled_raw, default=base.enabled),
            pages=max(1, pages_value),
            retries=max(1, retries_value),
            max_projects=max(1, cap_value),
        )

    return PropertyProjectSettings(
        city_key=city_key,
        city=city_name,
        state=state,
        district=district,
        country=country,
        region=region,
        currency=currency,
        duckduckgo_queries=queries,
        duckduckgo_allowed_hosts=duckduckgo_allowed_hosts,
        realestateindia_url=(_get_raw("realestateindia_url", "") or "").strip(),
        commonfloor_url=(_get_raw("commonfloor_url", "") or "").strip(),
        housing_url=(_get_raw("housing_url", "") or "").strip(),
        propertyfinder_url=(_get_raw("propertyfinder_url", "") or "").strip(),
        aqar_url=(_get_raw("aqar_url", "") or "").strip(),
        bayut_url=(_get_raw("bayut_url", "") or "").strip(),
        offplan_dubai_url=(_get_raw("offplan_dubai_url", "") or "").strip(),
        dld_project_csv_url=(_get_raw("dld_project_csv_url", "") or "").strip(),
        extra_source_urls=extra_source_urls,
        pages=max(1, int(_get_raw("pages", "5"))),
        duckduckgo_pages=max(1, int(_get_raw("duckduckgo_pages", "2"))),
        min_delay=float(_get_raw("min_delay_seconds", "2.0")),
        max_delay=float(_get_raw("max_delay_seconds", "6.0")),
        detail_retry=max(1, int(_get_raw("detail_retry_count", "3"))),
        request_timeout=max(5, int(_get_raw("request_timeout_seconds", "30"))),
        es_index=es_index,
        data_dir=Path(data_dir_value),
        schedule_hour=(_get_raw("schedule_hour", "3") or "3").strip(),
        schedule_minute=(_get_raw("schedule_minute", "0") or "0").strip(),
        schedule_timezone=(_get_raw("schedule_timezone", "Asia/Kolkata") or "Asia/Kolkata").strip(),
        source_tuning=source_tuning,
    )


DEFAULT_CITY = "kochi" if "kochi" in _city_sections else (sorted(_city_sections)[0] if _city_sections else "default")
SETTINGS = _load_settings_for_city(DEFAULT_CITY) if _city_sections else _default_settings()


_es_config = config["elasticsearch"] if config.has_section("elasticsearch") else {}
es_hosts = []
if hasattr(_es_config, "get"):
    hosts_raw = _es_config.get("host", "localhost:9200")
else:
    hosts_raw = "localhost:9200"
for host in str(hosts_raw).split(","):
    host = host.strip()
    if not host:
        continue
    if not host.startswith("http"):
        host = f"http://{host}"
    if ":" not in host.split("//")[-1]:
        host += ":9200"
    es_hosts.append(host)

es_user = _es_config.get("username", "elastic") if hasattr(_es_config, "get") else "elastic"
es_password = _es_config.get("password", "changeme") if hasattr(_es_config, "get") else "changeme"


def _activate_settings(city: str | None) -> PropertyProjectSettings:
    global SETTINGS
    if city:
        SETTINGS = _load_settings_for_city(city)
    return SETTINGS


def _activate_settings_from_records(records: list[dict[str, Any]]) -> PropertyProjectSettings:
    city_key = ""
    for rec in records:
        city_key = (_as_text(rec.get("target_city_key")) or _as_text(rec.get("_target_city")) or "").strip().lower()
        if city_key:
            break
    return _activate_settings(city_key or SETTINGS.city_key)


def _source_tuning_for(source_name: str) -> SourceTuning:
    tuned = SETTINGS.source_tuning.get(source_name)
    if tuned is None:
        tuned = DEFAULT_SOURCE_TUNING.get(source_name)
    if tuned is None:
        tuned = SourceTuning(enabled=True, pages=max(1, SETTINGS.pages), retries=1, max_projects=80)

    return SourceTuning(
        enabled=bool(tuned.enabled),
        pages=max(1, min(SETTINGS.pages, int(tuned.pages))),
        retries=max(1, int(tuned.retries)),
        max_projects=max(1, int(tuned.max_projects)),
    )


def _saved_data_dir() -> Path:
    out_dir = SETTINGS.data_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _build_http_client():
    if curl_requests is not None:
        try:
            return curl_requests.Session(), True
        except Exception:
            logger.exception("Failed to initialize curl_cffi; falling back to requests")
    return requests.Session(), False


def _session_get(session, url, *, timeout, use_curl_cffi=False):
    headers = dict(REQUEST_HEADERS)
    headers["User-Agent"] = random.choice(MODERN_USER_AGENTS)
    if use_curl_cffi:
        return session.get(url, headers=headers, timeout=timeout, impersonate=random.choice(IMPERSONATE_IDS))
    return session.get(url, headers=headers, timeout=timeout)


def _looks_blocked(text: str) -> bool:
    lowered = text.lower()
    if any(marker in lowered for marker in HARD_BLOCK_MARKERS):
        return True

    if any(marker in lowered for marker in SOFT_BLOCK_MARKERS):
        allow_markers = (
            "__next_data__",
            "new projects",
            "off-plan",
            "housing",
            "propertyfinder",
            "bayut",
            "aqar",
            "commonfloor",
            "realestateindia",
            "offplan dubai",
            "application/ld+json",
        )
        if not any(marker in lowered for marker in allow_markers):
            return True
    return False


def _fetch(session, url, *, retries=1, timeout=None, use_curl_cffi=False):
    tout = timeout or SETTINGS.request_timeout
    last_exc = None
    for attempt in range(retries):
        try:
            response = _session_get(session, url, timeout=tout, use_curl_cffi=use_curl_cffi)
            response.raise_for_status()
            text = response.text
            if _looks_blocked(text):
                raise RuntimeError("Received blocked/captcha response")
            return text
        except Exception as exc:
            last_exc = exc
            sleep_for = SETTINGS.min_delay * (attempt + 1)
            logger.warning("Request failed (%s). Retrying in %.1fs", exc, sleep_for)
            time.sleep(sleep_for)
    raise RuntimeError(f"Failed to fetch {url}: {last_exc}")


def _load_next_data(html_text: str) -> dict[str, Any] | None:
    match = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', html_text, re.S | re.I)
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


def _iter_jsonld_objects(html_text: str) -> Iterable[dict[str, Any]]:
    pattern = re.compile(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.S | re.I)
    for match in pattern.finditer(html_text):
        raw = match.group(1).strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    yield item
        elif isinstance(payload, dict):
            yield payload


def _normalize_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped.upper() == "NULL":
            return None
        return stripped
    return value


def _as_text(value: Any) -> str | None:
    value = _normalize_scalar(value)
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return str(value[0]).strip() if value else None
    if isinstance(value, dict):
        for key in ("value", "label", "name", "title", "text"):
            if key in value and value[key] not in (None, "", []):
                return str(value[key]).strip()
        return None
    return str(value).strip()


def _maybe_int(value: Any) -> int | None:
    text = _as_text(value)
    if not text:
        return None
    match = re.search(r"-?\d+", text.replace(",", ""))
    if not match:
        return None
    try:
        return int(match.group(0))
    except Exception:
        return None


def _maybe_float(value: Any) -> float | None:
    text = _as_text(value)
    if not text:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def _normalize_url(base_url: str, url: Any) -> str | None:
    text = _as_text(url)
    if not text:
        return None
    if text.startswith("http"):
        return text
    if text.startswith("www."):
        return f"https://{text}"
    if text.startswith("//"):
        return f"https:{text}"
    return urljoin(base_url, text)


def _normalize_images(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        images = [str(v).strip() for v in value if str(v).strip()]
        return images or None
    text = _as_text(value)
    if not text:
        return None
    return [text]


def _parse_area(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = _as_text(value)
    if not text:
        return None
    match = re.search(r"([0-9]+(?:[\.,][0-9]+)?)\s*(sq\.?\s*ft|sqft|sq\.?\s*m|sqm|acres?|cents?)", text, re.I)
    if not match:
        return _maybe_float(text)
    number = float(match.group(1).replace(",", ""))
    unit = match.group(2).lower().replace(" ", "")
    if "cent" in unit:
        return number * 435.6
    if "acre" in unit:
        return number * 43_560
    if "sqm" in unit or "sq.m" in unit:
        return number * 10.7639
    return number


def _detect_currency(text: str | None) -> str | None:
    if not text:
        return None
    lowered = text.lower()
    if any(token in lowered for token in ("aed", "dirham", "dhs")):
        return "AED"
    if any(token in lowered for token in ("sar", "riyal", "riyal")):
        return "SAR"
    if any(token in lowered for token in ("inr", "rs", "lakh", "lac", "crore")) or "\u20b9" in lowered:
        return "INR"
    return None


def _parse_price(value: Any, default_currency: str | None = None) -> tuple[int | None, int | None, str | None]:
    if value is None:
        return None, None, default_currency
    if isinstance(value, (int, float)):
        return int(value), None, default_currency or SETTINGS.currency

    text = _as_text(value)
    if not text:
        return None, None, default_currency

    lowered = text.lower()
    if "price on request" in lowered:
        return None, None, _detect_currency(lowered) or default_currency

    currency = _detect_currency(text) or (default_currency or SETTINGS.currency)
    cleaned = re.sub(r"(aed|sar|inr|rs\.?|dirhams?|dhs|riyals?)", " ", text, flags=re.I)
    cleaned = cleaned.replace("\u20b9", " ")

    matches = re.findall(r"([0-9]+(?:[\.,][0-9]+)?)\s*([a-zA-Z]+)?", cleaned)
    if not matches:
        return None, None, currency

    values: list[float] = []
    for number, unit in matches:
        try:
            val = float(number.replace(",", ""))
        except Exception:
            continue
        if unit:
            multiplier = PRICE_MULTIPLIERS.get(unit.strip().lower())
            if multiplier:
                val *= multiplier
        values.append(val)

    if not values:
        return None, None, currency
    if len(values) == 1:
        return int(values[0]), None, currency
    return int(min(values)), int(max(values)), currency


def _normalize_configurations(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        items = [str(v).strip() for v in value if str(v).strip()]
        return items
    text = _as_text(value)
    if not text:
        return []

    configs = []
    for part in re.split(r"[,;/|&+]", text):
        part = part.strip()
        if not part:
            continue
        match = re.match(r"(\d+)\s*(?:bhk|bed|bedroom|br)?", part, re.I)
        if match:
            configs.append(f"{match.group(1)}BHK")
        else:
            configs.append(part)
    return list(dict.fromkeys(configs))


def _normalize_property_types(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw_parts = [str(v).strip().lower() for v in value if str(v).strip()]
    else:
        text = _as_text(value)
        if not text:
            return []
        raw_parts = [part.strip().lower() for part in re.split(r"[,;/|&+]", text) if part.strip()]

    type_map = {
        "apartment": "apartment",
        "flat": "apartment",
        "villa": "villa",
        "townhouse": "townhouse",
        "row house": "townhouse",
        "duplex": "duplex",
        "penthouse": "penthouse",
        "studio": "studio",
        "plot": "plot",
        "plotted": "plotted-development",
        "land": "residential-land",
        "office": "office",
        "commercial": "commercial",
        "retail": "retail",
        "shop": "retail",
        "showroom": "showroom",
        "warehouse": "warehouse",
        "industrial": "industrial",
        "building": "building",
        "full floor": "full-floor",
    }

    normalized: list[str] = []
    for part in raw_parts:
        mapped = None
        for key, val in type_map.items():
            if key in part:
                mapped = val
                break
        if mapped:
            normalized.append(mapped)
        else:
            normalized.append(part)
    return list(dict.fromkeys(normalized))


def _normalize_amenities(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip().title() for v in value if str(v).strip()]
    text = _as_text(value)
    if not text:
        return []
    return [a.strip().title() for a in re.split(r"[,;/|]", text) if a.strip()]


def _normalize_launch_status(value: Any) -> str | None:
    text = _as_text(value)
    if not text:
        return None
    lowered = text.lower().replace("_", "-")
    lowered = re.sub(r"\s+", "-", lowered)
    if any(token in lowered for token in ("ready", "completed", "handover")):
        return "ready-to-move"
    if any(token in lowered for token in ("under-construction", "under-construction", "construction")):
        return "under-construction"
    if "pre-launch" in lowered or "prelaunch" in lowered:
        return "pre-launch"
    if "new-launch" in lowered or "newlaunch" in lowered:
        return "new-launch"
    if "upcoming" in lowered or "off-plan" in lowered or "offplan" in lowered:
        return "upcoming"
    return None


_MONTH_MAP = {
    "january": "01",
    "jan": "01",
    "february": "02",
    "feb": "02",
    "march": "03",
    "mar": "03",
    "april": "04",
    "apr": "04",
    "may": "05",
    "june": "06",
    "jun": "06",
    "july": "07",
    "jul": "07",
    "august": "08",
    "aug": "08",
    "september": "09",
    "sep": "09",
    "sept": "09",
    "october": "10",
    "oct": "10",
    "november": "11",
    "nov": "11",
    "december": "12",
    "dec": "12",
}
_QUARTER_MAP = {"q1": "03", "q2": "06", "q3": "09", "q4": "12"}


def _normalize_possession_date(value: Any) -> str | None:
    text = _as_text(value)
    if not text:
        return None
    raw = html.unescape(text).replace("\xa0", " ").strip().lower()
    raw = re.sub(r"\s+", " ", raw)
    raw = re.sub(
        r"^(date|by|in|from|starting|around|expected|estimated|possession|completion|delivery|handover|occupancy|status)\s*[:\-]?\s*",
        "",
        raw,
    ).strip(" .,:;-")

    if not raw:
        return None
    if re.search(r"\bready\W*(?:to\W*(?:move|occupy)|for\W*occupancy)\b", raw, re.I):
        return None

    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    m = re.match(r"^(\d{4})-(\d{2})$", raw)
    if m:
        return raw

    m = re.match(r"^([a-z]{3,9})[\s,\-/]+(\d{4})$", raw)
    if m:
        month = _MONTH_MAP.get(m.group(1))
        if month:
            return f"{m.group(2)}-{month}"

    m = re.match(r"^(q[1-4])[\s\-/]+(\d{4})$", raw)
    if m:
        month = _QUARTER_MAP.get(m.group(1))
        if month:
            return f"{m.group(2)}-{month}"

    m = re.match(r"^(\d{4})$", raw)
    if m:
        return f"{m.group(1)}-01"
    return None


def _extract_possession_metadata_from_text(text: str) -> dict[str, Any]:
    if not text:
        return {}
    t = html.unescape(text).replace("\xa0", " ")
    t = " ".join(t.split())
    out: dict[str, Any] = {}

    date_patterns = (
        r"(?:possession|completion|delivery|handover|occupancy)\W{0,24}(?:date|status)?\W{0,24}(?:by|in|from|starting|around|expected|estimated|on)?\W{0,24}(Q[1-4]\s*\d{4}|[A-Za-z]{3,9}\s+\d{4}|\d{4})",
        r"(?:expected|estimated)\W{0,16}(?:possession|completion|handover)?\W{0,16}(Q[1-4]\s*\d{4}|[A-Za-z]{3,9}\s+\d{4}|\d{4})",
    )

    for pattern in date_patterns:
        m = re.search(pattern, t, re.I)
        if not m:
            continue
        norm = _normalize_possession_date(m.group(1))
        if norm:
            out["possession_date"] = norm
            break

    status_match = re.search(
        r"(pre[-\s]?launch|new[-\s]?launch|under[-\s]?construction|ready[-\s]?(?:to[-\s]?move|for[-\s]?occupancy)|upcoming|off[-\s]?plan|completed)",
        t,
        re.I,
    )
    if status_match:
        normalized = _normalize_launch_status(status_match.group(1))
        if normalized:
            out["launch_status"] = normalized
    return out


def _html_to_text(html_text: str) -> str:
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html_text, flags=re.S | re.I)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _pick_ci(data: dict[str, Any], keys: Iterable[str]) -> Any:
    if not isinstance(data, dict):
        return None
    lower_to_key = {str(k).lower(): k for k in data.keys()}
    for key in keys:
        actual = lower_to_key.get(str(key).lower())
        if actual is None:
            continue
        value = data.get(actual)
        if value not in (None, "", []):
            return value
    return None


def _iter_nodes(node: Any) -> Iterable[dict[str, Any]]:
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _iter_nodes(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_nodes(item)


def _is_generic_project_name(name: str | None) -> bool:
    if not name:
        return True
    lowered = " ".join(name.lower().split())
    generic_tokens = (
        "new projects in",
        "projects in",
        "properties in",
        "property for sale",
        "buy apartments",
        "search results",
        "view details",
        "read more",
        "similar listings",
        "residential projects",
        "commercial projects",
    )
    return any(tok in lowered for tok in generic_tokens)


def _looks_like_project_entity_name(name: str | None) -> bool:
    if not name:
        return False
    lowered = " ".join(name.lower().split())
    if not lowered:
        return False
    if _is_generic_project_name(lowered):
        return False
    if lowered in {"read more", "view details", "details", "enquire now", "learn more"}:
        return False
    words = re.findall(r"\w+", lowered, flags=re.UNICODE)
    if len(words) == 1 and len(words[0]) < 5 and re.fullmatch(r"[a-z0-9]+", words[0]):
        return False
    return True


def _looks_like_project_url(url: str) -> bool:
    lowered = url.lower()
    blocked_domains = (
        "wa.me",
        "api.whatsapp.com",
        "twitter.com",
        "x.com",
        "facebook.com",
        "instagram.com",
        "youtube.com",
        "play.google.com",
    )
    if any(domain in lowered for domain in blocked_domains):
        return False
    blocked_tokens = (
        "/login",
        "/about",
        "/contact",
        "/privacy",
        "/terms",
        "/blog",
        "/faq",
        "wp-json",
        "xmlrpc",
    )
    if any(token in lowered for token in blocked_tokens):
        return False
    indicators = (
        "project",
        "property",
        "new-project",
        "off-plan",
        "offplan",
        "projects",
        "-pjid-",
        "/المشاريع-العقارية/",
        "%d8%a7%d9%84%d9%85%d8%b4%d8%a7%d8%b1%d9%8a%d8%b9-%d8%a7%d9%84%d8%b9%d9%82%d8%a7%d8%b1%d9%8a%d8%a9",
    )
    skip_extensions = (".jpg", ".jpeg", ".png", ".gif", ".svg", ".css", ".js", ".ico")
    if any(lowered.endswith(ext) for ext in skip_extensions):
        return False
    return any(ind in lowered for ind in indicators)


def _name_from_project_url(url: str) -> str | None:
    if not url:
        return None
    path = urlparse(url).path.strip("/")
    if not path:
        return None
    slug = unquote(path.split("/")[-1])
    slug = html.unescape(slug)
    slug = re.sub(r"-(?:pjid|prjid)-\d+$", "", slug, flags=re.I)
    slug = slug.replace("+", " ")
    slug = re.sub(r"[^\w\s-]", " ", slug)
    slug = slug.replace("-", " ").replace("_", " ")
    slug = re.sub(r"\s+", " ", slug).strip()
    if len(slug) < 3:
        return None
    if re.search(r"[a-zA-Z]", slug):
        return slug.title()
    return slug


def _city_aliases() -> set[str]:
    aliases: set[str] = set()

    city = (SETTINGS.city or "").strip().lower()
    city_key = (SETTINGS.city_key or "").strip().lower()

    for raw in (city, city_key):
        if not raw:
            continue
        aliases.add(raw)
        aliases.add(raw.replace("-", " "))
        aliases.add(raw.replace("_", " "))

    curated_aliases = {
        "thiruvananthapuram": {"trivandrum"},
        "kozhikode": {"calicut"},
        "mecca": {"makkah", "makkah al mukarramah", "مكة", "مكة المكرمة"},
        "medina": {"madinah", "al madinah", "المدينة", "المدينة المنورة", "المدينة-المنورة"},
        "dammam": {"eastern", "eastern province", "khobar", "al khobar", "الدمام", "الخبر", "الشرقية", "الظهران"},
        "riyadh": {"ar riyadh", "الرياض"},
        "jeddah": {"جدة"},
        "ras-al-khaimah": {"ras al khaimah", "rak"},
        "abu-dhabi": {"abu dhabi"},
        "al-ain": {"al ain"},
        "umm-al-quwain": {"umm al quwain"},
    }

    for key, vals in curated_aliases.items():
        if key in aliases or key.replace("-", " ") in aliases or key.replace("-", "_") in aliases:
            aliases.update(vals)

    # Remove very short aliases to avoid false positives.
    return {a for a in aliases if len(a.strip()) >= 3}


def _is_relevant_to_city(*texts: Any) -> bool:
    aliases = _city_aliases()
    if not aliases:
        return True
    haystack = " ".join(str(t or "") for t in texts).lower()
    haystack = haystack.replace("_", " ")
    return any(alias in haystack for alias in aliases)


def _city_slug_aliases() -> set[str]:
    out: set[str] = set()
    for alias in _city_aliases():
        slug = _city_slug(alias)
        if slug:
            out.add(slug)

    city_key_slug = _city_slug(SETTINGS.city_key)
    if city_key_slug:
        out.add(city_key_slug)

    return out


def _city_segment_matches_target(segment: str | None) -> bool:
    slug = _city_slug(segment)
    if not slug:
        return False

    candidates = {slug}
    base_slug = re.sub(r"-\d+$", "", slug)
    if base_slug:
        candidates.add(base_slug)

    target_aliases = _city_slug_aliases()
    return any(candidate in target_aliases for candidate in candidates)


def _passes_source_specific_city_filter(url: str, *, project_name: str = "", context_text: str = "") -> bool:
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").strip("/")
    path_decoded = unquote(path).lower()
    path_parts = [part for part in path_decoded.split("/") if part]

    if "uaeprojects.com" in host:
        if len(path_parts) >= 2 and path_parts[0] == "projects":
            return _city_segment_matches_target(path_parts[1])
        if len(path_parts) >= 2 and path_parts[0] == "state":
            return _city_segment_matches_target(path_parts[1])

        if path_parts:
            first = path_parts[0]
            non_city_roots = {
                "projects",
                "project",
                "state",
                "developer",
                "developers",
                "language",
                "en",
                "ar",
                "ru",
                "zh",
                "eu",
                "af",
            }
            if first not in non_city_roots and _city_segment_matches_target(first):
                return True

        return _is_relevant_to_city(project_name, url, context_text)

    return True


def _host_matches_allowed(raw_host: str, allowed_hosts: Iterable[str]) -> bool:
    host = (raw_host or "").strip().lower()
    if not host:
        return False
    if host.startswith("www."):
        host = host[4:]

    for allowed in allowed_hosts:
        token = (allowed or "").strip().lower()
        if not token:
            continue
        if token.startswith("www."):
            token = token[4:]
        if host == token or host.endswith(f".{token}"):
            return True
    return False


def _is_duckduckgo_listing_url_allowed(url: str) -> bool:
    allowed_hosts = SETTINGS.duckduckgo_allowed_hosts or []
    if not allowed_hosts:
        return True
    host = (urlparse(url).netloc or "").lower()
    return _host_matches_allowed(host, allowed_hosts)


def _clean_link_text_name(value: Any) -> str | None:
    text = _as_text(value)
    if not text:
        return None
    cleaned = html.unescape(text)
    cleaned = cleaned.replace("\xa0", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    # Keep first usable sentence/segment before CTA boilerplate.
    cleaned = re.split(r"(?:Copy Link|Get More Info|Call|Share on Facebook|Share on X|Share on WhatsApp)", cleaned, maxsplit=1, flags=re.I)[0]
    cleaned = cleaned.strip(" -|\u00a0")
    if not cleaned:
        return None
    # Arabic and English city-oriented extraction patterns.
    city_patterns = [
        rf"([A-Za-z][A-Za-z0-9\-&'\s]{{2,100}}?)\s*,\s*{re.escape(SETTINGS.city)}\b",
        r"([\u0600-\u06FF0-9\-\s]{2,100})\s*[,،]\s*مدينة\s+[\u0600-\u06FF]{2,30}",
        r"([A-Za-z][A-Za-z0-9\-&'\s]{2,100})\s+\d{2,6}$",
    ]
    for pattern in city_patterns:
        m = re.search(pattern, cleaned, re.I)
        if m:
            candidate = " ".join(m.group(1).split())
            if candidate and not _is_noise_project_name(candidate):
                return candidate

    # fallback: first small chunk
    first_chunk = re.split(r"[\.|\||\n]", cleaned, maxsplit=1)[0].strip()
    if first_chunk and len(first_chunk) <= 120 and not _is_noise_project_name(first_chunk):
        return first_chunk
    return None


def _extract_anchor_attribute(anchor_html: str, attr_name: str) -> str | None:
    pattern = rf"\b{re.escape(attr_name)}\s*=\s*([\"'])(.*?)\1"
    match = re.search(pattern, anchor_html, re.I | re.S)
    if not match:
        return None
    value = html.unescape(match.group(2))
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def _looks_like_project_detail_url(url: str) -> bool:
    lowered = url.lower()
    parsed = urlparse(lowered)
    path = parsed.path or ""
    host = parsed.netloc or ""

    aqar_ar_prefix = "/المشاريع-العقارية/"
    aqar_en_prefix = "/%d8%a7%d9%84%d9%85%d8%b4%d8%a7%d8%b1%d9%8a%d8%b9-%d8%a7%d9%84%d8%b9%d9%82%d8%a7%d8%b1%d9%8a%d8%a9/"
    if aqar_ar_prefix in path or aqar_en_prefix in path:
        segments = [seg for seg in path.strip("/").split("/") if seg]
        return len(segments) >= 3

    listing_patterns = (
        "/projects?page",
        "/new-projects?page",
        "/search",
        "/for-sale",
        "/for-rent",
        "/properties",
        "/project-search",
        "/projects/apartment-ht/",
        "/projects/villa-ht/",
        "/projects/commercial-ht/",
        "/in/projects/",
        "/new-projects/lp/",
        "/new-projects/dev/",
        "/new-projects/dev-lp/",
        "/new-projects/dev-list/",
        "/new-projects/search/",
        "/transactions/",
    )
    if any(token in lowered for token in listing_patterns):
        return False

    normalized_path = path.rstrip("/")
    if normalized_path in {"/new-projects", "/en/new-projects", "/offplan-projects-in-dubai", "/projects"}:
        return False
    if normalized_path.endswith("/new-projects") or normalized_path.endswith("/projects"):
        return False

    detail_indicators = (
        "-pjid-",
        "-cfid",
        "/projects/",
        "/new-projects/",
        "/property/details",
        "/povp-",
        "/offplan-",
    )
    if any(ind in lowered for ind in detail_indicators):
        return True

    if "offplan-dubai.com" in host:
        segment = path.strip("/")
        if not segment:
            return False
        blocked_prefixes = (
            "dubai-area/",
            "dubai-developer/",
            "dubai-real-estate-developers",
            "property-type/",
            "property-type",
            "offplan-projects-in-dubai",
            "areas",
            "blog",
            "contact-us",
            "privacy-policy",
            "terms-and-conditions",
            "feed",
            "wp-",
            "xmlrpc",
            "comments",
        )
        if any(segment.startswith(prefix) for prefix in blocked_prefixes):
            return False
        if "/" not in segment and "-" in segment:
            return True

    return False


def _is_noise_project_name(name: str | None) -> bool:
    text = (name or "").strip().lower()
    if not text:
        return True
    noise_tokens = (
        "sold house prices",
        "offplan dubai",
        "show all",
        "all projects",
        "new projects",
        "riyadh",
        "jeddah",
        "dubai",
        "abu dhabi",
        "sharjah",
        "ajman",
        "ras al khaimah",
        "fujairah",
        "umm al quwain",
        "al ain",
        "kochi",
        "thiruvananthapuram",
        "trivandrum",
        "kozhikode",
        "calicut",
        "dammam",
        "mecca",
        "makkah",
        "medina",
        "madinah",
    )
    if text in noise_tokens:
        return True
    if text.startswith("show ") and "properties" in text:
        return True
    if text.startswith("share on facebook"):
        return True
    if "copy link" in text and "get more info" in text:
        return True
    return False


def _split_location_parts(value: Any) -> list[str]:
    text = _as_text(value)
    if not text:
        return []
    parts = [part.strip() for part in text.split(",") if part and part.strip()]
    return parts


def _normalize_city_label(value: Any) -> str | None:
    text = _as_text(value)
    if not text:
        return None
    lowered = text.strip().lower()
    mapping = {
        "ar riyadh": "Riyadh",
        "riyadh": "Riyadh",
        "makkah": "Mecca",
        "makkah al mukarramah": "Mecca",
        "mecca": "Mecca",
        "madinah": "Medina",
        "al madinah": "Medina",
        "medina": "Medina",
        "abu dhabi": "Abu Dhabi",
        "al ain": "Al Ain",
        "ras al khaimah": "Ras Al Khaimah",
        "umm al quwain": "Umm Al Quwain",
        "thiruvananthapuram": "Thiruvananthapuram",
        "trivandrum": "Thiruvananthapuram",
        "kozhikode": "Kozhikode",
        "calicut": "Kozhikode",
    }
    return mapping.get(lowered, text.strip())


_BUILDER_NOISE_TAIL_PATTERNS = (
    r"\s*-\s*(?:realestateindia|commonfloor|housing|propertyfinder|bayut|aqar)\b.*$",
    r"\s+(?:search\s+from|search\s+over)\b.*$",
    r"\s*-\s*get\s+complete\s+.*$",
)

_BUILDER_NOISE_PHRASES = (
    "realestateindia.com",
    "search from",
    "search over",
    "get complete",
    "project details",
    "copy link",
    "get more info",
)

_BUILDER_HINT_TOKENS = {
    "builder",
    "builders",
    "developer",
    "developers",
    "realtor",
    "realtors",
    "properties",
    "property",
    "realty",
    "group",
    "homes",
    "infra",
    "construction",
    "constructions",
    "estates",
    "llp",
    "ltd",
    "limited",
    "pvt",
    "inc",
    "company",
}

_BUILDER_CONNECTOR_TOKENS = {
    "and",
    "or",
    "with",
    "for",
    "from",
    "by",
    "at",
    "in",
    "on",
    "to",
    "the",
    "of",
}


def _normalize_builder_name(value: Any) -> str | None:
    text = _as_text(value)
    if not text:
        return None

    cleaned = html.unescape(text)
    cleaned = re.sub(r"^\d+[\).\-\s]+", "", cleaned)
    cleaned = re.sub(r"\s*\|.*$", "", cleaned)
    for pattern in _BUILDER_NOISE_TAIL_PATTERNS:
        cleaned = re.sub(pattern, "", cleaned, flags=re.I)
    cleaned = cleaned.strip(" -,:|")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return None

    lowered = cleaned.lower()
    if lowered.startswith(("unknown", "none", "nil", "n/a", "na")):
        return None
    if any(phrase in lowered for phrase in _BUILDER_NOISE_PHRASES):
        return None
    if len(lowered) > 64:
        return None

    tokens = [token for token in re.split(r"[^a-z0-9]+", lowered) if token]
    if not tokens or len(tokens) > 8:
        return None

    if tokens[0] in _BUILDER_CONNECTOR_TOKENS:
        return None

    hint_count = sum(1 for token in tokens if token in _BUILDER_HINT_TOKENS)
    if hint_count == 0 and len(tokens) > 2:
        return None
    if hint_count == 0 and len(tokens) == 1 and len(tokens[0]) < 4:
        return None

    return cleaned


def _extract_propertyfinder_projects_from_next_data(next_data: dict[str, Any], country_code: str) -> list[dict[str, Any]]:
    page_props = ((next_data or {}).get("props") or {}).get("pageProps") or {}
    search_result = page_props.get("searchResult") if isinstance(page_props, dict) else None
    if not isinstance(search_result, dict):
        return []

    data = search_result.get("data")
    if isinstance(data, dict):
        projects = data.get("projects") if isinstance(data.get("projects"), list) else []
    elif isinstance(data, list):
        projects = data
    else:
        projects = []

    if not projects:
        return []

    domain = "https://www.propertyfinder.ae" if country_code.upper() == "UAE" else "https://www.propertyfinder.sa"
    source = "propertyfinder_ae" if country_code.upper() == "UAE" else "propertyfinder_sa"

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in projects:
        if not isinstance(row, dict):
            continue

        share_url = _normalize_url(domain, row.get("shareUrl"))
        if not share_url or not _looks_like_project_detail_url(share_url):
            continue

        project_name = _as_text(row.get("title"))
        if _is_noise_project_name(project_name):
            continue

        developer = row.get("developer") if isinstance(row.get("developer"), dict) else {}
        builder_name = _normalize_builder_name(developer.get("name"))
        location = row.get("location") if isinstance(row.get("location"), dict) else {}
        location_full = _as_text(location.get("fullName"))
        location_parts = _split_location_parts(location_full)
        city = _normalize_city_label(location_parts[0]) if location_parts else None
        locality = _as_text(location_parts[1] if len(location_parts) > 1 else None)
        if not locality and len(location_parts) > 2:
            locality = _as_text(location_parts[2])

        if not _is_relevant_to_city(project_name, share_url, city, locality, location_full):
            continue

        coords = location.get("coordinates") if isinstance(location.get("coordinates"), dict) else {}
        lat = _maybe_float(coords.get("lat") or coords.get("latitude"))
        lng = _maybe_float(coords.get("lng") or coords.get("lon") or coords.get("longitude"))

        pr = row.get("priceRange") if isinstance(row.get("priceRange"), dict) else {}
        pmin = _maybe_int(pr.get("min")) or _maybe_int(row.get("startingPrice"))
        pmax = _maybe_int(pr.get("max"))
        pcur = SETTINGS.currency

        amenities_raw = row.get("amenities") if isinstance(row.get("amenities"), list) else []
        amenities = []
        for amenity in amenities_raw:
            if isinstance(amenity, dict):
                name = _as_text(amenity.get("name"))
                if name:
                    amenities.append(name)

        payment_plans = row.get("paymentPlans") if isinstance(row.get("paymentPlans"), list) else []
        payment_plan = ", ".join([_as_text(x) for x in payment_plans if _as_text(x)]) or None
        property_types = _normalize_property_types(row.get("propertyTypes"))
        bedrooms = row.get("bedrooms") if isinstance(row.get("bedrooms"), list) else []
        configurations = _normalize_configurations([f"{b}BHK" for b in bedrooms if _as_text(b)])
        delivery_date = _normalize_possession_date(row.get("deliveryDate"))
        launch_status = _normalize_launch_status(row.get("constructionPhase") or row.get("stockAvailability") or row.get("salesPhase"))
        images = _normalize_images(row.get("images"))

        rec: dict[str, Any] = {
            "project_name": project_name,
            "builder_name": builder_name,
            "developer_name": builder_name,
            "project_url": share_url,
            "source_url": share_url,
            "source": source,
            "price_min": pmin,
            "price_max": pmax,
            "price_currency": pcur,
            "property_types": property_types,
            "configurations": configurations,
            "amenities": amenities,
            "payment_plan": payment_plan,
            "launch_status": launch_status,
            "possession_date": delivery_date,
            "completion_date": delivery_date,
            "city": city,
            "locality": locality,
            "latitude": lat,
            "longitude": lng,
            "images": images,
            "project_description": _as_text(row.get("description")),
            "data_quality": "detail",
        }

        key = rec.get("project_url")
        if not key or key in seen:
            continue
        seen.add(key)
        out.append({k: v for k, v in rec.items() if v not in (None, "", [])})
    return out


def _extract_bayut_projects_from_next_data(next_data: dict[str, Any], country_code: str) -> list[dict[str, Any]]:
    domain = "https://www.bayut.com" if country_code.upper() == "UAE" else "https://www.bayut.sa"
    source = "bayut_uae" if country_code.upper() == "UAE" else "bayut_ksa"

    page_props = ((next_data or {}).get("props") or {}).get("pageProps") or {}
    store_state = page_props.get("storeState") if isinstance(page_props, dict) else None
    if not isinstance(store_state, dict):
        return []

    content_api = store_state.get("contentApi") if isinstance(store_state.get("contentApi"), dict) else {}
    queries = content_api.get("queries") if isinstance(content_api.get("queries"), dict) else {}
    if not queries:
        return []

    project_rows: list[dict[str, Any]] = []
    for value in queries.values():
        if not isinstance(value, dict):
            continue
        data = value.get("data") if isinstance(value.get("data"), dict) else {}
        rows = data.get("projects") if isinstance(data.get("projects"), list) else []
        for row in rows:
            if isinstance(row, dict):
                project_rows.append(row)

    if not project_rows:
        return []

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in project_rows:
        pid = _maybe_int(row.get("id"))
        title = _as_text(row.get("title"))
        if _is_noise_project_name(title):
            continue

        slug_raw = (_as_text(row.get("slug")) or "").strip()
        if not slug_raw:
            continue
        slug = slug_raw.lower()
        if pid is not None:
            suffix = f"-{pid}-{pid}"
            if slug.endswith(suffix):
                slug = slug[: -len(suffix)] + f"-{pid}"
            elif not slug.endswith(f"-{pid}"):
                slug = f"{slug}-{pid}"
        project_url = _normalize_url(domain, f"/en/new-projects/{slug}/")
        if not project_url or not _looks_like_project_detail_url(project_url):
            continue

        city_name = _normalize_city_label(row.get("city_name") or row.get("city_name_l1"))
        locality = _as_text(row.get("location_name") or row.get("location_name_l1"))
        if not _is_relevant_to_city(title, project_url, city_name, locality):
            continue

        pmin = _maybe_int(row.get("min_price"))
        pmax = _maybe_int(row.get("max_price"))
        images = _normalize_images([img.get("url") for img in row.get("image_list", []) if isinstance(img, dict)])

        property_types_raw = row.get("property_types") if isinstance(row.get("property_types"), list) else []
        property_types: list[str] = []
        configurations: list[str] = []
        for item in property_types_raw:
            if not isinstance(item, dict):
                continue
            ptype = _as_text(item.get("type_title") or item.get("type_title_l1"))
            if ptype:
                property_types.extend(_normalize_property_types(ptype))
            area_min = _maybe_float(item.get("min_area"))
            area_max = _maybe_float(item.get("max_area"))
            if area_min is not None and area_max is not None and area_min == area_max:
                configurations.append(str(area_min))

        rec: dict[str, Any] = {
            "project_name": title,
            "project_url": project_url,
            "source_url": project_url,
            "source": source,
            "city": city_name,
            "locality": locality,
            "builder_name": _normalize_builder_name(row.get("developer_name")),
            "property_types": list(dict.fromkeys(property_types)),
            "configurations": list(dict.fromkeys(configurations)),
            "price_min": pmin,
            "price_max": pmax,
            "price_currency": SETTINGS.currency,
            "project_description": _as_text(row.get("description") or row.get("description_l1")),
            "launch_status": _normalize_launch_status(row.get("construction_status") or row.get("project_status")),
            "images": images,
            "data_quality": "detail",
        }

        key = rec.get("project_url")
        if not key or key in seen:
            continue
        seen.add(key)
        out.append({k: v for k, v in rec.items() if v not in (None, "", [])})

    return out


def _derive_property_category(types: list[str]) -> str:
    if not types:
        return "residential"
    has_commercial = any(t in COMMERCIAL_TYPE_TOKENS for t in types)
    has_residential = any(t in RESIDENTIAL_TYPE_TOKENS for t in types)
    if has_commercial and has_residential:
        return "mixed-use"
    if has_commercial:
        return "commercial"
    return "residential"


def _extract_project_from_candidate(
    candidate: dict[str, Any],
    *,
    base_url: str,
    source: str,
    enforce_city_relevance: bool = False,
) -> dict[str, Any] | None:
    if not any(_pick_ci(candidate, keys) is not None for keys in (NAME_KEYS, URL_KEYS, BUILDER_KEYS, PRICE_KEYS, TYPE_KEYS)):
        return None

    project_name = _as_text(_pick_ci(candidate, NAME_KEYS))
    project_url = _normalize_url(base_url, _pick_ci(candidate, URL_KEYS))

    if not project_name and project_url:
        project_name = _name_from_project_url(project_url)

    if not project_name and not project_url:
        return None

    if not project_url:
        return None

    if _is_noise_project_name(project_name):
        return None

    if project_name and len(project_name) > 220:
        return None

    if project_url and not _looks_like_project_detail_url(project_url):
        return None

    if project_name and _is_generic_project_name(project_name):
        if project_url:
            project_name = _name_from_project_url(project_url)

    if project_name and not _looks_like_project_entity_name(project_name):
        return None

    if project_url and not _looks_like_project_url(project_url) and not project_name:
        return None

    builder_name = _normalize_builder_name(_pick_ci(candidate, BUILDER_KEYS))
    types = _normalize_property_types(_pick_ci(candidate, TYPE_KEYS))
    configs = _normalize_configurations(_pick_ci(candidate, CONFIG_KEYS))
    locality = _as_text(_pick_ci(candidate, LOCALITY_KEYS))
    city = _as_text(_pick_ci(candidate, CITY_KEYS))
    state = _as_text(_pick_ci(candidate, STATE_KEYS))
    country = _as_text(_pick_ci(candidate, COUNTRY_KEYS))
    possession = _normalize_possession_date(_pick_ci(candidate, POSSESSION_KEYS))
    launch_status = _normalize_launch_status(_pick_ci(candidate, STATUS_KEYS))
    rera = _as_text(_pick_ci(candidate, RERA_KEYS))
    description = _as_text(_pick_ci(candidate, DESCRIPTION_KEYS))
    amenities = _normalize_amenities(_pick_ci(candidate, AMENITY_KEYS))
    images = _normalize_images(_pick_ci(candidate, IMAGE_KEYS))
    lat = _maybe_float(_pick_ci(candidate, LAT_KEYS))
    lng = _maybe_float(_pick_ci(candidate, LNG_KEYS))
    pmin, pmax, pcur = _parse_price(_pick_ci(candidate, PRICE_KEYS), default_currency=SETTINGS.currency)

    if enforce_city_relevance and not _is_relevant_to_city(project_name, project_url, locality, city, state, country):
        return None

    record: dict[str, Any] = {
        "project_name": project_name,
        "project_url": project_url,
        "source_url": project_url,
        "source": source,
        "builder_name": builder_name,
        "property_types": types,
        "configurations": configs,
        "locality": locality,
        "city": city,
        "state": state,
        "country": country,
        "possession_date": possession,
        "launch_status": launch_status,
        "rera_number": rera,
        "project_description": description,
        "amenities": amenities,
        "images": images,
        "latitude": lat,
        "longitude": lng,
        "price_min": pmin,
        "price_max": pmax,
        "price_currency": pcur,
        "data_quality": "listing",
    }

    return {k: v for k, v in record.items() if v not in (None, "", [])}


def _extract_projects_from_json(
    payload: dict[str, Any],
    *,
    base_url: str,
    source: str,
    enforce_city_relevance: bool = False,
) -> list[dict[str, Any]]:
    projects: list[dict[str, Any]] = []
    seen: set[str] = set()
    for node in _iter_nodes(payload):
        rec = _extract_project_from_candidate(
            node,
            base_url=base_url,
            source=source,
            enforce_city_relevance=enforce_city_relevance,
        )
        if not rec:
            continue
        dedupe_key = rec.get("project_url") or rec.get("project_name")
        if not dedupe_key or dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        projects.append(rec)
    return projects


def _extract_detail_from_next_data(next_data: dict[str, Any]) -> dict[str, Any]:
    best_node: dict[str, Any] | None = None
    best_score = 0

    for node in _iter_nodes(next_data):
        if not isinstance(node, dict):
            continue

        score = 0
        if _pick_ci(node, NAME_KEYS):
            score += 4
        if _pick_ci(node, PRICE_KEYS):
            score += 3
        if _pick_ci(node, BUILDER_KEYS):
            score += 2
        if _pick_ci(node, CONFIG_KEYS):
            score += 2
        if _pick_ci(node, RERA_KEYS):
            score += 2
        if _pick_ci(node, LOCALITY_KEYS):
            score += 1
        if _pick_ci(node, DESCRIPTION_KEYS):
            score += 1
        if _pick_ci(node, AMENITY_KEYS):
            score += 1
        if _pick_ci(node, LAT_KEYS) or _pick_ci(node, LNG_KEYS):
            score += 1

        if score > best_score:
            best_score = score
            best_node = node

    if not best_node or best_score < 4:
        return {}

    pmin, pmax, pcur = _parse_price(_pick_ci(best_node, PRICE_KEYS), default_currency=SETTINGS.currency)

    detail: dict[str, Any] = {
        "project_name": _as_text(_pick_ci(best_node, NAME_KEYS)),
        "builder_name": _normalize_builder_name(_pick_ci(best_node, BUILDER_KEYS)),
        "property_types": _normalize_property_types(_pick_ci(best_node, TYPE_KEYS)),
        "configurations": _normalize_configurations(_pick_ci(best_node, CONFIG_KEYS)),
        "locality": _as_text(_pick_ci(best_node, LOCALITY_KEYS)),
        "city": _as_text(_pick_ci(best_node, CITY_KEYS)),
        "state": _as_text(_pick_ci(best_node, STATE_KEYS)),
        "country": _as_text(_pick_ci(best_node, COUNTRY_KEYS)),
        "possession_date": _normalize_possession_date(_pick_ci(best_node, POSSESSION_KEYS)),
        "launch_status": _normalize_launch_status(_pick_ci(best_node, STATUS_KEYS)),
        "rera_number": _as_text(_pick_ci(best_node, RERA_KEYS)),
        "project_description": _as_text(_pick_ci(best_node, DESCRIPTION_KEYS)),
        "amenities": _normalize_amenities(_pick_ci(best_node, AMENITY_KEYS)),
        "images": _normalize_images(_pick_ci(best_node, IMAGE_KEYS)),
        "latitude": _maybe_float(_pick_ci(best_node, LAT_KEYS)),
        "longitude": _maybe_float(_pick_ci(best_node, LNG_KEYS)),
        "price_min": pmin,
        "price_max": pmax,
        "price_currency": pcur,
    }
    return {k: v for k, v in detail.items() if v not in (None, "", [])}


def _extract_projects_from_jsonld(
    html_text: str,
    *,
    base_url: str,
    source: str,
    enforce_city_relevance: bool = False,
) -> list[dict[str, Any]]:
    projects: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _add(rec: dict[str, Any] | None):
        if not rec:
            return
        key = rec.get("project_url") or rec.get("project_name")
        if not key or key in seen:
            return
        seen.add(key)
        projects.append(rec)

    for obj in _iter_jsonld_objects(html_text):
        obj_type = _as_text(obj.get("@type"))
        obj_type_l = (obj_type or "").lower()

        if obj_type_l == "itemlist":
            for item in obj.get("itemListElement", []):
                if isinstance(item, dict):
                    data = item.get("item") if isinstance(item.get("item"), dict) else item
                    rec = _extract_project_from_candidate(
                        data,
                        base_url=base_url,
                        source=source,
                        enforce_city_relevance=enforce_city_relevance,
                    )
                    _add(rec)
            continue

        rec = _extract_project_from_candidate(
            obj,
            base_url=base_url,
            source=source,
            enforce_city_relevance=enforce_city_relevance,
        )
        _add(rec)
    return projects


def _extract_projects_from_links(
    html_text: str,
    *,
    base_url: str,
    source: str,
    allowed_link_tokens: tuple[str, ...] = (),
    enforce_city_relevance: bool = False,
) -> list[dict[str, Any]]:
    projects: list[dict[str, Any]] = []
    seen: set[str] = set()

    link_pattern = re.compile(r'(<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>)', re.S | re.I)
    for match in link_pattern.finditer(html_text):
        anchor_html = match.group(1)
        href = match.group(2)
        inner_html = match.group(3)
        raw_name = re.sub(r"<[^>]+>", "", inner_html).strip()
        title_name = _extract_anchor_attribute(anchor_html, "title")
        aria_name = _extract_anchor_attribute(anchor_html, "aria-label")
        name = (
            _clean_link_text_name(title_name)
            or _clean_link_text_name(aria_name)
            or _clean_link_text_name(raw_name)
            or raw_name
        )
        if not href:
            continue
        url = _normalize_url(base_url, href)
        if not url:
            continue

        if allowed_link_tokens and not any(token in url.lower() for token in allowed_link_tokens):
            continue
        if not _looks_like_project_url(url):
            continue
        if not _looks_like_project_detail_url(url):
            continue

        parsed = urlparse(url)
        path_value = parsed.path or ""
        path_lower = path_value.lower()
        path_unquoted = unquote(path_value).lower()
        if "aqar.fm" in (parsed.netloc or "").lower() and (
            "/%d8%a7%d9%84%d9%85%d8%b4%d8%a7%d8%b1%d9%8a%d8%b9-%d8%a7%d9%84%d8%b9%d9%82%d8%a7%d8%b1%d9%8a%d8%a9/" in path_lower
            or "/المشاريع-العقارية/" in path_unquoted
        ):
            name = _name_from_project_url(url) or name

        if _is_generic_project_name(name):
            name = _name_from_project_url(url) or name
        if _is_noise_project_name(name):
            name = _name_from_project_url(url) or name
        if not _looks_like_project_entity_name(name):
            continue

        snippet = html_text[max(0, match.start() - 800):match.end() + 1000]
        pmin, pmax, pcur = _parse_price(snippet, default_currency=SETTINGS.currency)
        configs = _normalize_configurations(" ".join(re.findall(r"\d+\s*BHK", snippet, re.I)))
        pmeta = _extract_possession_metadata_from_text(snippet)

        if enforce_city_relevance:
            if not _is_relevant_to_city(name, url, snippet):
                continue
            if not _passes_source_specific_city_filter(url, project_name=name, context_text=snippet):
                continue

        rec = {
            "project_name": name,
            "project_url": url,
            "source_url": url,
            "source": source,
            "price_min": pmin,
            "price_max": pmax,
            "price_currency": pcur,
            "configurations": configs,
            "possession_date": pmeta.get("possession_date"),
            "launch_status": pmeta.get("launch_status"),
            "data_quality": "listing",
        }
        key = rec["project_url"]
        if key in seen:
            continue
        seen.add(key)
        projects.append({k: v for k, v in rec.items() if v not in (None, "", [])})

    return projects


def _parse_generic_listing_html(
    html_text: str,
    *,
    base_url: str,
    source: str,
    allowed_link_tokens: tuple[str, ...] = (),
    enforce_city_relevance: bool = False,
) -> list[dict[str, Any]]:
    projects: list[dict[str, Any]] = []

    next_data = _load_next_data(html_text)
    if next_data:
        projects.extend(
            _extract_projects_from_json(
                next_data,
                base_url=base_url,
                source=source,
                enforce_city_relevance=enforce_city_relevance,
            )
        )

    projects.extend(
        _extract_projects_from_jsonld(
            html_text,
            base_url=base_url,
            source=source,
            enforce_city_relevance=enforce_city_relevance,
        )
    )
    projects.extend(
        _extract_projects_from_links(
            html_text,
            base_url=base_url,
            source=source,
            allowed_link_tokens=allowed_link_tokens,
            enforce_city_relevance=enforce_city_relevance,
        )
    )

    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for rec in projects:
        key = rec.get("project_url") or rec.get("project_name")
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(rec)
    return deduped


def parse_duckduckgo_serp(html_text: str) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    ddg_link_pattern = re.compile(r'class="result__a"[^>]+href="([^"]+)"', re.S)
    for match in ddg_link_pattern.finditer(html_text):
        raw_url = match.group(1)
        if raw_url.startswith("//duckduckgo.com/l/?uddg="):
            encoded = raw_url.split("uddg=")[1].split("&")[0]
            url = unquote(encoded)
        elif raw_url.startswith("http"):
            url = raw_url
        else:
            continue
        title_match = re.search(r'class="result__a"[^>]*>(.*?)</a>', html_text[match.start():match.start() + 500], re.S)
        name = re.sub(r"<[^>]+>", "", title_match.group(1)).strip() if title_match else ""
        if _looks_like_project_url(url):
            results.append({"name": name or "", "url": url, "source": "duckduckgo"})

    if not results:
        all_hrefs = re.findall(r'href="([^"]+)"', html_text)
        for raw_url in all_hrefs:
            if "uddg=" in raw_url:
                encoded = raw_url.split("uddg=")[1].split("&")[0]
                url = unquote(encoded)
            elif raw_url.startswith("http"):
                url = raw_url
            else:
                continue
            if _looks_like_project_url(url):
                results.append({"name": "", "url": url, "source": "duckduckgo"})

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in results:
        if row["url"] in seen:
            continue
        seen.add(row["url"])
        deduped.append(row)
    return deduped


def parse_realestateindia_listing(html_text: str) -> list[dict[str, Any]]:
    return _parse_generic_listing_html(
        html_text,
        base_url="https://www.realestateindia.com",
        source="realestateindia",
        allowed_link_tokens=("/projects/", "-pjid-"),
        enforce_city_relevance=True,
    )


def parse_commonfloor_listing(html_text: str) -> list[dict[str, Any]]:
    return _parse_generic_listing_html(
        html_text,
        base_url="https://www.commonfloor.com",
        source="commonfloor",
        allowed_link_tokens=("project", "cfid", "/property/"),
        enforce_city_relevance=True,
    )


def parse_housing_listing(html_text: str) -> list[dict[str, Any]]:
    return _parse_generic_listing_html(
        html_text,
        base_url="https://housing.com",
        source="housing",
        allowed_link_tokens=("project", "prjid", "/in/projects/"),
        enforce_city_relevance=True,
    )


def parse_propertyfinder_listing(html_text: str, country_code: str) -> list[dict[str, Any]]:
    domain = "https://www.propertyfinder.ae" if country_code.upper() == "UAE" else "https://www.propertyfinder.sa"
    source = "propertyfinder_ae" if country_code.upper() == "UAE" else "propertyfinder_sa"

    next_data = _load_next_data(html_text)
    if next_data:
        specialized = _extract_propertyfinder_projects_from_next_data(next_data, country_code)
        if specialized:
            return specialized

    return _parse_generic_listing_html(
        html_text,
        base_url=domain,
        source=source,
        allowed_link_tokens=("/new-projects/", "/project", "property"),
        enforce_city_relevance=True,
    )


def parse_aqar_listing(html_text: str) -> list[dict[str, Any]]:
    return _parse_generic_listing_html(
        html_text,
        base_url="https://aqar.fm",
        source="aqar",
        allowed_link_tokens=(
            "/المشاريع-العقارية/",
            "%d8%a7%d9%84%d9%85%d8%b4%d8%a7%d8%b1%d9%8a%d8%b9-%d8%a7%d9%84%d8%b9%d9%82%d8%a7%d8%b1%d9%8a%d8%a9",
            "project",
        ),
        enforce_city_relevance=True,
    )


def parse_bayut_listing(html_text: str, country_code: str) -> list[dict[str, Any]]:
    domain = "https://www.bayut.com" if country_code.upper() == "UAE" else "https://www.bayut.sa"
    source = "bayut_uae" if country_code.upper() == "UAE" else "bayut_ksa"

    next_data = _load_next_data(html_text)
    if next_data:
        specialized = _extract_bayut_projects_from_next_data(next_data, country_code)
        if specialized:
            return specialized

    return _parse_generic_listing_html(
        html_text,
        base_url=domain,
        source=source,
        allowed_link_tokens=("/new-project", "/property/details", "project"),
        enforce_city_relevance=True,
    )


def parse_offplan_dubai_listing(html_text: str) -> list[dict[str, Any]]:
    return _parse_generic_listing_html(
        html_text,
        base_url="https://www.offplan-dubai.com",
        source="offplan_dubai",
        allowed_link_tokens=("offplan", "projects", "dubai"),
        enforce_city_relevance=True,
    )


def parse_dld_projects_csv(csv_text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not csv_text or "," not in csv_text:
        return rows

    def get_val(row: dict[str, Any], aliases: tuple[str, ...]) -> Any:
        lower_map = {str(k).lower(): k for k in row.keys()}
        for alias in aliases:
            actual = lower_map.get(alias.lower())
            if actual is None:
                continue
            value = row.get(actual)
            if value not in (None, "", []):
                return value
        return None

    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        project_name = _as_text(get_val(row, ("Project Name", "project_name", "Project")))
        if not project_name:
            continue
        developer_name = _normalize_builder_name(get_val(row, ("Developer Name", "developer_name", "Developer")))
        project_type = _as_text(get_val(row, ("Project Type", "project_type", "Type")))
        project_status = _as_text(get_val(row, ("Project Status", "project_status", "Status")))
        completion_percentage = _maybe_float(get_val(row, ("Completed %", "completion_percentage", "Completion %")))
        completion_date = _normalize_possession_date(get_val(row, ("Completion Date", "End Date", "end_date")))
        project_value = _as_text(get_val(row, ("Project Value", "project_value", "Value")))
        pmin, pmax, _ = _parse_price(project_value, default_currency="AED")

        rec: dict[str, Any] = {
            "project_name": project_name,
            "builder_name": developer_name,
            "developer_name": developer_name,
            "property_types": _normalize_property_types(project_type),
            "project_status": project_status,
            "launch_status": _normalize_launch_status(project_status),
            "completion_percentage": completion_percentage,
            "completion_date": completion_date,
            "price_min": pmin,
            "price_max": pmax,
            "price_currency": "AED",
            "city": "Dubai",
            "state": "Dubai",
            "country": "UAE",
            "source": "dld_open_data",
            "source_url": SETTINGS.dld_project_csv_url or None,
            "data_quality": "detail",
        }
        rows.append({k: v for k, v in rec.items() if v not in (None, "", [])})

    return rows


def _select_listing_parser(url: str, *, source_override: str | None = None):
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    scheme = parsed.scheme or "https"
    domain = f"{scheme}://{parsed.netloc}" if parsed.netloc else ""

    if "realestateindia" in host:
        return parse_realestateindia_listing
    if "commonfloor" in host:
        return parse_commonfloor_listing
    if "housing.com" in host:
        return parse_housing_listing
    if "propertyfinder" in host:
        return lambda html_text: parse_propertyfinder_listing(html_text, SETTINGS.country)
    if "aqar.fm" in host:
        return parse_aqar_listing
    if "bayut" in host:
        return lambda html_text: parse_bayut_listing(html_text, SETTINGS.country)
    if "offplan-dubai" in host:
        return parse_offplan_dubai_listing

    if "uaeprojects.com" in host:
        return lambda html_text: _parse_generic_listing_html(
            html_text,
            base_url=domain,
            source=source_override or "uaeprojects",
            allowed_link_tokens=("/projects/",),
            enforce_city_relevance=True,
        )

    if "madaproperties.sa" in host:
        return lambda html_text: _parse_generic_listing_html(
            html_text,
            base_url=domain,
            source=source_override or "madaproperties",
            allowed_link_tokens=("/projects/",),
            enforce_city_relevance=True,
        )

    return lambda html_text: _extract_projects_from_links(
        html_text,
        base_url=domain,
        source=source_override or "discovered_listing",
        allowed_link_tokens=("project", "property", "offplan", "-pjid-"),
        enforce_city_relevance=True,
    )


def _build_source_url(source_url: str, page: int) -> str:
    base = source_url.strip()
    if not base:
        return base
    if "{page}" in base:
        return base.format(page=page)
    if page <= 1:
        return base

    separator = "&" if "?" in base else "?"
    return f"{base}{separator}page={page}"


def parse_project_detail_page(html_text: str, source: str = "") -> dict[str, Any]:
    detail: dict[str, Any] = {}
    source_l = (source or "").lower()

    next_data = _load_next_data(html_text)
    if next_data:
        try:
            next_candidates = _extract_projects_from_json(next_data, base_url="", source=source or "detail")
            if next_candidates:
                seed = next_candidates[0]
                for key in (
                    "project_name",
                    "builder_name",
                    "property_types",
                    "configurations",
                    "locality",
                    "city",
                    "state",
                    "country",
                    "possession_date",
                    "launch_status",
                    "rera_number",
                    "project_description",
                    "amenities",
                    "images",
                    "latitude",
                    "longitude",
                    "price_min",
                    "price_max",
                    "price_currency",
                ):
                    if key in seed and seed[key] not in (None, "", []):
                        detail[key] = seed[key]

            seed_from_next_data = _extract_detail_from_next_data(next_data)
            for key, value in seed_from_next_data.items():
                if not detail.get(key):
                    detail[key] = value
        except Exception:
            logger.debug("Could not parse detail NEXT_DATA payload", exc_info=True)

    for obj in _iter_jsonld_objects(html_text):
        obj_type = _as_text(obj.get("@type"))
        obj_type_l = (obj_type or "").lower()
        if obj_type_l in {"itemlist", "breadcrumblist", "website"}:
            continue
        if "name" in obj and not detail.get("project_name"):
            detail["project_name"] = _as_text(obj.get("name"))
        if "description" in obj and not detail.get("project_description"):
            detail["project_description"] = _as_text(obj.get("description"))
        if "image" in obj and not detail.get("images"):
            images = _normalize_images(obj.get("image"))
            if images:
                detail["images"] = images

        geo = obj.get("geo") if isinstance(obj.get("geo"), dict) else {}
        if geo:
            detail["latitude"] = _maybe_float(geo.get("latitude"))
            detail["longitude"] = _maybe_float(geo.get("longitude"))

        brand = obj.get("brand")
        if isinstance(brand, dict) and not detail.get("builder_name"):
            detail["builder_name"] = _normalize_builder_name(brand.get("name"))

        offers = obj.get("offers")
        if isinstance(offers, dict):
            if not detail.get("price_min"):
                pmin, pmax, pcur = _parse_price(offers.get("price"), default_currency=SETTINGS.currency)
                if pmin:
                    detail["price_min"] = pmin
                if pmax:
                    detail["price_max"] = pmax
                if pcur:
                    detail["price_currency"] = pcur
        elif isinstance(offers, list):
            price_values = []
            for offer in offers:
                if not isinstance(offer, dict):
                    continue
                pmin, _, pcur = _parse_price(offer.get("price"), default_currency=SETTINGS.currency)
                if pmin:
                    price_values.append(pmin)
                    if pcur and not detail.get("price_currency"):
                        detail["price_currency"] = pcur
            if price_values:
                detail["price_min"] = min(price_values)
                detail["price_max"] = max(price_values)

    title_match = re.search(r"<title[^>]*>(.*?)</title>", html_text, re.I | re.S)
    if title_match and not detail.get("project_name"):
        title = re.sub(r"<[^>]+>", "", title_match.group(1)).strip()
        title = re.sub(r"\s*\|.*$", "", title).strip()
        if title:
            detail["project_name"] = title

    desc_match = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']', html_text, re.I | re.S)
    if not desc_match:
        desc_match = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']description["\']', html_text, re.I | re.S)
    if desc_match and not detail.get("project_description"):
        detail["project_description"] = desc_match.group(1).strip()

    text = _html_to_text(html_text)

    if not detail.get("builder_name"):
        builder_match = re.search(
            r"(?:by|developer|developed by)\s+([A-Z][A-Za-z0-9&\-\.\s]{2,80})",
            text,
            re.I,
        )
        if builder_match:
            detail["builder_name"] = _normalize_builder_name(builder_match.group(1))

    detail_builder_name = _normalize_builder_name(detail.get("builder_name"))
    if detail_builder_name:
        detail["builder_name"] = detail_builder_name
    else:
        detail.pop("builder_name", None)

    if not detail.get("configurations"):
        bhk_matches = re.findall(r"(\d+)\s*BHK", text, re.I)
        if bhk_matches:
            detail["configurations"] = [f"{n}BHK" for n in dict.fromkeys(bhk_matches)]

    if not detail.get("property_types"):
        type_candidates = re.findall(
            r"\b(apartment|apartments|villa|villas|townhouse|duplex|penthouse|commercial|office|retail|shop|showroom|warehouse)\b",
            text,
            re.I,
        )
        if type_candidates:
            detail["property_types"] = _normalize_property_types(type_candidates)

    if not detail.get("price_min"):
        price_match = re.search(
            r"(?:AED|SAR|INR|Rs\.?|Dhs|Dirham)?\s*[0-9][0-9,\.]*\s*(?:Cr|Crore|Lac|Lakh|K|M|Million|B|Billion)?(?:\s*(?:-|to)\s*[0-9][0-9,\.]*\s*(?:Cr|Crore|Lac|Lakh|K|M|Million|B|Billion)?)?",
            text,
            re.I,
        )
        if price_match:
            pmin, pmax, pcur = _parse_price(price_match.group(0), default_currency=SETTINGS.currency)
            if pmin:
                detail["price_min"] = pmin
            if pmax:
                detail["price_max"] = pmax
            if pcur:
                detail["price_currency"] = pcur

    if not detail.get("super_area_min_sqft"):
        area_match = re.search(r"([0-9,.]+)\s*(?:sq\.?\s*ft|sqft|sq\.?\s*m|sqm)", text, re.I)
        if area_match:
            detail["super_area_min_sqft"] = _parse_area(area_match.group(0))

    if not detail.get("locality"):
        locality_match = re.search(
            r"(?:located in|location|at)\s+([A-Za-z][A-Za-z\s\-]{2,80})(?:,\s*(?:Dubai|Abu Dhabi|Sharjah|Ajman|Riyadh|Jeddah|Kochi|Kozhikode|Thiruvananthapuram))",
            text,
            re.I,
        )
        if locality_match:
            detail["locality"] = locality_match.group(1).strip()

    if not detail.get("rera_number"):
        rera_match = re.search(r"(?:RERA|PRJ)\s*(?:No\.?|Number|#)?\s*[:\-]?\s*([A-Za-z0-9\-/]{4,80})", text, re.I)
        if rera_match:
            detail["rera_number"] = rera_match.group(1).strip()

    pos_meta = _extract_possession_metadata_from_text(text)
    if pos_meta.get("possession_date") and not detail.get("possession_date"):
        detail["possession_date"] = pos_meta["possession_date"]
    if pos_meta.get("launch_status") and not detail.get("launch_status"):
        detail["launch_status"] = pos_meta["launch_status"]

    if not detail.get("payment_plan"):
        payment_match = re.search(r"\b(\d{1,2}\s*/\s*\d{1,2}\s*/\s*\d{1,2})\b", text)
        if payment_match:
            detail["payment_plan"] = payment_match.group(1).replace(" ", "")

    if not detail.get("completion_percentage"):
        completion_match = re.search(r"(\d{1,3})\s*%\s*(?:complete|completed|completion)", text, re.I)
        if completion_match:
            detail["completion_percentage"] = int(completion_match.group(1))

    if "propertyfinder" in source_l and not detail.get("project_highlights"):
        highlights = re.findall(r"\b(?:payment plan|handover|delivery|starting from|developer)\b", text, re.I)
        if highlights:
            detail["project_highlights"] = list(dict.fromkeys([h.title() for h in highlights]))

    return {k: v for k, v in detail.items() if v not in (None, "", [])}


def _strip_bom_and_fences(text: str) -> str:
    out = text.lstrip("\ufeff").strip()
    if out.startswith("```"):
        out = re.sub(r"^```[a-zA-Z0-9_-]*\s*\n?", "", out, count=1)
        if out.endswith("```"):
            out = out[:-3].rstrip()
    out = re.sub(r"^\)\]\}',?\s*\n", "", out)
    return out


def _find_first_json_block(text: str) -> str | None:
    text = text.strip()
    start_idx = None
    depth = 0
    in_str = False
    escaped = False
    quote = None

    for idx, ch in enumerate(text):
        if start_idx is None:
            if ch in "[{":
                start_idx = idx
                depth = 1
                continue
        else:
            if in_str:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == quote:
                    in_str = False
            else:
                if ch in ('"', "'"):
                    in_str = True
                    quote = ch
                elif ch in "[{":
                    depth += 1
                elif ch in "]}":
                    depth -= 1
                    if depth == 0:
                        return text[start_idx: idx + 1]
    return None


def parse_llm_json(text: str) -> Any:
    cleaned = _strip_bom_and_fences(text)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    if json_repair is not None:
        try:
            return json_repair.loads(cleaned)
        except Exception:
            pass

    if dirtyjson is not None:
        try:
            return dirtyjson.loads(cleaned, search_for_first_object=True)
        except Exception:
            pass

    block = _find_first_json_block(cleaned)
    if block:
        return parse_llm_json(block)

    fallback = re.sub(r"\bNone\b", "null", cleaned)
    fallback = re.sub(r"\bTrue\b", "true", fallback)
    fallback = re.sub(r"\bFalse\b", "false", fallback)
    fallback = "".join(ch for ch in fallback if (ord(ch) >= 32 or ch in "\n\r\t"))
    return json.loads(fallback)


def ai_extract_project(text: str) -> dict[str, Any]:
    system_prompt = (
        "You are an extraction engine. Return one compact JSON object only. "
        "No markdown, no code fences, no explanatory text."
    )
    user_prompt = f"{PROJECT_EXTRACT_PROMPT}\n\nText to extract from:\n{text[:8000]}"
    try:
        result_text = call_llm(system_prompt=system_prompt, user_prompt=user_prompt, json_mode=True)
        parsed = parse_llm_json(result_text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception as exc:
        logger.warning("AI extraction failed: %s", exc)
        return {}


def _normalize_city_component(value: Any) -> str:
    text = (_as_text(value) or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text


def _project_id_for_city(project_name: str, builder_name: str, city_key: str | None, country: str | None) -> str:
    name_norm = re.sub(r"\s+", " ", project_name.strip().lower())
    builder_norm = re.sub(r"\s+", " ", builder_name.strip().lower())
    city_component = _normalize_city_component(city_key)
    country_component = _normalize_city_component(country)
    combined = "|".join(filter(None, [name_norm, builder_norm, city_component, country_component]))
    return hashlib.sha1(combined.encode("utf-8")).hexdigest()


def _to_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_project_record(raw: dict[str, Any], discovered_at: str | None = None) -> dict[str, Any]:
    project_name = _as_text(raw.get("project_name"))
    if _is_generic_project_name(project_name):
        project_name = _name_from_project_url(_as_text(raw.get("project_url")) or "") or project_name
    if not project_name:
        return {}
    if not _looks_like_project_entity_name(project_name):
        return {}

    builder_name = _normalize_builder_name(raw.get("builder_name") or raw.get("developer_name")) or "Unknown"
    developer_name = _normalize_builder_name(raw.get("developer_name")) or (builder_name if builder_name != "Unknown" else None)

    raw_city = _as_text(raw.get("city") or raw.get("target_city") or SETTINGS.city)
    raw_state = _as_text(raw.get("state") or SETTINGS.state)
    raw_country = _as_text(raw.get("country") or SETTINGS.country)
    raw_region = _as_text(raw.get("region") or SETTINGS.region)

    city_key = (
        _normalize_city_component(raw.get("target_city_key"))
        or _normalize_city_component(raw.get("_target_city"))
        or _normalize_city_component(SETTINGS.city_key)
        or _normalize_city_component(raw_city)
        or "unknown"
    )
    target_city = _as_text(raw.get("target_city")) or SETTINGS.city or raw_city or city_key.replace("-", " ").title()

    property_types = _normalize_property_types(raw.get("property_types") or raw.get("property_type"))
    property_category = _as_text(raw.get("property_category")) or _derive_property_category(property_types)
    launch_status = _normalize_launch_status(raw.get("launch_status") or raw.get("project_status"))
    project_status = _as_text(raw.get("project_status") or launch_status)

    pmin = raw.get("price_min")
    pmax = raw.get("price_max")
    pcur = _as_text(raw.get("price_currency")) or SETTINGS.currency
    if (pmin is None and pmax is None) and raw.get("price") is not None:
        pmin, pmax, parsed_cur = _parse_price(raw.get("price"), default_currency=SETTINGS.currency)
        if parsed_cur:
            pcur = parsed_cur

    possession_date = _normalize_possession_date(raw.get("possession_date") or raw.get("completion_date"))
    completion_date = _normalize_possession_date(raw.get("completion_date") or raw.get("possession_date"))
    completion_percentage = _maybe_float(raw.get("completion_percentage"))

    record: dict[str, Any] = {key: None for key in STANDARD_SCHEMA_FIELDS}
    for key in STANDARD_SCHEMA_FIELDS:
        if key in raw:
            record[key] = raw[key]

    record.update(
        {
            "project_name": project_name,
            "builder_name": builder_name,
            "developer_name": developer_name,
            "property_types": property_types,
            "property_category": property_category,
            "launch_status": launch_status or "under-construction",
            "project_status": project_status,
            "configurations": _normalize_configurations(raw.get("configurations")),
            "amenities": _normalize_amenities(raw.get("amenities")),
            "city": raw_city or SETTINGS.city,
            "state": raw_state or SETTINGS.state,
            "district": _as_text(raw.get("district")) or SETTINGS.district or None,
            "country": raw_country or SETTINGS.country,
            "region": raw_region or SETTINGS.region,
            "target_city_key": city_key,
            "target_city": target_city,
            "possession_date": possession_date,
            "completion_date": completion_date,
            "completion_percentage": completion_percentage,
            "price_currency": pcur,
            "price_min": int(pmin) if pmin is not None else None,
            "price_max": int(pmax) if pmax is not None else None,
            "price_per_sqft": _maybe_int(raw.get("price_per_sqft")),
            "latitude": _maybe_float(raw.get("latitude")),
            "longitude": _maybe_float(raw.get("longitude")),
            "rera_number": _as_text(raw.get("rera_number")),
            "rera_status": _as_text(raw.get("rera_status")),
            "project_description": _as_text(raw.get("project_description")),
            "project_highlights": raw.get("project_highlights") or [],
            "payment_plan": _as_text(raw.get("payment_plan")),
            "images": _normalize_images(raw.get("images")),
            "brochure_url": _as_text(raw.get("brochure_url")),
            "project_url": _as_text(raw.get("project_url")),
            "builder_url": _as_text(raw.get("builder_url")),
            "source_url": _as_text(raw.get("source_url") or raw.get("project_url")),
            "source": _as_text(raw.get("source")) or "unknown",
        }
    )

    data_quality = _as_text(raw.get("data_quality"))
    if not data_quality:
        if record.get("project_description") or record.get("images") or record.get("rera_number"):
            data_quality = "detail"
        elif record.get("configurations") or record.get("price_min") or record.get("locality"):
            data_quality = "listing"
        else:
            data_quality = "lead"
    record["data_quality"] = data_quality

    now_iso = discovered_at or _to_iso_now()
    record["discovered_at"] = _as_text(raw.get("discovered_at")) or now_iso
    record["updated_at"] = _to_iso_now()
    record["id"] = _project_id_for_city(project_name, builder_name, city_key, record.get("country"))

    list_keys = {"property_types", "configurations", "amenities", "project_highlights"}
    mandatory_keys = set(MANDATORY_FIELDS)
    cleaned: dict[str, Any] = {}
    for key, value in record.items():
        if key in list_keys:
            cleaned[key] = value or []
        elif key in mandatory_keys:
            cleaned[key] = value
        elif value not in (None, "", []):
            cleaned[key] = value

    return cleaned


def deduplicate_projects(projects: list[dict[str, Any]]) -> list[dict[str, Any]]:
    quality_rank = {"lead": 1, "listing": 2, "detail": 3}
    by_id: dict[str, dict[str, Any]] = {}

    for project in projects:
        pid = project.get("id")
        if not pid:
            continue
        existing = by_id.get(pid)
        if existing is None:
            by_id[pid] = project
            continue

        merged = existing.copy()
        for key, value in project.items():
            if value in (None, "", []):
                continue
            if key == "source":
                merged_sources = set()
                for src in (existing.get("source"), project.get("source")):
                    if src:
                        merged_sources.add(src)
                merged["source"] = ",".join(sorted(merged_sources))
            elif key == "data_quality":
                old_q = quality_rank.get(str(existing.get("data_quality", "lead")).lower(), 1)
                new_q = quality_rank.get(str(value).lower(), 1)
                merged["data_quality"] = value if new_q >= old_q else existing.get("data_quality", value)
            elif key in ("price_min", "price_max"):
                old_v = existing.get(key)
                if old_v is None:
                    merged[key] = value
                elif key == "price_min":
                    merged[key] = min(old_v, value)
                else:
                    merged[key] = max(old_v, value)
            elif key in ("amenities", "property_types", "configurations", "project_highlights"):
                merged[key] = list(dict.fromkeys((existing.get(key) or []) + (value or [])))
            elif merged.get(key) in (None, "", []):
                merged[key] = value
        by_id[pid] = merged

    return list(by_id.values())


ES_INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "5s",
    },
    "mappings": {
        "dynamic": True,
        "dynamic_templates": [
            {"dates_iso": {"match": "*_at", "mapping": {"type": "date", "format": "strict_date_optional_time||epoch_millis"}}},
            {"dates": {"match": "*_date", "mapping": {"type": "keyword"}}},
            {"strings": {"match_mapping_type": "string", "mapping": {"type": "keyword", "ignore_above": 512}}},
            {"doubleNums": {"match_mapping_type": "double", "mapping": {"type": "double"}}},
            {"longNums": {"match_mapping_type": "long", "mapping": {"type": "long"}}},
        ],
        "properties": {
            "id": {"type": "keyword"},
            "project_name": {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}},
            "builder_name": {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}},
            "developer_name": {"type": "keyword"},
            "property_types": {"type": "keyword"},
            "property_category": {"type": "keyword"},
            "launch_status": {"type": "keyword"},
            "project_status": {"type": "keyword"},
            "target_city_key": {"type": "keyword"},
            "target_city": {"type": "keyword"},
            "city": {"type": "keyword"},
            "state": {"type": "keyword"},
            "country": {"type": "keyword"},
            "region": {"type": "keyword"},
            "locality": {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}},
            "price_min": {"type": "long"},
            "price_max": {"type": "long"},
            "price_currency": {"type": "keyword"},
            "price_per_sqft": {"type": "long"},
            "completion_percentage": {"type": "double"},
            "rera_number": {"type": "keyword"},
            "payment_plan": {"type": "keyword"},
            "source": {"type": "keyword"},
            "data_quality": {"type": "keyword"},
            "project_url": {"type": "keyword"},
            "source_url": {"type": "keyword"},
            "project_description": {"type": "text"},
            "amenities": {"type": "keyword"},
            "project_highlights": {"type": "keyword"},
        },
    },
}


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


def index_projects_to_es(projects: list[dict[str, Any]], es: Elasticsearch | None = None, index: str | None = None) -> int:
    target_index = index or SETTINGS.es_index
    if not projects:
        logger.info("No projects to index into Elasticsearch (%s)", target_index)
        return 0

    if es is None:
        es = es_client()
    ensure_index(es, target_index)

    df = pd.DataFrame(projects)
    df = df.drop_duplicates(subset=["id"], keep="last").reset_index(drop=True)
    actions = list(df_to_actions(df, target_index))
    if not actions:
        logger.info("No serializable project records for Elasticsearch")
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
    logger.info("Indexed %d project records into ES index %s", indexed, target_index)
    return indexed


class InputParams(BaseModel):
    city: str = DEFAULT_CITY


@task
async def discover_projects(params: InputParams = None) -> list[dict[str, Any]]:
    selected_city = params.city if params else DEFAULT_CITY
    _activate_settings(selected_city)

    session, use_curl = _build_http_client()
    discovered: list[dict[str, Any]] = []
    listing_urls_to_visit: list[str] = []
    seen_project_keys: set[str] = set()
    source_added_counts: dict[str, int] = {key: 0 for key in SOURCE_KEYS}
    now = _to_iso_now()

    def add_project(
        project: dict[str, Any],
        *,
        source_hint: str | None = None,
        source_bucket: str | None = None,
    ) -> bool:
        if not isinstance(project, dict):
            return False
        rec = project.copy()
        rec.setdefault("city", SETTINGS.city)
        rec.setdefault("state", SETTINGS.state)
        rec.setdefault("district", SETTINGS.district)
        rec.setdefault("country", SETTINGS.country)
        rec.setdefault("region", SETTINGS.region)
        rec.setdefault("price_currency", SETTINGS.currency)
        rec.setdefault("source", source_hint or "unknown")
        rec.setdefault("data_quality", "listing")
        rec["discovered_at"] = now
        rec["_target_city"] = SETTINGS.city_key

        key = rec.get("project_url") or rec.get("source_url") or rec.get("project_name")
        if not key:
            return False
        if key in seen_project_keys:
            return False
        seen_project_keys.add(str(key))
        discovered.append(rec)
        if source_bucket:
            source_added_counts[source_bucket] = source_added_counts.get(source_bucket, 0) + 1
        return True

    def add_listing_url(url: str):
        if not url or url in listing_urls_to_visit:
            return
        listing_urls_to_visit.append(url)

    source_jobs = [
        ("realestateindia", SETTINGS.realestateindia_url, parse_realestateindia_listing),
        ("commonfloor", SETTINGS.commonfloor_url, parse_commonfloor_listing),
        ("housing", SETTINGS.housing_url, parse_housing_listing),
        ("propertyfinder", SETTINGS.propertyfinder_url, lambda html_text: parse_propertyfinder_listing(html_text, SETTINGS.country)),
        ("aqar", SETTINGS.aqar_url, parse_aqar_listing),
        ("bayut", SETTINGS.bayut_url, lambda html_text: parse_bayut_listing(html_text, SETTINGS.country)),
        ("offplan_dubai", SETTINGS.offplan_dubai_url, parse_offplan_dubai_listing),
    ]

    if SETTINGS.extra_source_urls:
        for extra_url in SETTINGS.extra_source_urls:
            parser = _select_listing_parser(extra_url, source_override="extra_sources")
            source_jobs.append(("extra_sources", extra_url, parser))

    for source_name, source_url, parser in source_jobs:
        if not source_url:
            continue
        tuning = _source_tuning_for(source_name)
        if not tuning.enabled:
            logger.info("Skipping source %s due source tuning disabled", source_name)
            continue

        pages = tuning.pages
        for page in range(1, pages + 1):
            if source_added_counts.get(source_name, 0) >= tuning.max_projects:
                logger.info(
                    "Stopping source %s at cap %d projects",
                    source_name,
                    tuning.max_projects,
                )
                break

            url = _build_source_url(source_url, page)
            logger.info("Scraping %s page %d: %s", source_name, page, url)
            try:
                html_text = _fetch(session, url, retries=tuning.retries, use_curl_cffi=use_curl)
                projects = parser(html_text)
                added_from_page = 0
                for project in projects:
                    if source_added_counts.get(source_name, 0) >= tuning.max_projects:
                        break
                    if add_project(project, source_hint=source_name, source_bucket=source_name):
                        added_from_page += 1
                logger.info(
                    "Found %d projects from %s page %d (%d added, source_total=%d/%d)",
                    len(projects),
                    source_name,
                    page,
                    added_from_page,
                    source_added_counts.get(source_name, 0),
                    tuning.max_projects,
                )
            except Exception as exc:
                logger.warning("Failed %s page %d: %s", source_name, page, exc)
                exc_text = str(exc).lower()
                if "404" in exc_text or "not found" in exc_text:
                    logger.info(
                        "Stopping pagination for %s after page %d due not-found response",
                        source_name,
                        page,
                    )
                    break
            await asyncio.sleep(random.uniform(SETTINGS.min_delay, SETTINGS.max_delay))

    if SETTINGS.dld_project_csv_url:
        logger.info("Fetching DLD project CSV feed: %s", SETTINGS.dld_project_csv_url)
        try:
            csv_text = _fetch(session, SETTINGS.dld_project_csv_url, retries=1, use_curl_cffi=use_curl)
            dld_projects = parse_dld_projects_csv(csv_text)
            for row in dld_projects:
                add_project(row)
            logger.info("Added %d projects from DLD CSV source", len(dld_projects))
        except Exception as exc:
            logger.warning("Failed to process DLD CSV source: %s", exc)

    for query in SETTINGS.duckduckgo_queries:
        for page in range(SETTINGS.duckduckgo_pages):
            offset = page * 10
            ddg_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}&s={offset}"
            logger.info("DuckDuckGo discovery query='%s' page=%d", query, page + 1)
            try:
                ddg_html = _fetch(session, ddg_url, retries=2, use_curl_cffi=use_curl)
                ddg_results = parse_duckduckgo_serp(ddg_html)
                for row in ddg_results:
                    add_listing_url(row["url"])
                logger.info("Discovered %d listing URLs via DuckDuckGo", len(ddg_results))
            except Exception as exc:
                logger.warning("DuckDuckGo query failed (%s): %s", query, exc)
            await asyncio.sleep(random.uniform(SETTINGS.min_delay, SETTINGS.max_delay))

    max_listing_visits = min(120, max(20, len(listing_urls_to_visit)))
    skipped_disallowed_listing_urls = 0
    for listing_url in listing_urls_to_visit[:max_listing_visits]:
        if not _is_duckduckgo_listing_url_allowed(listing_url):
            skipped_disallowed_listing_urls += 1
            logger.info("Skipping discovered listing URL due host allowlist: %s", listing_url)
            continue
        logger.info("Visiting discovered listing page: %s", listing_url)
        try:
            listing_html = _fetch(session, listing_url, retries=1, use_curl_cffi=use_curl)
            parser = _select_listing_parser(listing_url)
            projects = parser(listing_html)
            for project in projects:
                add_project(project)
        except Exception as exc:
            logger.warning("Failed visiting listing page %s: %s", listing_url, exc)
        await asyncio.sleep(random.uniform(SETTINGS.min_delay, SETTINGS.max_delay))

    if skipped_disallowed_listing_urls:
        logger.info(
            "Skipped %d discovered listing URLs due duckduckgo_allowed_hosts for city %s",
            skipped_disallowed_listing_urls,
            SETTINGS.city_key,
        )

    logger.info("Total discovered projects: %d", len(discovered))
    out_dir = _saved_data_dir()
    pd.DataFrame(discovered).to_json(out_dir / "discovered_raw.json", orient="records", force_ascii=False, indent=2)
    return discovered


@task
async def enrich_project_details(discovered: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not discovered:
        logger.info("No discovered projects to enrich")
        return []

    _activate_settings_from_records(discovered)
    session, use_curl = _build_http_client()
    enriched: list[dict[str, Any]] = []

    for idx, project in enumerate(discovered):
        project_name = project.get("project_name") or "(unnamed)"
        detail_url = project.get("project_url") or project.get("source_url")
        source = project.get("source", "unknown")
        logger.info("Enriching %d/%d: %s", idx + 1, len(discovered), project_name)

        detail_fetched = False
        if detail_url:
            for attempt in range(SETTINGS.detail_retry):
                try:
                    html_text = _fetch(session, detail_url, retries=1, use_curl_cffi=use_curl)
                    detail_fetched = True
                    parsed_detail = parse_project_detail_page(html_text, source=source)
                    for key, value in parsed_detail.items():
                        if value is not None and (project.get(key) in (None, "", [])):
                            project[key] = value

                    needs_ai = not project.get("builder_name") or not project.get("property_types") or not project.get("configurations")
                    if needs_ai:
                        ai_result = ai_extract_project(_html_to_text(html_text))
                        for key, value in ai_result.items():
                            if value is not None and (project.get(key) in (None, "", [])):
                                project[key] = value
                    break
                except Exception as exc:
                    logger.warning(
                        "Detail fetch failed for %s (attempt %d/%d): %s",
                        detail_url,
                        attempt + 1,
                        SETTINGS.detail_retry,
                        exc,
                    )
                    await asyncio.sleep(random.uniform(SETTINGS.min_delay, SETTINGS.max_delay))

        if not detail_fetched and not project.get("project_name"):
            continue

        normalized = normalize_project_record(project, discovered_at=project.get("discovered_at"))
        if normalized:
            enriched.append(normalized)
        await asyncio.sleep(random.uniform(SETTINGS.min_delay, SETTINGS.max_delay))

    logger.info("Enriched %d projects from %d discovered", len(enriched), len(discovered))
    out_dir = _saved_data_dir()
    pd.DataFrame(enriched).to_json(out_dir / "enriched_projects.json", orient="records", force_ascii=False, indent=2)
    return enriched


@task
async def standardize_and_index(enriched: list[dict[str, Any]]) -> int:
    if not enriched:
        logger.info("No enriched projects to index")
        return 0

    _activate_settings_from_records(enriched)

    normalized = [normalize_project_record(row, discovered_at=row.get("discovered_at")) for row in enriched]
    normalized = [row for row in normalized if row]
    deduped = deduplicate_projects(normalized)

    logger.info("After dedupe: %d unique projects (from %d)", len(deduped), len(enriched))
    indexed = index_projects_to_es(deduped)

    out_dir = _saved_data_dir()
    pd.DataFrame(deduped).to_json(out_dir / "standardized_projects.json", orient="records", force_ascii=False, indent=2)
    return indexed


def _build_triggers() -> list[Trigger]:
    triggers: list[Trigger] = []
    for city_key, _ in sorted(_city_sections.items()):
        city_settings = _load_settings_for_city(city_key)
        triggers.append(
            Trigger(
                id=f"property_projects_daily_{city_key}",
                name=f"Property Projects Daily ({city_settings.city})",
                description=f"Run property projects pipeline daily for {city_settings.city}, {city_settings.country}",
                params=InputParams(city=city_key),
                schedule=CronTrigger(
                    hour=city_settings.schedule_hour,
                    minute=city_settings.schedule_minute,
                    timezone=city_settings.schedule_timezone,
                ),
            )
        )
    return triggers


if _city_sections:
    register_pipeline(
        id="property_projects_pipeline",
        description="Discover, enrich and index apartments, villas and commercial projects for configured Kerala/UAE/KSA cities.",
        tasks=[discover_projects, enrich_project_details, standardize_and_index],
        triggers=_build_triggers(),
        params=InputParams,
    )
else:  # pragma: no cover - startup guard
    logger.warning("No [property_projects.<city>] sections found in config.ini; property projects pipeline not registered")
