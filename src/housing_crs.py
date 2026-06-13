"""Housing.com scraper and pipeline.

Scrapes Housing.com listing pages for buy, rent, commercial, PG/co-living,
and plots inventory across configured cities. It enriches each listing with
detail-page data, indexes records into Elasticsearch, and exports a JSON
snapshot to Cloudflare R2.
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import json
import logging
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import unquote, urljoin, urlparse

import boto3
import numpy as np
import pandas as pd
import requests

try:  # pragma: no cover - optional dependency
    from curl_cffi import requests as curl_requests
except Exception:  # pragma: no cover - gracefully degrade
    curl_requests = None

try:  # pragma: no cover - optional dependency
    import json_repair
except Exception:  # pragma: no cover - gracefully degrade
    json_repair = None

from apscheduler.triggers.cron import CronTrigger
from elasticsearch import Elasticsearch, helpers
from pydantic import BaseModel

from plombery import Trigger, register_pipeline, task
from config import read_config


logger = logging.getLogger(__name__)


CONFIG_SECTION = "housing"
SOURCE_NAME = "housing.com"
HOUSING_BASE_URL = "https://housing.com"

DEFAULT_CITY_SLUGS = (
    "kochi",
    "thiruvananthapuram",
    "kozhikode",
    "mumbai",
    "bengaluru",
    "new-delhi",
    "hyderabad",
    "chennai",
    "kolkata",
    "pune",
)

DEFAULT_CITY_LABELS = {
    "kochi": "Kochi",
    "thiruvananthapuram": "Thiruvananthapuram",
    "kozhikode": "Kozhikode",
    "mumbai": "Mumbai",
    "bengaluru": "Bengaluru",
    "new-delhi": "New Delhi",
    "hyderabad": "Hyderabad",
    "chennai": "Chennai",
    "kolkata": "Kolkata",
    "pune": "Pune",
}

DEFAULT_URL_TEMPLATES = {
    "buy": "https://housing.com/in/buy/{city}?page={page}",
    "rent": "https://housing.com/in/rent/{city}?page={page}",
    "commercial": "https://housing.com/in/commercial/{city}?page={page}",
    "pg": "https://housing.com/in/pg/{city}?page={page}",
    "plots": "https://housing.com/in/plots/{city}?page={page}",
}

DEFAULT_INDEXES = {
    "buy": "housing_properties",
    "rent": "housing_rent_properties",
    "commercial": "housing_commercial_properties",
    "pg": "housing_pg_properties",
    "plots": "housing_plots_properties",
}

REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
}

MODERN_USER_AGENTS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.6478.126 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.6422.78 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.6367.207 Safari/537.36",
)

IMPERSONATE_IDS = ("chrome110", "chrome120", "chrome124", "chrome131")

HARD_BLOCK_MARKERS = (
    "verify you are a human",
    "access denied",
    "unusual traffic",
    "are you a robot",
    "security challenge",
    "temporarily blocked",
    "cloudflare ray id",
)

SOFT_BLOCK_MARKERS = (
    "captcha",
    "blocked",
)

SOFT_BLOCK_ALLOW_MARKERS = (
    "__next_data__",
    "application/ld+json",
    "housing",
    "buy",
    "rent",
    "commercial",
    "projects",
)

RETRYABLE_HTTP_STATUS = {403, 408, 409, 425, 429, 500, 502, 503, 504}

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

ID_KEYS = (
    "id",
    "projectId",
    "project_id",
    "propertyId",
    "property_id",
    "listingId",
    "prjid",
    "uuid",
)

URL_KEYS = (
    "url",
    "projectUrl",
    "projectURL",
    "propertyUrl",
    "detailUrl",
    "detail_url",
    "href",
    "seoUrl",
    "permalink",
    "link",
)

TITLE_KEYS = (
    "projectName",
    "project_name",
    "projectTitle",
    "propertyName",
    "title",
    "name",
    "heading",
    "displayName",
    "societyName",
)

BUILDER_KEYS = (
    "builderName",
    "builder_name",
    "builder",
    "developerName",
    "developer_name",
    "developer",
)

PRICE_KEYS = (
    "price",
    "priceRange",
    "price_range",
    "startingPrice",
    "minPrice",
    "maxPrice",
    "amount",
    "priceText",
)

PRICE_PER_SQFT_KEYS = (
    "pricePerSqft",
    "price_per_sqft",
    "pricePerUnitArea",
    "ratePerSqft",
)

AREA_KEYS = (
    "area",
    "areaSqft",
    "area_sqft",
    "builtUpArea",
    "superBuiltupArea",
    "carpetArea",
    "plotArea",
    "size",
)

BHK_KEYS = (
    "bhk",
    "bedrooms",
    "bedroom",
    "bedroomCount",
    "numBedrooms",
)

BATH_KEYS = (
    "bathrooms",
    "bathroom",
    "bathroomCount",
    "numBathrooms",
)

TYPE_KEYS = (
    "propertyType",
    "property_type",
    "propertyTypes",
    "category",
    "type",
)

STATUS_KEYS = (
    "listingStatus",
    "status",
    "constructionStatus",
    "launchStatus",
)

LOCALITY_KEYS = (
    "locality",
    "localityName",
    "area",
    "areaName",
    "address",
)

CITY_KEYS = (
    "city",
    "cityName",
    "city_name",
)

LAT_KEYS = ("lat", "latitude")
LNG_KEYS = ("lng", "lon", "longitude")
RERA_KEYS = ("rera", "reraId", "reraNumber", "reraNo")
POSSESSION_KEYS = ("possessionDate", "possession", "handoverDate", "completionDate")
CONFIG_KEYS = ("configuration", "configurations", "bhk")
AMENITY_KEYS = ("amenities", "features", "projectAmenities")
IMAGE_KEYS = ("images", "image", "gallery", "photos")

MANDATORY_FIELDS = ("id", "title", "detail_url", "city", "source")

GENERIC_TITLE_TOKENS = (
    "projects in",
    "properties in",
    "property for",
    "new projects",
    "view details",
    "read more",
    "search results",
    "all properties",
)

MONTH_MAP = {
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

QUARTER_MAP = {"q1": "03", "q2": "06", "q3": "09", "q4": "12"}


@dataclass(slots=True)
class HousingBaseSettings:
    enabled: bool
    min_delay_seconds: float
    max_delay_seconds: float
    detail_retry_count: int
    request_timeout: int
    stop_on_existing: bool
    data_dir: Path
    schedule_hour: str
    schedule_minute: str
    schedule_timezone: str


@dataclass(slots=True)
class HousingJobSettings:
    name: str
    category: str
    city_slug: str
    city_label: str
    listing_url_template: str
    pages: int
    es_index: str


@dataclass(slots=True)
class HttpClient:
    session: Any
    use_curl_cffi: bool


ES_INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "5s",
    },
    "mappings": {
        "dynamic": True,
        "dynamic_templates": [
            {"dates_iso": {"match": "*_iso", "mapping": {"type": "date", "format": "strict_date_time"}}},
            {"epochs": {"match": "*_epoch", "mapping": {"type": "long"}}},
            {"strings": {"match_mapping_type": "string", "mapping": {"type": "keyword", "ignore_above": 256}}},
            {"doubleNums": {"match_mapping_type": "double", "mapping": {"type": "double"}}},
            {"longNums": {"match_mapping_type": "long", "mapping": {"type": "long"}}},
        ],
        "properties": {
            "id": {"type": "keyword"},
            "detail_url": {"type": "keyword"},
            "title": {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}},
            "builder_name": {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}},
            "city": {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}},
            "locality": {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}},
            "listing_category": {"type": "keyword"},
            "source": {"type": "keyword"},
            "source_page": {"type": "integer"},
        },
    },
}


def _section_get(section: Any, key: str, default: str = "") -> str:
    if section is None:
        return default
    if hasattr(section, "get"):
        try:
            value = section.get(key, fallback=default, raw=True)
        except TypeError:
            value = section.get(key, default)
    elif isinstance(section, Mapping):
        value = section.get(key, default)
    else:
        value = default
    if value is None:
        return default
    return str(value)


def _parse_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _parse_int(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def _parse_float(value: Any, default: float) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _city_slug(value: str | None) -> str:
    text = (value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text


def _city_label(slug: str) -> str:
    if slug in DEFAULT_CITY_LABELS:
        return DEFAULT_CITY_LABELS[slug]
    return slug.replace("-", " ").title()


def _parse_city_labels(raw: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for chunk in str(raw or "").split(","):
        part = chunk.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        key = _city_slug(k)
        val = v.strip()
        if key and val:
            out[key] = val
    return out


def _parse_csv(raw: str) -> list[str]:
    values: list[str] = []
    for token in str(raw or "").split(","):
        norm = _city_slug(token)
        if norm and norm not in values:
            values.append(norm)
    return values


def _build_listing_page_url(job: HousingJobSettings, page: int) -> str:
    url = job.listing_url_template.strip()
    url = url.replace("{city}", job.city_slug)
    if "{page}" in url:
        return url.replace("{page}", str(page))
    if page <= 1:
        return url
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}page={page}"


def _build_job_settings(section: Any, city_slugs: list[str], city_labels: dict[str, str], default_pages: int) -> list[HousingJobSettings]:
    category_specs = (
        ("buy", "buy_listing_url", "pages", "es_index"),
        ("rent", "rent_listing_url", "rent_pages", "rent_es_index"),
        ("commercial", "commercial_listing_url", "commercial_pages", "commercial_es_index"),
        ("pg", "pg_listing_url", "pg_pages", "pg_es_index"),
        ("plots", "plots_listing_url", "plots_pages", "plots_es_index"),
    )

    jobs: list[HousingJobSettings] = []
    for category, url_key, pages_key, index_key in category_specs:
        fallback_url = DEFAULT_URL_TEMPLATES[category]
        if category == "buy":
            fallback_url = _section_get(section, "listing_url", fallback_url)

        url_template = (_section_get(section, url_key, fallback_url) or "").strip()
        if not url_template:
            continue

        pages = max(1, _parse_int(_section_get(section, pages_key, str(default_pages)), default_pages))
        es_index = (_section_get(section, index_key, DEFAULT_INDEXES[category]) or DEFAULT_INDEXES[category]).strip()

        for slug in city_slugs:
            label = city_labels.get(slug) or _city_label(slug)
            jobs.append(
                HousingJobSettings(
                    name=f"{category}_{slug.replace('-', '_')}",
                    category=category,
                    city_slug=slug,
                    city_label=label,
                    listing_url_template=url_template,
                    pages=pages,
                    es_index=es_index,
                )
            )
    return jobs


config = read_config()

has_housing_section = config.has_section(CONFIG_SECTION)
housing_section = config[CONFIG_SECTION] if has_housing_section else {}

enabled_default = has_housing_section
housing_enabled = _parse_bool(_section_get(housing_section, "enabled", str(enabled_default).lower()), default=enabled_default)

cities = _parse_csv(_section_get(housing_section, "cities", ",".join(DEFAULT_CITY_SLUGS)))
if not cities:
    cities = list(DEFAULT_CITY_SLUGS)

city_labels_override = _parse_city_labels(_section_get(housing_section, "city_labels", ""))
CITY_LABELS = {slug: _city_label(slug) for slug in cities}
CITY_LABELS.update(city_labels_override)

base_data_dir = Path((_section_get(housing_section, "data_dir", "saved_data/housing") or "saved_data/housing").strip())

BASE_SETTINGS = HousingBaseSettings(
    enabled=housing_enabled,
    min_delay_seconds=_parse_float(_section_get(housing_section, "min_delay_seconds", "2.0"), 2.0),
    max_delay_seconds=_parse_float(_section_get(housing_section, "max_delay_seconds", "5.0"), 5.0),
    detail_retry_count=max(1, _parse_int(_section_get(housing_section, "detail_retry_count", "3"), 3)),
    request_timeout=max(5, _parse_int(_section_get(housing_section, "request_timeout_seconds", "30"), 30)),
    stop_on_existing=_parse_bool(_section_get(housing_section, "stop_on_existing", "true"), default=True),
    data_dir=base_data_dir,
    schedule_hour=(_section_get(housing_section, "schedule_hour", "4") or "4").strip(),
    schedule_minute=(_section_get(housing_section, "schedule_minute", "15") or "15").strip(),
    schedule_timezone=(_section_get(housing_section, "schedule_timezone", "Asia/Kolkata") or "Asia/Kolkata").strip(),
)

DEFAULT_PAGES = max(1, _parse_int(_section_get(housing_section, "pages", "2"), 2))
JOB_SETTINGS = _build_job_settings(housing_section, cities, CITY_LABELS, DEFAULT_PAGES)

if not JOB_SETTINGS:
    logger.warning("No Housing.com jobs configured. Scraper task will no-op until URLs are configured.")


_es_config = config["elasticsearch"] if config.has_section("elasticsearch") else {}


def _parse_es_hosts(raw_hosts: str) -> list[str]:
    hosts: list[str] = []
    for host in str(raw_hosts or "localhost:9200").split(","):
        value = host.strip()
        if not value:
            continue
        if not value.startswith("http"):
            value = f"http://{value}"
        if ":" not in value.split("//")[-1]:
            value = f"{value}:9200"
        hosts.append(value)
    return hosts or ["http://localhost:9200"]


es_hosts = _parse_es_hosts(_section_get(_es_config, "host", "localhost:9200"))
es_user = _section_get(_es_config, "username", "elastic")
es_password = _section_get(_es_config, "password", "changeme")


def _build_http_client() -> HttpClient:
    if curl_requests is not None:
        try:
            return HttpClient(session=curl_requests.Session(), use_curl_cffi=True)
        except Exception:  # pragma: no cover - fallback path
            logger.exception("Failed to initialize curl_cffi session; falling back to requests")
    return HttpClient(session=requests.Session(), use_curl_cffi=False)


def _retry_url(url: str, attempt: int) -> str:
    if attempt <= 0:
        return url
    separator = "&" if "?" in url else "?"
    nonce = int(time.time() * 1000) % 1_000_000
    return f"{url}{separator}_retry={attempt}&_nonce={nonce}"


def _session_get(client: HttpClient, url: str, *, timeout: int, workaround_mode: bool = False):
    headers = dict(REQUEST_HEADERS)
    headers["User-Agent"] = random.choice(MODERN_USER_AGENTS)
    headers["Referer"] = HOUSING_BASE_URL + "/"
    if workaround_mode:
        headers["Accept"] = "text/html,application/json;q=0.9,*/*;q=0.8"
        headers["Accept-Language"] = random.choice(("en-US,en;q=0.9", "en-IN,en;q=0.9"))

    if client.use_curl_cffi:
        return client.session.get(
            url,
            headers=headers,
            timeout=timeout,
            impersonate=random.choice(IMPERSONATE_IDS),
            allow_redirects=True,
        )
    return client.session.get(url, headers=headers, timeout=timeout, allow_redirects=True)


def _looks_blocked(text: str) -> bool:
    lowered = (text or "").lower()
    if any(marker in lowered for marker in HARD_BLOCK_MARKERS):
        return True
    if any(marker in lowered for marker in SOFT_BLOCK_MARKERS):
        if not any(marker in lowered for marker in SOFT_BLOCK_ALLOW_MARKERS):
            return True
    return False


def _fetch(
    client: HttpClient,
    url: str,
    *,
    retries: int = 1,
    timeout: int | None = None,
    base_settings: HousingBaseSettings | None = None,
) -> str:
    settings = base_settings or BASE_SETTINGS
    tout = timeout or settings.request_timeout
    last_exc: Exception | None = None

    for attempt in range(max(1, retries)):
        fetch_url = _retry_url(url, attempt)
        workaround_mode = attempt > 0
        try:
            response = _session_get(client, fetch_url, timeout=tout, workaround_mode=workaround_mode)
            status_code = getattr(response, "status_code", None)
            if status_code in RETRYABLE_HTTP_STATUS:
                raise RuntimeError(f"Retryable status code {status_code}")
            response.raise_for_status()
            text = response.text or ""
            if _looks_blocked(text):
                raise RuntimeError("Received blocked/captcha response")
            return text
        except Exception as exc:  # pragma: no cover - networking best effort
            last_exc = exc
            base_sleep = settings.min_delay_seconds * (attempt + 1)
            jitter = random.uniform(0, settings.min_delay_seconds)
            sleep_for = min(base_sleep + jitter, settings.max_delay_seconds * 2)
            logger.warning("Request failed for %s (%s). Retrying in %.1fs", fetch_url, exc, sleep_for)
            time.sleep(max(0.1, sleep_for))

    raise RuntimeError(f"Failed to fetch {url}: {last_exc}")


def _loads_json_best_effort(raw: str) -> Any:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        pass
    try:
        return json.loads(html.unescape(text))
    except Exception:
        pass
    if json_repair is not None:  # pragma: no branch - optional fallback
        try:
            return json_repair.loads(text)
        except Exception:
            pass
    return None


def _load_next_data(html_text: str) -> dict[str, Any] | None:
    script_match = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', html_text, re.S | re.I)
    if script_match:
        payload = _loads_json_best_effort(script_match.group(1))
        if isinstance(payload, dict):
            return payload

    assign_match = re.search(r"__NEXT_DATA__\s*=\s*(\{.*?\})\s*;", html_text, re.S | re.I)
    if assign_match:
        payload = _loads_json_best_effort(assign_match.group(1))
        if isinstance(payload, dict):
            return payload
    return None


def _iter_jsonld_objects(html_text: str) -> Iterable[dict[str, Any]]:
    pattern = re.compile(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.S | re.I)
    for match in pattern.finditer(html_text):
        payload = _loads_json_best_effort(match.group(1).strip())
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    yield item
        elif isinstance(payload, dict):
            yield payload


def _iter_nodes(node: Any) -> Iterable[dict[str, Any]]:
    stack: list[Any] = [node]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            yield current
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)


def _normalize_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped.upper() == "NULL":
            return None
        return stripped
    return value


def _unwrap_value(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    if isinstance(value, dict):
        for key in ("value", "label", "name", "title", "text"):
            if key in value and value[key] not in (None, "", []):
                return value[key]
    return value


def _as_text(value: Any) -> str | None:
    value = _unwrap_value(value)
    value = _normalize_scalar(value)
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return str(value)
    return str(value).strip()


def _pick(data: Mapping[str, Any] | None, keys: Iterable[str]) -> Any:
    if not isinstance(data, Mapping):
        return None
    for key in keys:
        if key in data and data[key] not in (None, "", []):
            return data[key]
    lowered = {str(k).lower(): k for k in data.keys()}
    for key in keys:
        actual = lowered.get(str(key).lower())
        if actual is None:
            continue
        value = data.get(actual)
        if value not in (None, "", []):
            return value
    return None


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


def _detect_currency(text: str | None) -> str | None:
    if not text:
        return None
    lowered = text.lower()
    if "inr" in lowered or "rs" in lowered or "lakh" in lowered or "lac" in lowered or "crore" in lowered:
        return "INR"
    if "aed" in lowered or "dirham" in lowered or "dhs" in lowered:
        return "AED"
    if "sar" in lowered or "riyal" in lowered:
        return "SAR"
    if "usd" in lowered or "$" in lowered:
        return "USD"
    return None


def _parse_price(value: Any, default_currency: str | None = "INR") -> tuple[int | None, int | None, str | None]:
    if value is None:
        return None, None, default_currency
    if isinstance(value, (int, float)):
        return int(value), None, default_currency

    text = _as_text(value)
    if not text:
        return None, None, default_currency

    lowered = text.lower()
    if "price on request" in lowered:
        return None, None, _detect_currency(lowered) or default_currency

    currency = _detect_currency(text) or default_currency

    cleaned = re.sub(r"(inr|rs\.?|aed|sar|usd|dirhams?|dhs|riyals?)", " ", text, flags=re.I)
    cleaned = cleaned.replace("$", " ")
    matches = re.findall(r"([0-9]+(?:[\.,][0-9]+)?)\s*([a-zA-Z]+)?", cleaned)
    if not matches:
        return None, None, currency

    values: list[float] = []
    for number, unit in matches:
        try:
            parsed = float(number.replace(",", ""))
        except Exception:
            continue
        if unit:
            multiplier = PRICE_MULTIPLIERS.get(unit.strip().lower())
            if multiplier:
                parsed *= multiplier
        values.append(parsed)

    if not values:
        return None, None, currency
    if len(values) == 1:
        return int(values[0]), None, currency
    return int(min(values)), int(max(values)), currency


def _parse_area(value: Any) -> tuple[float | None, float | None]:
    if value is None:
        return None, None
    if isinstance(value, (int, float)):
        sqft = float(value)
        sqm = round(sqft / 10.7639, 3)
        return sqft, sqm

    text = _as_text(value)
    if not text:
        return None, None

    match = re.search(
        r"([0-9]+(?:[\.,][0-9]+)?)\s*(sq\.?\s*ft|sqft|sq\.?\s*m|sqm|sq\.?\s*yd|sqyd|acre|acres)",
        text,
        re.I,
    )
    if not match:
        sqft = _maybe_float(text)
        if sqft is None:
            return None, None
        return sqft, round(sqft / 10.7639, 3)

    number = float(match.group(1).replace(",", ""))
    unit = match.group(2).lower().replace(" ", "").replace(".", "")

    if "sqft" in unit:
        sqft = number
        return sqft, round(sqft / 10.7639, 3)
    if "sqm" in unit:
        sqm = number
        return round(sqm * 10.7639, 3), sqm
    if "sqyd" in unit:
        sqft = number * 9.0
        return round(sqft, 3), round(sqft / 10.7639, 3)
    if "acre" in unit:
        sqft = number * 43_560.0
        return round(sqft, 3), round(sqft / 10.7639, 3)

    return None, None


def _normalize_url(url: Any) -> str | None:
    text = _as_text(url)
    if not text:
        return None
    if text.startswith("http"):
        return text
    if text.startswith("//"):
        return f"https:{text}"
    return urljoin(HOUSING_BASE_URL, text)


def _is_housing_url(url: str | None) -> bool:
    if not url:
        return False
    host = (urlparse(url).netloc or "").lower()
    return host.endswith("housing.com")


def _name_from_url(url: str | None) -> str | None:
    if not url:
        return None
    path = urlparse(url).path.strip("/")
    if not path:
        return None
    slug = unquote(path.split("/")[-1])
    slug = html.unescape(slug)
    slug = re.sub(r"-prjid-\d+$", "", slug, flags=re.I)
    slug = re.sub(r"-\d+$", "", slug)
    slug = slug.replace("+", " ").replace("-", " ").replace("_", " ")
    slug = re.sub(r"[^\w\s]", " ", slug)
    slug = re.sub(r"\s+", " ", slug).strip()
    if len(slug) < 3:
        return None
    return slug.title()


def _normalize_amenities(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip().title() for v in value if str(v).strip()]
    text = _as_text(value)
    if not text:
        return []
    return [part.strip().title() for part in re.split(r"[,;/|]", text) if part.strip()]


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
        "duplex": "duplex",
        "penthouse": "penthouse",
        "studio": "studio",
        "plot": "plot",
        "land": "land",
        "commercial": "commercial",
        "office": "office",
        "retail": "retail",
        "shop": "retail",
        "showroom": "showroom",
        "warehouse": "warehouse",
        "pg": "pg",
        "co-living": "co-living",
    }

    normalized: list[str] = []
    for part in raw_parts:
        mapped = None
        for key, val in type_map.items():
            if key in part:
                mapped = val
                break
        normalized.append(mapped or part)
    return list(dict.fromkeys(normalized))


def _type_from_category(category: str) -> list[str]:
    mapping = {
        "buy": ["residential"],
        "rent": ["residential"],
        "commercial": ["commercial"],
        "pg": ["pg", "co-living"],
        "plots": ["plot"],
    }
    return mapping.get(category, [category])


def _normalize_configurations(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw_values = [str(v).strip() for v in value if str(v).strip()]
    else:
        text = _as_text(value)
        if not text:
            return []
        raw_values = [part.strip() for part in re.split(r"[,;/|&+]", text) if part.strip()]

    configurations: list[str] = []
    for part in raw_values:
        normalized = part.lower().replace(" ", "")
        m_bhk = re.match(r"(\d+(?:\.\d+)?)bhk", normalized)
        if m_bhk:
            configurations.append(f"{m_bhk.group(1)}BHK")
            continue
        m_numeric = re.match(r"^\d+$", normalized)
        if m_numeric:
            configurations.append(f"{m_numeric.group(0)}BHK")
            continue
        if "studio" in normalized:
            configurations.append("Studio")
            continue
        if "rk" in normalized:
            m_rk = re.search(r"(\d+)", normalized)
            if m_rk:
                configurations.append(f"{m_rk.group(1)}RK")
            else:
                configurations.append("RK")
            continue
        configurations.append(part)
    return list(dict.fromkeys(configurations))


def _normalize_possession_date(value: Any) -> str | None:
    text = _as_text(value)
    if not text:
        return None

    raw = html.unescape(text).replace("\xa0", " ").strip().lower()
    raw = re.sub(r"\s+", " ", raw)
    raw = re.sub(
        r"^(date|by|in|from|starting|around|expected|estimated|possession|completion|delivery|handover|occupancy)\s*[:\-]?\s*",
        "",
        raw,
    ).strip(" .,:;-")

    if not raw:
        return None
    if "ready to move" in raw or "ready for occupancy" in raw:
        return None

    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    m = re.match(r"^(\d{4})-(\d{2})$", raw)
    if m:
        return raw

    m = re.match(r"^([a-z]{3,9})[\s,\-/]+(\d{4})$", raw)
    if m:
        month = MONTH_MAP.get(m.group(1))
        if month:
            return f"{m.group(2)}-{month}"

    m = re.match(r"^(q[1-4])[\s\-/]+(\d{4})$", raw)
    if m:
        month = QUARTER_MAP.get(m.group(1))
        if month:
            return f"{m.group(2)}-{month}"

    m = re.match(r"^(\d{4})$", raw)
    if m:
        return f"{m.group(1)}-01"
    return None


def _extract_images(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        images = [str(v).strip() for v in value if str(v).strip()]
        return images or None
    text = _as_text(value)
    if not text:
        return None
    return [text]


def _is_generic_title(value: str | None) -> bool:
    text = (value or "").strip().lower()
    if not text:
        return True
    if len(text) < 3:
        return True
    if text in {"details", "view details", "read more", "project", "property", "buy", "rent"}:
        return True
    return any(token in text for token in GENERIC_TITLE_TOKENS)


def _looks_like_listing_link(url: str, category: str) -> bool:
    lowered = url.lower()
    if not _is_housing_url(url):
        return False
    if re.search(r"[?&]page=", lowered):
        return False
    if re.search(r"/page-?\d+\b", lowered):
        return False
    if any(token in lowered for token in ("/login", "/contact", "/privacy", "/terms", "/news", "/guides")):
        return False

    parsed = urlparse(lowered)
    segments = [part for part in parsed.path.strip("/").split("/") if part]
    leaf = segments[-1] if segments else ""
    if leaf in {"buy", "rent", "commercial", "pg", "plots", "search", "resale", "projects", "properties"}:
        return False

    if "-prjid-" in lowered or "/in/projects/" in lowered:
        return True

    if f"/in/{category}/" in lowered:
        compact_leaf = re.sub(r"[^a-z0-9]", "", leaf)
        if len(compact_leaf) >= 5 and "project" in lowered:
            return True

    return False


def _stable_id(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def _score_candidate_dict(item: dict[str, Any]) -> float:
    if not isinstance(item, dict):
        return 0.0
    keyset = set(item.keys())
    score = 0.0
    if keyset & set(URL_KEYS):
        score += 3.0
    if keyset & set(TITLE_KEYS):
        score += 2.0
    if keyset & set(PRICE_KEYS):
        score += 1.5
    if keyset & set(ID_KEYS):
        score += 1.0
    if keyset & set(AREA_KEYS):
        score += 1.0
    if keyset & set(BHK_KEYS):
        score += 1.0
    return score


def _listing_from_candidate(candidate: dict[str, Any], *, category: str, city_label: str, city_slug: str) -> dict[str, Any] | None:
    if _score_candidate_dict(candidate) < 3.0:
        return None

    raw_url = _pick(candidate, URL_KEYS)
    detail_url = _normalize_url(raw_url)
    if not detail_url or not _is_housing_url(detail_url):
        return None

    title = _as_text(_pick(candidate, TITLE_KEYS)) or _name_from_url(detail_url)
    if _is_generic_title(title):
        return None

    price_min, price_max, price_currency = _parse_price(_pick(candidate, PRICE_KEYS), default_currency="INR")
    area_sqft, area_sqm = _parse_area(_pick(candidate, AREA_KEYS))
    configurations = _normalize_configurations(_pick(candidate, CONFIG_KEYS))
    property_types = _normalize_property_types(_pick(candidate, TYPE_KEYS))
    if not property_types:
        property_types = _type_from_category(category)

    bedrooms = _maybe_int(_pick(candidate, BHK_KEYS))
    bathrooms = _maybe_int(_pick(candidate, BATH_KEYS))
    price_per_sqft = _maybe_float(_pick(candidate, PRICE_PER_SQFT_KEYS))
    builder_name = _as_text(_pick(candidate, BUILDER_KEYS))
    locality = _as_text(_pick(candidate, LOCALITY_KEYS))
    city = _as_text(_pick(candidate, CITY_KEYS)) or city_label
    listing_status = _as_text(_pick(candidate, STATUS_KEYS))
    latitude = _maybe_float(_pick(candidate, LAT_KEYS))
    longitude = _maybe_float(_pick(candidate, LNG_KEYS))
    rera_number = _as_text(_pick(candidate, RERA_KEYS))
    possession_date = _normalize_possession_date(_pick(candidate, POSSESSION_KEYS))
    amenities = _normalize_amenities(_pick(candidate, AMENITY_KEYS))
    images = _extract_images(_pick(candidate, IMAGE_KEYS))

    record_id = _as_text(_pick(candidate, ID_KEYS))
    if not record_id:
        record_id = _stable_id(detail_url)

    listing: dict[str, Any] = {
        "id": record_id,
        "title": title,
        "builder_name": builder_name,
        "price_min": price_min,
        "price_max": price_max,
        "price_currency": price_currency,
        "price_per_sqft": price_per_sqft,
        "area_sqft": area_sqft,
        "area_sqm": area_sqm,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "property_types": property_types,
        "configurations": configurations,
        "listing_status": listing_status,
        "city": city,
        "city_slug": city_slug,
        "locality": locality,
        "latitude": latitude,
        "longitude": longitude,
        "detail_url": detail_url,
        "rera_number": rera_number,
        "possession_date": possession_date,
        "amenities": amenities,
        "images": images,
        "listing_category": category,
        "source": SOURCE_NAME,
        "raw_listing": candidate,
    }
    return {k: v for k, v in listing.items() if v not in (None, "", [])}


def _listing_from_jsonld(item: dict[str, Any], *, category: str, city_label: str, city_slug: str) -> dict[str, Any] | None:
    candidate = dict(item)
    if "item" in candidate and isinstance(candidate["item"], dict):
        inner = candidate["item"]
        candidate.update(inner)
    if "offers" in candidate and isinstance(candidate["offers"], dict):
        offers = candidate["offers"]
        if "price" in offers and "price" not in candidate:
            candidate["price"] = offers["price"]
        if "priceCurrency" in offers and "price_currency" not in candidate:
            candidate["price_currency"] = offers["priceCurrency"]
    return _listing_from_candidate(candidate, category=category, city_label=city_label, city_slug=city_slug)


def _extract_records_from_links(html_text: str, *, category: str, city_label: str, city_slug: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    pattern = re.compile(r"<a[^>]+href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", re.I | re.S)
    for href, raw_text in pattern.findall(html_text):
        detail_url = _normalize_url(href)
        if not detail_url or not _looks_like_listing_link(detail_url, category):
            continue
        text = re.sub(r"<[^>]+>", " ", raw_text)
        text = html.unescape(text).replace("\xa0", " ")
        text = re.sub(r"\s+", " ", text).strip()

        title = text if not _is_generic_title(text) else _name_from_url(detail_url)
        if _is_generic_title(title):
            continue

        record = {
            "id": _stable_id(detail_url),
            "title": title,
            "detail_url": detail_url,
            "city": city_label,
            "city_slug": city_slug,
            "listing_category": category,
            "property_types": _type_from_category(category),
            "source": SOURCE_NAME,
        }
        records.append(record)
    return records


def _dedupe_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for record in records:
        key = _as_text(record.get("id")) or _as_text(record.get("detail_url"))
        if not key:
            continue
        deduped[key] = record
    return list(deduped.values())


def _merge_unique(existing: Any, incoming: Any) -> list[str]:
    values: list[str] = []
    for part in (existing or []):
        text = _as_text(part)
        if text and text not in values:
            values.append(text)
    for part in (incoming or []):
        text = _as_text(part)
        if text and text not in values:
            values.append(text)
    return values


def _validate_record(record: dict[str, Any]) -> dict[str, Any]:
    missing = [field for field in MANDATORY_FIELDS if not _as_text(record.get(field))]
    if missing:
        raise ValueError(f"Missing mandatory fields: {', '.join(missing)}")

    title = _as_text(record.get("title"))
    if _is_generic_title(title):
        raise ValueError("Generic title")

    detail_url = _as_text(record.get("detail_url"))
    if not detail_url or not _is_housing_url(detail_url):
        raise ValueError("Invalid/non-Housing detail_url")

    price_min = record.get("price_min")
    price_max = record.get("price_max")
    if isinstance(price_min, (int, float)) and isinstance(price_max, (int, float)) and price_min > price_max:
        record["price_min"], record["price_max"] = int(price_max), int(price_min)

    return {key: value for key, value in record.items() if value not in (None, "", [])}


def parse_housing_listing_page(
    html_text: str,
    *,
    category: str = "buy",
    city_label: str = "",
    city_slug: str = "",
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []

    next_data = _load_next_data(html_text)
    if next_data:
        for node in _iter_nodes(next_data):
            listing = _listing_from_candidate(node, category=category, city_label=city_label, city_slug=city_slug)
            if listing:
                records.append(listing)

    if not records:
        for obj in _iter_jsonld_objects(html_text):
            obj_type = str(obj.get("@type", "")).lower()
            if obj_type == "itemlist":
                for item in obj.get("itemListElement", []) or []:
                    if isinstance(item, dict):
                        listing = _listing_from_jsonld(item, category=category, city_label=city_label, city_slug=city_slug)
                        if listing:
                            records.append(listing)
            else:
                listing = _listing_from_jsonld(obj, category=category, city_label=city_label, city_slug=city_slug)
                if listing:
                    records.append(listing)

    if not records:
        records.extend(_extract_records_from_links(html_text, category=category, city_label=city_label, city_slug=city_slug))

    if not records:
        return pd.DataFrame()

    validated: list[dict[str, Any]] = []
    for record in _dedupe_records(records):
        try:
            validated.append(_validate_record(record))
        except Exception:
            continue

    if not validated:
        return pd.DataFrame()
    return pd.DataFrame.from_records(validated)


def _score_detail_dict(item: dict[str, Any]) -> float:
    if not isinstance(item, dict):
        return 0.0
    keyset = set(item.keys())
    score = 0.0
    for key in ("description", "amenities", "features", "address", "location"):
        if key in keyset:
            score += 1.0
    for key in ("price", "priceRange", "images", "gallery", "projectName"):
        if key in keyset:
            score += 1.0
    for key in ("bhk", "bedrooms", "bathrooms", "area", "propertyType"):
        if key in keyset:
            score += 0.5
    if keyset & set(URL_KEYS):
        score += 0.5
    return score


def _find_best_detail(payload: dict[str, Any]) -> dict[str, Any] | None:
    best: dict[str, Any] | None = None
    best_score = 0.0
    for node in _iter_nodes(payload):
        score = _score_detail_dict(node)
        if score > best_score:
            best_score = score
            best = node
    return best


def _extract_meta_map(html_text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for match in re.finditer(r"<meta[^>]+>", html_text, re.I):
        tag = match.group(0)
        key_match = re.search(r"(?:name|property)\s*=\s*[\"']([^\"']+)[\"']", tag, re.I)
        value_match = re.search(r"content\s*=\s*[\"']([^\"']*)[\"']", tag, re.I | re.S)
        if not key_match or not value_match:
            continue
        key = key_match.group(1).strip().lower()
        value = html.unescape(value_match.group(1)).strip()
        if key and value:
            out[key] = value
    return out


def _extract_rera_number(text: str) -> str | None:
    if not text:
        return None
    m = re.search(r"\b[A-Z]{0,4}-?RERA\/[A-Z0-9\-/]+\b", text, re.I)
    if m:
        return m.group(0).upper()
    m = re.search(r"\bRERA\s*[:\-]\s*([A-Z0-9\-/]+)\b", text, re.I)
    if m:
        return m.group(1).upper()
    return None


def _extract_possession_from_text(text: str) -> str | None:
    if not text:
        return None
    patterns = (
        r"(?:possession|completion|handover|delivery)\W{0,24}(?:date|status)?\W{0,24}(?:by|in|from|expected|estimated|around)?\W{0,24}(Q[1-4]\s*\d{4}|[A-Za-z]{3,9}\s+\d{4}|\d{4})",
        r"(?:expected|estimated)\W{0,16}(?:possession|completion)?\W{0,16}(Q[1-4]\s*\d{4}|[A-Za-z]{3,9}\s+\d{4}|\d{4})",
    )
    for pattern in patterns:
        m = re.search(pattern, text, re.I)
        if not m:
            continue
        normalized = _normalize_possession_date(m.group(1))
        if normalized:
            return normalized
    return None


def _to_epoch_and_iso(value: Any) -> tuple[int | None, str | None]:
    if value is None:
        return None, None
    if isinstance(value, (int, float)):
        epoch = int(value)
        try:
            iso = datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
        except Exception:
            iso = None
        return epoch, iso
    text = _as_text(value)
    if not text:
        return None, None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        return int(dt.timestamp()), dt.isoformat()
    except Exception:
        return None, None


def _detail_from_jsonld(obj: dict[str, Any]) -> dict[str, Any]:
    offers = obj.get("offers") if isinstance(obj.get("offers"), dict) else {}
    price_value = offers.get("price") if offers else None
    if price_value is None:
        price_value = obj.get("price")
    price_min, price_max, price_currency = _parse_price(price_value)

    address = obj.get("address") if isinstance(obj.get("address"), dict) else {}
    geo = obj.get("geo") if isinstance(obj.get("geo"), dict) else {}

    detail = {
        "detail_title": _as_text(obj.get("name")),
        "detail_description": _as_text(obj.get("description")),
        "detail_price_min": price_min,
        "detail_price_max": price_max,
        "detail_price_currency": price_currency or _as_text(offers.get("priceCurrency")) if offers else price_currency,
        "detail_images": _extract_images(obj.get("image")),
        "detail_locality": _as_text(address.get("addressLocality")),
        "detail_city": _as_text(address.get("addressLocality")),
        "detail_state": _as_text(address.get("addressRegion")),
        "detail_latitude": _maybe_float(geo.get("latitude")),
        "detail_longitude": _maybe_float(geo.get("longitude")),
    }
    return {k: v for k, v in detail.items() if v not in (None, "", [])}


def _detail_from_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    price_min, price_max, price_currency = _parse_price(_pick(candidate, PRICE_KEYS))
    area_sqft, area_sqm = _parse_area(_pick(candidate, AREA_KEYS))

    detail: dict[str, Any] = {
        "detail_title": _as_text(_pick(candidate, TITLE_KEYS)),
        "detail_builder_name": _as_text(_pick(candidate, BUILDER_KEYS)),
        "detail_price_min": price_min,
        "detail_price_max": price_max,
        "detail_price_currency": price_currency,
        "detail_price_per_sqft": _maybe_float(_pick(candidate, PRICE_PER_SQFT_KEYS)),
        "detail_area_sqft": area_sqft,
        "detail_area_sqm": area_sqm,
        "detail_bedrooms": _maybe_int(_pick(candidate, BHK_KEYS)),
        "detail_bathrooms": _maybe_int(_pick(candidate, BATH_KEYS)),
        "detail_property_types": _normalize_property_types(_pick(candidate, TYPE_KEYS)),
        "detail_configurations": _normalize_configurations(_pick(candidate, CONFIG_KEYS)),
        "detail_locality": _as_text(_pick(candidate, LOCALITY_KEYS)),
        "detail_city": _as_text(_pick(candidate, CITY_KEYS)),
        "detail_latitude": _maybe_float(_pick(candidate, LAT_KEYS)),
        "detail_longitude": _maybe_float(_pick(candidate, LNG_KEYS)),
        "detail_rera_number": _as_text(_pick(candidate, RERA_KEYS)),
        "detail_possession_date": _normalize_possession_date(_pick(candidate, POSSESSION_KEYS)),
        "detail_amenities": _normalize_amenities(_pick(candidate, AMENITY_KEYS)),
        "detail_images": _extract_images(_pick(candidate, IMAGE_KEYS)),
    }

    for key in ("updatedAt", "modifiedAt", "createdAt", "postedAt", "postedDate"):
        if key in candidate:
            epoch, iso = _to_epoch_and_iso(candidate.get(key))
            detail["detail_updated_epoch"] = epoch
            detail["detail_updated_iso"] = iso
            break

    return {k: v for k, v in detail.items() if v not in (None, "", [])}


def parse_housing_detail_page(html_text: str) -> dict[str, Any]:
    detail: dict[str, Any] = {}

    for obj in _iter_jsonld_objects(html_text):
        obj_type = str(obj.get("@type", "")).lower()
        if obj_type and obj_type != "itemlist":
            detail.update(_detail_from_jsonld(obj))
            break

    next_data = _load_next_data(html_text)
    if next_data:
        best = _find_best_detail(next_data)
        if isinstance(best, dict):
            detail.update(_detail_from_candidate(best))
            detail["detail_raw"] = best

    meta = _extract_meta_map(html_text)
    if "detail_title" not in detail:
        detail["detail_title"] = meta.get("og:title") or meta.get("twitter:title")
    if "detail_description" not in detail:
        detail["detail_description"] = meta.get("og:description") or meta.get("description")

    page_text = re.sub(r"<script[^>]*>.*?</script>", " ", html_text, flags=re.S | re.I)
    page_text = re.sub(r"<style[^>]*>.*?</style>", " ", page_text, flags=re.S | re.I)
    page_text = re.sub(r"<[^>]+>", " ", page_text)
    page_text = html.unescape(page_text).replace("\xa0", " ")
    page_text = re.sub(r"\s+", " ", page_text).strip()

    context_text = " ".join(
        [
            page_text,
            _as_text(detail.get("detail_description")) or "",
            meta.get("description", ""),
            meta.get("og:description", ""),
        ]
    )

    if "detail_rera_number" not in detail:
        detail["detail_rera_number"] = _extract_rera_number(context_text)
    if "detail_possession_date" not in detail:
        detail["detail_possession_date"] = _extract_possession_from_text(context_text)

    return {k: v for k, v in detail.items() if v not in (None, "", [])}


def _merge_detail_payload(record: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    if not detail:
        return record

    if not record.get("title") and detail.get("detail_title"):
        record["title"] = detail["detail_title"]
    if not record.get("builder_name") and detail.get("detail_builder_name"):
        record["builder_name"] = detail["detail_builder_name"]
    if record.get("price_min") is None and detail.get("detail_price_min") is not None:
        record["price_min"] = detail["detail_price_min"]
    if record.get("price_max") is None and detail.get("detail_price_max") is not None:
        record["price_max"] = detail["detail_price_max"]
    if not record.get("price_currency") and detail.get("detail_price_currency"):
        record["price_currency"] = detail["detail_price_currency"]
    if record.get("price_per_sqft") is None and detail.get("detail_price_per_sqft") is not None:
        record["price_per_sqft"] = detail["detail_price_per_sqft"]

    if record.get("area_sqft") is None and detail.get("detail_area_sqft") is not None:
        record["area_sqft"] = detail["detail_area_sqft"]
    if record.get("area_sqm") is None and detail.get("detail_area_sqm") is not None:
        record["area_sqm"] = detail["detail_area_sqm"]

    if record.get("bedrooms") is None and detail.get("detail_bedrooms") is not None:
        record["bedrooms"] = detail["detail_bedrooms"]
    if record.get("bathrooms") is None and detail.get("detail_bathrooms") is not None:
        record["bathrooms"] = detail["detail_bathrooms"]

    if detail.get("detail_property_types"):
        record["property_types"] = _merge_unique(record.get("property_types"), detail.get("detail_property_types"))
    if detail.get("detail_configurations"):
        record["configurations"] = _merge_unique(record.get("configurations"), detail.get("detail_configurations"))

    if not record.get("locality") and detail.get("detail_locality"):
        record["locality"] = detail["detail_locality"]
    if not record.get("city") and detail.get("detail_city"):
        record["city"] = detail["detail_city"]

    if record.get("latitude") is None and detail.get("detail_latitude") is not None:
        record["latitude"] = detail["detail_latitude"]
    if record.get("longitude") is None and detail.get("detail_longitude") is not None:
        record["longitude"] = detail["detail_longitude"]

    if not record.get("rera_number") and detail.get("detail_rera_number"):
        record["rera_number"] = detail["detail_rera_number"]
    if not record.get("possession_date") and detail.get("detail_possession_date"):
        record["possession_date"] = detail["detail_possession_date"]

    if detail.get("detail_amenities"):
        record["amenities"] = _merge_unique(record.get("amenities"), detail.get("detail_amenities"))
    if detail.get("detail_images"):
        record["images"] = _merge_unique(record.get("images"), detail.get("detail_images"))

    for key, value in detail.items():
        if value not in (None, "", []):
            record[key] = value

    return record


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
    except Exception as exc:  # pragma: no cover - best effort
        logger.warning("Failed to fetch ES info: %s", exc)
    return es


def ensure_index(es: Elasticsearch, index: str) -> None:
    if not es.indices.exists(index=index):
        es.indices.create(index=index, body=ES_INDEX_MAPPING)


def es_doc_exists(es: Elasticsearch, index: str, doc_id: str) -> bool:
    try:
        return bool(es.exists(index=index, id=doc_id))
    except Exception:
        return False


def df_to_actions(df: pd.DataFrame, default_index: str | None) -> Iterable[dict[str, Any]]:
    clean = df.replace({np.nan: None})
    for record in clean.to_dict(orient="records"):
        target_index = record.pop("_target_index", None) or default_index
        if not target_index:
            raise ValueError("Missing target index for Housing.com document")
        doc_id = _as_text(record.get("id"))
        if not doc_id:
            continue
        yield {
            "_index": target_index,
            "_id": doc_id,
            "_op_type": "index",
            "_source": record,
        }


async def _fetch_detail_with_retry(
    client: HttpClient,
    url: str | None,
    base_settings: HousingBaseSettings,
) -> dict[str, Any]:
    if not url:
        return {}
    for attempt in range(base_settings.detail_retry_count):
        try:
            html_text = _fetch(client, url, retries=2, base_settings=base_settings)
            detail = parse_housing_detail_page(html_text)
            if detail:
                return detail
        except Exception as exc:  # pragma: no cover - observability
            logger.warning(
                "Detail fetch failed for %s (attempt %s/%s): %s",
                url,
                attempt + 1,
                base_settings.detail_retry_count,
                exc,
            )
        await asyncio.sleep(random.uniform(base_settings.min_delay_seconds, base_settings.max_delay_seconds))
    return {}


def _job_output_dir(base_dir: Path, job: HousingJobSettings) -> Path:
    out_dir = base_dir / job.category / job.city_slug
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


async def _scrape_job(
    client: HttpClient,
    es: Elasticsearch,
    job: HousingJobSettings,
    *,
    base_settings: HousingBaseSettings | None = None,
) -> list[dict[str, Any]]:
    settings = base_settings or BASE_SETTINGS
    ensure_index(es, job.es_index)

    job_rows: list[dict[str, Any]] = []
    done = False

    for page in range(1, job.pages + 1):
        url = _build_listing_page_url(job, page)
        logger.info("Fetching Housing.com %s listing page %s", job.name, url)
        listing_html = _fetch(client, url, retries=2, base_settings=settings)

        df = parse_housing_listing_page(
            listing_html,
            category=job.category,
            city_label=job.city_label,
            city_slug=job.city_slug,
        )
        if df.empty:
            logger.info("No %s listings parsed from page %s, stopping job.", job.name, page)
            break
        logger.info("Parsed %d Housing.com %s listings from page %d", len(df), job.name, page)

        for record in df.to_dict(orient="records"):
            record.setdefault("listing_category", job.category)
            record.setdefault("city_slug", job.city_slug)
            record.setdefault("city", job.city_label)
            record["source_page"] = page
            record["source"] = SOURCE_NAME

            rec_id = _as_text(record.get("id"))
            if rec_id and settings.stop_on_existing and es_doc_exists(es, job.es_index, rec_id):
                logger.info("Encountered existing listing id=%s in job=%s; stopping pagination.", rec_id, job.name)
                done = True
                break

            detail_payload = await _fetch_detail_with_retry(client, _as_text(record.get("detail_url")), settings)
            record = _merge_detail_payload(record, detail_payload)

            try:
                validated = _validate_record(record)
            except Exception as exc:
                logger.debug("Skipping invalid housing record (%s): %s", job.name, exc)
                continue

            validated["_target_index"] = job.es_index
            job_rows.append(validated)

            await asyncio.sleep(random.uniform(settings.min_delay_seconds, settings.max_delay_seconds))

        out_dir = _job_output_dir(settings.data_dir, job)
        csv_path = out_dir / f"page_{page}.csv"
        df.to_csv(csv_path, index=False)
        logger.info("Saved raw %s listings to %s", job.name, csv_path)

        if done:
            break

    return job_rows


@task
async def housing_property_data() -> None:
    if not BASE_SETTINGS.enabled:
        logger.info("Housing.com scraper disabled via config. Skipping run.")
        return
    if not JOB_SETTINGS:
        raise ValueError("No Housing.com job settings available")

    BASE_SETTINGS.data_dir.mkdir(parents=True, exist_ok=True)

    client = _build_http_client()
    es = es_client()

    all_rows: list[dict[str, Any]] = []

    for job in JOB_SETTINGS:
        try:
            job_rows = await _scrape_job(client, es, job, base_settings=BASE_SETTINGS)
        except Exception as exc:
            logger.exception("Housing.com job failed for %s: %s", job.name, exc)
            continue

        if not job_rows:
            logger.info("No rows collected for Housing.com job %s", job.name)
            continue

        job_df = pd.DataFrame(job_rows)
        dedupe_keys = ["id", "listing_category", "city_slug"]
        job_df = job_df.drop_duplicates(subset=dedupe_keys, keep="last").reset_index(drop=True)

        logger.info("Indexing %d Housing.com %s documents into ES index %s", len(job_df), job.name, job.es_index)
        bulk_resp = helpers.bulk(
            es,
            df_to_actions(job_df, default_index=None),
            chunk_size=500,
            request_timeout=120,
            raise_on_error=False,
            raise_on_exception=False,
        )
        logger.info("ES bulk response for Housing.com job %s: %s", job.name, bulk_resp)

        all_rows.extend(job_rows)

    final_df = pd.DataFrame(all_rows)
    if final_df.empty:
        raise ValueError("No Housing.com records collected across configured jobs")

    dedupe_keys = ["id", "listing_category", "city_slug"]
    final_df = final_df.drop_duplicates(subset=dedupe_keys, keep="last").reset_index(drop=True)

    out_json = Path("housing_listings.json")
    json_df = final_df.drop(columns=["_target_index"], errors="ignore")
    json_df.to_json(out_json, orient="records", force_ascii=True)
    logger.info("Wrote %s with %d rows", out_json, len(json_df))


class InputParams(BaseModel):
    """Pipeline parameters placeholder (no external inputs required)."""


def fetch_all_docs(es: Elasticsearch, index: str, fields: list[str] | None = None):
    query: dict[str, Any] = {"query": {"match_all": {}}}
    if fields:
        query["_source"] = fields
    for doc in helpers.scan(es, index=index, query=query, preserve_order=False, size=1000):
        yield doc.get("_source", {})


@task
async def export_housing_json_to_r2() -> None:
    if not BASE_SETTINGS.enabled:
        logger.info("Housing.com export disabled because scraper is disabled.")
        return

    cloudflare_config = config["cloudflare"]
    es = es_client()

    rows: list[dict[str, Any]] = []
    indices = {job.es_index for job in JOB_SETTINGS}
    for index in sorted(indices):
        if not es.indices.exists(index=index):
            logger.warning("Index not found: %s - skipping export for this index.", index)
            continue
        rows.extend(fetch_all_docs(es, index))

    if not rows:
        raise SystemExit("No Housing.com documents returned from ES.")

    df = pd.DataFrame.from_records(rows)
    out_path = Path("housing_listings.json")
    df.to_json(out_path, orient="records", force_ascii=True)
    logger.info("Wrote %s with %d rows and %d columns", out_path, len(df), len(df.columns))

    session = boto3.session.Session()
    s3 = session.client(
        service_name="s3",
        endpoint_url=cloudflare_config["R2_ENDPOINT"],
        aws_access_key_id=cloudflare_config["ACCESS_KEY_ID"],
        aws_secret_access_key=cloudflare_config["SECRET_ACCESS_KEY"],
    )

    bucket = cloudflare_config.get("PROP_BUCKET") or cloudflare_config.get("BUCKET")
    if not bucket:
        raise KeyError("Missing PROP_BUCKET/BUCKET configuration for Cloudflare export")

    key = "data/housing_listings.json"
    s3.upload_file(
        Filename=str(out_path),
        Bucket=bucket,
        Key=key,
        ExtraArgs={
            "ContentType": "application/json",
            "ACL": "public-read",
            "CacheControl": "public, max-age=60",
        },
    )
    logger.info("Uploaded to r2://%s/%s", bucket, key)


register_pipeline(
    id="housing_pipeline",
    description="Scrape Housing.com listings across categories/cities and export snapshots.",
    tasks=[housing_property_data, export_housing_json_to_r2],
    triggers=[
        Trigger(
            id="housing_daily",
            name="Housing Daily",
            description="Run Housing.com scraper daily",
            params=InputParams(),
            schedule=CronTrigger(
                hour=BASE_SETTINGS.schedule_hour,
                minute=BASE_SETTINGS.schedule_minute,
                timezone=BASE_SETTINGS.schedule_timezone,
            ),
        )
    ],
    params=InputParams,
)
