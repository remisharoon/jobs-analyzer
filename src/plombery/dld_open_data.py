"""DLD Open Data scraper and pipeline (web version).

This module scrapes Dubai Land Department's (DLD) "Real Estate Data" portal
(`https://dubailand.gov.ae/en/open-data/real-estate-data/`).  The site is a
Next.js application that exposes all dataset payloads through the page's
``__NEXT_DATA__`` bootstrap JSON as well as dataset-specific download links.

The scraper mirrors the patterns used by the ``allsopp_crs`` module: it fetches
the page HTML, extracts structured data, normalises the payload, and indexes
each dataset into Elasticsearch before persisting CSV artefacts and maintaining
incremental state on disk.

The site occasionally returns a reCAPTCHA challenge when it suspects bot
behaviour.  The scraper detects those HTML responses and raises a dedicated
``RecaptchaBlockedError`` so the scheduler can alert operators.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

import numpy as np
import pandas as pd
import requests
from apscheduler.triggers.cron import CronTrigger
from elasticsearch import Elasticsearch, helpers
from pydantic import BaseModel

from plombery import Trigger, register_pipeline, task

try:  # pragma: no cover - fallback for local test environments
    from config import read_config
except ModuleNotFoundError:  # pragma: no cover
    from plombery.config import read_config


logger = logging.getLogger(__name__)


DATASET_KEYS = {
    "transactions": "Transactions",
    "rents": "Rents",
    "projects": "Projects",
    "valuations": "Valuations",
    "land": "Land",
    "building": "Building",
    "unit": "Unit",
    "broker": "Broker",
    "developer": "Developer",
}


REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/json,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
}

RECAPTCHA_MAX_RETRIES = 3
RECAPTCHA_BACKOFF_SECONDS = 30


@dataclass(slots=True)
class DLDBaseSettings:
    page_url: str
    lookback_days: int
    buffer_days: int
    request_timeout: int


@dataclass(slots=True)
class DatasetConfig:
    key: str
    label: str
    slug: str
    date_field: str
    es_index: str
    buffer_days: int | None = None


CONFIG_SECTION = "dld_open_data"

config = read_config()

try:
    dld_section: Mapping[str, str] = config[CONFIG_SECTION]
except KeyError:
    dld_section = {}

try:
    es_section: Mapping[str, str] = config["elasticsearch"]
except KeyError:
    es_section = {}


def _get_int(section: Mapping[str, str], key: str, default: int) -> int:
    try:
        return int(section.get(key, default))
    except (TypeError, ValueError):
        return default


BASE_SETTINGS = DLDBaseSettings(
    page_url=dld_section.get(
        "page_url",
        "https://dubailand.gov.ae/en/open-data/real-estate-data/",
    ),
    lookback_days=_get_int(dld_section, "lookback_days", 30),
    buffer_days=_get_int(dld_section, "buffer_days", 3),
    request_timeout=_get_int(dld_section, "timeout_seconds", 30),
)


def _build_dataset_config(key: str, label: str, section: Mapping[str, str]) -> DatasetConfig | None:
    date_field = section.get(f"{key}_date_field")
    es_index = section.get(f"{key}_es_index") or es_section.get("dld_index")
    if not date_field or not es_index:
        logger.debug("Skipping dataset %s: missing date_field/index", key)
        return None
    slug = section.get(f"{key}_slug") or key
    buffer_days: int | None = None
    raw_buffer = section.get(f"{key}_buffer_days")
    if raw_buffer:
        try:
            buffer_days = int(raw_buffer)
        except ValueError:
            logger.warning(
                "Invalid %s_buffer_days value '%s'; falling back to defaults.",
                key,
                raw_buffer,
            )
    return DatasetConfig(
        key=key,
        label=label,
        slug=slug.lower(),
        date_field=date_field,
        es_index=es_index,
        buffer_days=buffer_days,
    )


DATASET_CONFIGS: list[DatasetConfig] = [
    cfg
    for key, label in DATASET_KEYS.items()
    if (cfg := _build_dataset_config(key, label, dld_section))
]


STATE_PATH = Path("saved_data/dld_open_data/state.json")
STATE_PATH.parent.mkdir(parents=True, exist_ok=True)


class RecaptchaBlockedError(RuntimeError):
    """Raised when the site responds with a reCAPTCHA challenge."""


def _load_state(path: Path = STATE_PATH) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        logger.warning("Failed to parse %s; starting with empty state.", path)
        return {}


def _save_state(state: dict[str, Any], path: Path = STATE_PATH) -> None:
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        try:
            return datetime.fromisoformat(cleaned).date()
        except ValueError:
            pass
        for fmt in (
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%d/%m/%Y",
            "%m/%d/%Y",
        ):
            try:
                return datetime.strptime(cleaned[: len(fmt)], fmt).date()
            except ValueError:
                continue
    return None


def _compute_start_date(cfg: DatasetConfig, state: Mapping[str, Any]) -> date:
    buffer_days = cfg.buffer_days or BASE_SETTINGS.buffer_days
    dataset_state = state.get(cfg.key, {})
    prev_max = dataset_state.get("max_date")
    today = datetime.now(timezone.utc).date()
    if prev_max:
        prev_date = _parse_date(prev_max) or (today - timedelta(days=BASE_SETTINGS.lookback_days))
        start = prev_date - timedelta(days=buffer_days)
    else:
        start = today - timedelta(days=BASE_SETTINGS.lookback_days)
    if start > today:
        start = today
    return start


def _load_next_data(html_text: str) -> dict[str, Any]:
    marker = '<script id="__NEXT_DATA__"'
    idx = html_text.find(marker)
    if idx == -1:
        raise ValueError("Unable to locate __NEXT_DATA__ script")
    start = html_text.find(">", idx)
    if start == -1:
        raise ValueError("Malformed __NEXT_DATA__ script tag")
    start += 1
    end = html_text.find("</script>", start)
    if end == -1:
        raise ValueError("Unable to locate end of __NEXT_DATA__ script")
    payload = html_text[start:end]
    return json.loads(payload)


def _walk_json(node: Any) -> Iterator[Any]:
    """Breadth-first traversal yielding every nested mapping/list element."""

    queue: deque[Any] = deque([node])
    while queue:
        current = queue.popleft()
        yield current
        if isinstance(current, Mapping):
            queue.extend(current.values())
        elif isinstance(current, Sequence) and not isinstance(current, (str, bytes, bytearray)):
            queue.extend(current)


def _find_dataset_node(next_data: Mapping[str, Any], cfg: DatasetConfig) -> Mapping[str, Any] | None:
    target_slug = cfg.slug.lower()
    target_label = cfg.label.lower()
    for node in _walk_json(next_data):
        if not isinstance(node, Mapping):
            continue
        slug = str(node.get("slug") or node.get("key") or node.get("id") or "").lower()
        title = str(node.get("title") or node.get("name") or node.get("label") or "").lower()
        if slug == target_slug or title == target_label:
            return node
    return None


def _extract_table_node(node: Mapping[str, Any]) -> Mapping[str, Any] | None:
    if "table" in node and isinstance(node["table"], Mapping):
        return node["table"]
    for key in ("tableData", "grid", "dataTable"):
        value = node.get(key)
        if isinstance(value, Mapping):
            return value
    for value in node.values():
        if isinstance(value, Mapping):
            result = _extract_table_node(value)
            if result is not None:
                return result
    return None


def _normalise_columns(columns: Any) -> list[str]:
    names: list[str] = []
    if isinstance(columns, Sequence) and not isinstance(columns, (str, bytes, bytearray)):
        for column in columns:
            if isinstance(column, Mapping):
                candidate = (
                    column.get("dataIndex")
                    or column.get("key")
                    or column.get("field")
                    or column.get("id")
                    or column.get("name")
                    or column.get("title")
                    or column.get("label")
                )
                if candidate:
                    names.append(str(candidate))
                    continue
            elif isinstance(column, str):
                names.append(column)
        if names:
            return names
    return []


def _table_to_dataframe(table: Mapping[str, Any]) -> pd.DataFrame:
    columns = _normalise_columns(table.get("columns") or table.get("headers"))
    data = table.get("rows") or table.get("data") or table.get("body") or []
    records: list[dict[str, Any]] = []
    if isinstance(data, Mapping):
        # Some endpoints wrap rows in {"items": [...]}
        for key in ("items", "rows", "data"):
            maybe_rows = data.get(key)
            if isinstance(maybe_rows, Sequence) and not isinstance(maybe_rows, (str, bytes, bytearray)):
                data = maybe_rows
                break
    if isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
        for row in data:
            if isinstance(row, Mapping):
                records.append(dict(row))
            elif isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
                record: dict[str, Any] = {}
                for idx, value in enumerate(row):
                    key = columns[idx] if idx < len(columns) else f"column_{idx}"
                    record[key] = value
                records.append(record)
    return pd.DataFrame.from_records(records)


def _extract_data_url(node: Mapping[str, Any]) -> str | None:
    for key in ("downloadUrl", "dataUrl", "csvUrl", "apiUrl"):
        value = node.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _prepare_dataset_url(url: str, start_date: date, end_date: date) -> str:
    from_str = start_date.strftime("%Y-%m-%d")
    to_str = end_date.strftime("%Y-%m-%d")
    replacements = {
        "{fromDate}": from_str,
        "{FromDate}": from_str,
        "{fromdate}": from_str,
        "{toDate}": to_str,
        "{ToDate}": to_str,
        "{todate}": to_str,
    }
    for placeholder, value in replacements.items():
        if placeholder in url:
            url = url.replace(placeholder, value)

    parsed = urlparse(url)
    query_params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    updated = False
    for key in list(query_params.keys()):
        lower = key.lower()
        if lower in {"fromdate", "from", "from_date", "start", "startdate"}:
            query_params[key] = from_str
            updated = True
        elif lower in {"todate", "to", "to_date", "end", "enddate"}:
            query_params[key] = to_str
            updated = True
    if not updated:
        # append sensible defaults
        query_params.setdefault("FromDate", from_str)
        query_params.setdefault("ToDate", to_str)

    rebuilt = parsed._replace(query=urlencode(query_params, doseq=True))
    return urlunparse(rebuilt)


def _check_recaptcha(response: requests.Response) -> None:
    if "text/html" in response.headers.get("Content-Type", ""):
        snippet = response.text.lower()
        if "i'm not a robot" in snippet or "recaptcha" in snippet:
            raise RecaptchaBlockedError(
                "Blocked by reCAPTCHA challenge when fetching DLD data."
            )


def _get_with_recaptcha_retry(
    session: requests.Session,
    url: str,
    *,
    timeout: int,
    headers: Mapping[str, str],
) -> requests.Response:
    last_error: RecaptchaBlockedError | None = None
    for attempt in range(RECAPTCHA_MAX_RETRIES):
        response = session.get(url, timeout=timeout, headers=headers)
        try:
            _check_recaptcha(response)
        except RecaptchaBlockedError as exc:
            last_error = exc
            if attempt == RECAPTCHA_MAX_RETRIES - 1:
                raise
            backoff = RECAPTCHA_BACKOFF_SECONDS * (2**attempt)
            logger.warning(
                "Encountered reCAPTCHA challenge when fetching %s (attempt %s/%s). "
                "Retrying in %s seconds.",
                url,
                attempt + 1,
                RECAPTCHA_MAX_RETRIES,
                backoff,
            )
            time.sleep(backoff)
            continue
        return response
    if last_error is not None:  # pragma: no cover - defensive guard
        raise last_error
    raise RuntimeError("Unexpected failure fetching URL without response or error")


def _download_dataset(
    session: requests.Session,
    url: str,
) -> tuple[pd.DataFrame, str]:
    response = _get_with_recaptcha_retry(
        session,
        url,
        timeout=BASE_SETTINGS.request_timeout,
        headers=REQUEST_HEADERS,
    )
    content_type = response.headers.get("Content-Type", "").lower()
    source_url = response.url
    if "application/json" in content_type or response.text.strip().startswith("{"):
        data = response.json()
        df = _json_payload_to_df(data)
    elif "text/csv" in content_type or content_type.endswith("/csv"):
        df = pd.read_csv(io.StringIO(response.text))
    else:
        # Some endpoints return CSV without the correct header
        try:
            df = pd.read_csv(io.StringIO(response.text))
        except Exception as exc:  # pragma: no cover - defensive
            raise RuntimeError(f"Unsupported response format from {url}: {content_type}") from exc
    return df, source_url


def _json_payload_to_df(payload: Any) -> pd.DataFrame:
    if isinstance(payload, Mapping):
        if "columns" in payload and any(k in payload for k in ("rows", "data")):
            return _table_to_dataframe(payload)
        for key in ("table", "tableData", "grid", "dataTable"):
            if key in payload and isinstance(payload[key], Mapping):
                return _table_to_dataframe(payload[key])
        for key in ("data", "rows", "items", "records"):
            value = payload.get(key)
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                return pd.DataFrame.from_records(value)
        # Recursively search for a table definition
        for value in payload.values():
            if isinstance(value, (Mapping, list, tuple)):
                df = _json_payload_to_df(value)
                if not df.empty:
                    return df
    elif isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        return pd.DataFrame.from_records(payload)
    return pd.DataFrame()


def es_client() -> Elasticsearch:
    hosts = es_section.get("host", "")
    es = Elasticsearch(
        hosts=hosts.split(",") if hosts else None,
        http_auth=(es_section.get("username"), es_section.get("password")),
        timeout=30,
        max_retries=3,
        retry_on_timeout=True,
    )
    try:
        info = es.info()
        logger.info("Connected to ES: %s", info.get("cluster_name", "unknown"))
    except Exception as exc:  # pragma: no cover - connection failures shouldn't break scraping
        logger.warning("Failed to fetch ES info: %s", exc)
    return es


def ensure_index(es: Elasticsearch, index: str) -> None:
    mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "30s",
        },
        "mappings": {
            "dynamic": True,
            "dynamic_templates": [
                {"dates_iso": {"match": "*_iso", "mapping": {"type": "date", "format": "strict_date_time"}}},
                {"epochs": {"match": "*_epoch", "mapping": {"type": "long"}}},
                {"strings": {"match_mapping_type": "string", "mapping": {"type": "keyword", "ignore_above": 512}}},
            ],
            "properties": {
                "_dataset": {"type": "keyword"},
                "_source_url": {"type": "keyword"},
            },
        },
    }
    if not es.indices.exists(index=index):
        es.indices.create(index=index, body=mapping)


def _fetch_page(session: requests.Session) -> dict[str, Any]:
    response = _get_with_recaptcha_retry(
        session,
        BASE_SETTINGS.page_url,
        timeout=BASE_SETTINGS.request_timeout,
        headers=REQUEST_HEADERS,
    )
    response.raise_for_status()
    return _load_next_data(response.text)


def _ensure_date_column(df: pd.DataFrame, desired: str) -> str:
    if desired in df.columns:
        return desired
    lower_lookup = {str(col).lower(): col for col in df.columns if isinstance(col, str)}
    lowered = desired.lower()
    if lowered in lower_lookup:
        return lower_lookup[lowered]
    raise KeyError(f"Unable to locate date column '{desired}' in dataset columns: {list(df.columns)}")


def _extract_max_date_from_df(df: pd.DataFrame, date_column: str) -> date | None:
    parsed_dates = df[date_column].map(_parse_date)
    parsed_dates = parsed_dates.dropna()
    if parsed_dates.empty:
        return None
    return max(parsed_dates)


def _df_to_actions(df: pd.DataFrame, index: str) -> Iterable[dict[str, Any]]:
    clean = df.replace({np.nan: None})
    for record in clean.to_dict(orient="records"):
        doc_id = record.get("_id") or record.get("id")
        dataset = record.get("_dataset")
        if doc_id and dataset:
            es_id = f"{dataset}-{doc_id}"
        elif doc_id:
            es_id = str(doc_id)
        else:
            es_id = None
        action: dict[str, Any] = {
            "_index": index,
            "_op_type": "index",
            "_source": record,
        }
        if es_id:
            action["_id"] = es_id
        yield action


def _persist_artifacts(cfg: DatasetConfig, df: pd.DataFrame) -> Path:
    out_dir = Path("saved_data/dld_open_data") / cfg.key
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    csv_path = out_dir / f"{cfg.key}_{timestamp}.csv"
    df.to_csv(csv_path, index=False)
    return csv_path


def _prepare_dataframe(
    df: pd.DataFrame,
    cfg: DatasetConfig,
    source_url: str,
) -> pd.DataFrame:
    if df.empty:
        return df
    date_column = _ensure_date_column(df, cfg.date_field)
    df["_dataset"] = cfg.key
    df["_source_url"] = source_url
    df["_extracted_at_iso"] = datetime.now(timezone.utc).isoformat()
    # ensure date column convertible to string for CSV/ES
    df[date_column] = df[date_column].apply(lambda v: v.isoformat() if isinstance(v, date) else v)
    return df


def _fetch_dataset(
    session: requests.Session,
    next_data: Mapping[str, Any],
    cfg: DatasetConfig,
    start_date: date,
    end_date: date,
) -> tuple[pd.DataFrame, str]:
    node = _find_dataset_node(next_data, cfg)
    if node is None:
        raise KeyError(f"Unable to locate dataset '{cfg.label}' in page data")
    table_node = _extract_table_node(node)
    if table_node is not None:
        df = _table_to_dataframe(table_node)
        source_url = BASE_SETTINGS.page_url
    else:
        data_url = _extract_data_url(node)
        if not data_url:
            raise KeyError(f"Dataset '{cfg.label}' lacks downloadable data.")
        prepared_url = _prepare_dataset_url(data_url, start_date, end_date)
        df, source_url = _download_dataset(session, prepared_url)
    if df.empty:
        return df, BASE_SETTINGS.page_url
    df = _prepare_dataframe(df, cfg, source_url)
    return df, source_url


@task
async def dld_open_data_pipeline() -> None:
    if not DATASET_CONFIGS:
        raise RuntimeError("No DLD Open Data datasets configured. Please populate config.ini [dld_open_data].")

    state = _load_state()
    session = requests.Session()
    next_data = _fetch_page(session)
    es = es_client()

    today = datetime.now(timezone.utc).date()

    for cfg in DATASET_CONFIGS:
        ensure_index(es, cfg.es_index)
        start = _compute_start_date(cfg, state)
        logger.info("Fetching DLD %s data starting %s", cfg.label, start)
        try:
            df, source_url = _fetch_dataset(session, next_data, cfg, start, today)
        except RecaptchaBlockedError:
            raise
        except Exception as exc:
            logger.exception("Failed to fetch dataset %s: %s", cfg.label, exc)
            continue

        if df.empty:
            logger.info("No records returned for %s", cfg.label)
            continue

        date_column = _ensure_date_column(df, cfg.date_field)
        max_dt = _extract_max_date_from_df(df, date_column)
        if max_dt:
            state.setdefault(cfg.key, {})["max_date"] = max_dt.isoformat()

        csv_path = _persist_artifacts(cfg, df)
        logger.info("Saved %s records for %s to %s", len(df), cfg.label, csv_path)

        logger.info("Indexing %s records into %s", len(df), cfg.es_index)
        bulk_resp = helpers.bulk(
            es,
            _df_to_actions(df, cfg.es_index),
            chunk_size=500,
            request_timeout=120,
            raise_on_error=False,
            raise_on_exception=False,
        )
        logger.info("ES bulk response for %s (source %s): %s", cfg.label, source_url, bulk_resp)

    _save_state(state)


class InputParams(BaseModel):
    """Placeholder for pipeline parameters (none required)."""


register_pipeline(
    id="dld_open_data_pipeline",
    description="Ingest Dubai Land Department open datasets into Elasticsearch via the public web portal.",
    tasks=[dld_open_data_pipeline],
    triggers=[
        Trigger(
            id="dld_open_daily",
            name="DLD Open Data Daily",
            description="Pull DLD open data every morning.",
            params=InputParams(),
            schedule=CronTrigger(hour="6", minute="0", timezone="Asia/Dubai"),
        )
    ],
    params=InputParams,
)

