"""DLD Open Data scraper and pipeline.

This module harvests structured datasets exposed through the Dubai Land
Department's open-data CKAN instance.  Similar to the Allsopp scraper, it
fetches incremental updates, normalises the payload, and indexes them into
Elasticsearch before exporting local artefacts.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

import numpy as np
import pandas as pd
import requests
from apscheduler.triggers.cron import CronTrigger
from elasticsearch import Elasticsearch, helpers
from pydantic import BaseModel

from plombery import Trigger, register_pipeline, task
from config import read_config


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


@dataclass(slots=True)
class DLDBaseSettings:
    api_base_url: str
    page_size: int
    lookback_days: int
    buffer_days: int
    request_timeout: int


@dataclass(slots=True)
class DatasetConfig:
    key: str
    label: str
    resource_id: str
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
    api_base_url=dld_section.get("base_url", "https://opendata.dubailand.gov.ae/api/3/action"),
    page_size=_get_int(dld_section, "page_size", 500),
    lookback_days=_get_int(dld_section, "lookback_days", 30),
    buffer_days=_get_int(dld_section, "buffer_days", 3),
    request_timeout=_get_int(dld_section, "timeout_seconds", 30),
)


def _build_dataset_config(key: str, label: str, section: Mapping[str, str]) -> DatasetConfig | None:
    resource_id = section.get(f"{key}_resource_id")
    date_field = section.get(f"{key}_date_field")
    es_index = section.get(f"{key}_es_index") or es_section.get("dld_index")
    if not resource_id or not date_field or not es_index:
        logger.debug("Skipping dataset %s: missing resource_id/date_field/index", key)
        return None
    buffer_days: int | None = None
    raw_buffer = section.get(f"{key}_buffer_days")
    if raw_buffer:
        try:
            buffer_days = int(raw_buffer)
        except ValueError:
            logger.warning("Invalid %s_buffer_days value '%s'; falling back to defaults.", key, raw_buffer)
    return DatasetConfig(
        key=key,
        label=label,
        resource_id=resource_id,
        date_field=date_field,
        es_index=es_index,
        buffer_days=buffer_days,
    )


DATASET_CONFIGS: list[DatasetConfig] = [
    cfg for key, label in DATASET_KEYS.items() if (cfg := _build_dataset_config(key, label, dld_section))
]


STATE_PATH = Path("saved_data/dld_open_data/state.json")
STATE_PATH.parent.mkdir(parents=True, exist_ok=True)


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
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
            try:
                return datetime.strptime(value[: len(fmt)], fmt).date()
            except ValueError:
                continue
    return None


def _compute_start_date(cfg: DatasetConfig, state: dict[str, Any]) -> date:
    buffer_days = cfg.buffer_days or BASE_SETTINGS.buffer_days
    dataset_state = state.get(cfg.key, {})
    prev_max = dataset_state.get("max_date")
    today = datetime.utcnow().date()
    if prev_max:
        prev_date = _parse_date(prev_max) or (today - timedelta(days=BASE_SETTINGS.lookback_days))
        start = prev_date - timedelta(days=buffer_days)
    else:
        start = today - timedelta(days=BASE_SETTINGS.lookback_days)
    if start > today:
        start = today
    return start


class RecaptchaBlockedError(RuntimeError):
    """Raised when the API responds with reCAPTCHA / anti-bot HTML."""


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
    except Exception as exc:  # pragma: no cover
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


def _build_sql(cfg: DatasetConfig, start_date: date, offset: int, limit: int) -> str:
    start_str = start_date.strftime("%Y-%m-%d")
    resource = cfg.resource_id.replace('"', '""')
    date_field = cfg.date_field.replace('"', '""')
    return (
        f'SELECT * FROM "{resource}" '
        f'WHERE "{date_field}" >= \'{start_str}\' '
        f'ORDER BY "{date_field}" ASC '
        f'LIMIT {limit} OFFSET {offset}'
    )


RequestCallable = Callable[[Mapping[str, str]], requests.Response]


def _request(
    session: requests.Session,
    url: str,
    params: Mapping[str, str],
) -> requests.Response:
    response = session.get(
        url,
        params=params,
        timeout=BASE_SETTINGS.request_timeout,
        headers={"User-Agent": "Mozilla/5.0 (compatible; JobsAnalyzerBot/1.0)"},
    )
    if "text/html" in response.headers.get("Content-Type", ""):
        text = response.text.lower()
        if "i'm not a robot" in text or "recaptcha" in text:
            raise RecaptchaBlockedError("Blocked by reCAPTCHA. Manual intervention required.")
    response.raise_for_status()
    return response


def _fetch_dataset_records(
    session: requests.Session,
    cfg: DatasetConfig,
    start_date: date,
) -> list[dict[str, Any]]:
    sql_endpoint = f"{BASE_SETTINGS.api_base_url.rstrip('/')}/datastore_search_sql"
    records: list[dict[str, Any]] = []
    offset = 0
    while True:
        sql = _build_sql(cfg, start_date, offset, BASE_SETTINGS.page_size)
        logger.debug("Fetching %s batch offset=%s", cfg.key, offset)
        resp = _request(session, sql_endpoint, {"sql": sql})
        payload = resp.json()
        if not payload.get("success"):
            raise RuntimeError(f"CKAN API returned error for dataset {cfg.key}: {payload}")
        batch = payload.get("result", {}).get("records", [])
        if not batch:
            break
        records.extend(batch)
        if len(batch) < BASE_SETTINGS.page_size:
            break
        offset += BASE_SETTINGS.page_size
    return records


def _extract_max_date(records: Iterable[dict[str, Any]], date_field: str) -> date | None:
    max_dt: date | None = None
    for record in records:
        value = record.get(date_field) or record.get(date_field.lower())
        parsed = _parse_date(value)
        if parsed and (max_dt is None or parsed > max_dt):
            max_dt = parsed
    return max_dt


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
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    csv_path = out_dir / f"{cfg.key}_{timestamp}.csv"
    df.to_csv(csv_path, index=False)
    return csv_path


@task
async def dld_open_data_pipeline() -> None:
    if not DATASET_CONFIGS:
        raise RuntimeError("No DLD Open Data datasets configured. Please populate config.ini [dld_open_data].")

    state = _load_state()
    session = requests.Session()
    es = es_client()

    for cfg in DATASET_CONFIGS:
        ensure_index(es, cfg.es_index)
        start = _compute_start_date(cfg, state)
        logger.info("Fetching DLD %s data starting %s", cfg.label, start)
        records = _fetch_dataset_records(session, cfg, start)
        if not records:
            logger.info("No new records for %s", cfg.label)
            continue

        for record in records:
            record["_dataset"] = cfg.key
            record["_source_url"] = cfg.resource_id
        df = pd.DataFrame.from_records(records)
        if df.empty:
            logger.info("Dataset %s returned empty frame", cfg.label)
            continue

        max_dt = _extract_max_date(records, cfg.date_field)
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
        logger.info("ES bulk response for %s: %s", cfg.label, bulk_resp)

    _save_state(state)


class InputParams(BaseModel):
    """Placeholder for pipeline parameters (none required)."""


register_pipeline(
    id="dld_open_data_pipeline",
    description="Ingest Dubai Land Department open datasets into Elasticsearch.",
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
