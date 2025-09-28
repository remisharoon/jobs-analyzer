"""Crswtch scraper & pipeline.

This module mirrors the existing Dubizzle pipeline but targets Crswtch listings.
It parses the streaming HTML payload that Next.js emits (`self.__next_f.push` chunks),
extracts listing summaries, enriches each record with detail-page structured data,
and finally writes the normalized payload into Elasticsearch.
"""

from __future__ import annotations

import asyncio
import json
import random
import re
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import requests
from apscheduler.triggers.cron import CronTrigger
from elasticsearch import Elasticsearch, helpers
import boto3
import os
from pydantic import BaseModel
from plombery import Trigger, get_logger, register_pipeline, task
import logging
from config import read_config


_STREAM_CHUNK_RE = re.compile(r'self.__next_f.push\(\[1,"(.*?)"\]\)', re.S)
_ITEM_LIST_PREFIX = '{"@context":"https://schema.org","@type":"ItemList"'
_CAR_ENTITY_PREFIX = '{"@context":"https://schema.org","@type":["Car"'

# logger = get_logger(__name__)
logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CrswtchSettings:
    listing_url_template: str
    pages: int
    min_delay_seconds: float
    max_delay_seconds: float
    detail_retry_count: int
    es_index: str



config = read_config()

CONFIG_SECTION = 'crstch'
crstch_section = config[CONFIG_SECTION]

if not crstch_section:
    raise KeyError("Missing [crstch] section in config.ini")

listing_url_template = crstch_section['listing_url']
pages = int(crstch_section.get('pages', '1'))
min_delay = float(crstch_section.get('min_delay_seconds', '1.5'))
max_delay = float(crstch_section.get('max_delay_seconds', '4.0'))
detail_retry_count = int(crstch_section.get('detail_retry_count', '3'))

_es_config = config['elasticsearch']
es_index = crstch_section.get('es_index') or _es_config.get('carswitch_index', 'carswitch_cars')

SETTINGS = CrswtchSettings(
    listing_url_template=listing_url_template,
    pages=pages,
    min_delay_seconds=min_delay,
    max_delay_seconds=max_delay,
    detail_retry_count=detail_retry_count,
    es_index=es_index,
)


es_hosts = _es_config['host'].split(',')
es_user = _es_config['username']
es_password = _es_config['password']


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

REQUEST_HEADERS = {
    'User-Agent': USER_AGENT,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Connection': 'keep-alive',
}


# ----------------------------------------------------------------------------
# Parsing helpers
# ----------------------------------------------------------------------------

def _iter_decoded_chunks(html_text: str) -> Iterable[str]:
    """Yield decoded Next.js stream chunks from the Crswtch HTML."""
    for match in _STREAM_CHUNK_RE.finditer(html_text):
        raw = match.group(1)
        try:
            decoded = json.loads(f'"{raw}"')
        except json.JSONDecodeError:
            continue
        if decoded:
            yield decoded


def _extract_balanced_json(payload: str) -> str | None:
    """Return the first balanced JSON object substring from *payload*."""
    depth = 0
    end_idx = None
    for idx, ch in enumerate(payload):
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                end_idx = idx + 1
                break
    if end_idx is None:
        return None
    return payload[:end_idx]


def _parse_listing_summary(html_text: str) -> dict[str, dict[str, Any]]:
    """Extract the summary cards (price, mileage, etc.) from listing page HTML."""
    records: dict[str, dict[str, Any]] = {}
    for chunk in _iter_decoded_chunks(html_text):
        for line in chunk.split('\n'):
            if ':' not in line:
                continue
            key, remainder = line.split(':', 1)
            if not key or not key.startswith('c'):
                continue
            remainder = remainder.strip().rstrip(',')
            if not remainder.startswith('{'):
                continue
            candidate = _extract_balanced_json(remainder)
            if not candidate:
                continue
            try:
                data = json.loads(candidate)
            except json.JSONDecodeError:
                continue
            listing_id = str(data.get('id')) if data.get('id') is not None else None
            if not listing_id:
                continue
            data['id'] = listing_id
            records[listing_id] = data
    return records


def _extract_json_objects(chunk: str, prefix: str) -> Iterable[dict[str, Any]]:
    """Yield JSON objects starting with *prefix* from a decoded chunk."""
    idx = chunk.find(prefix)
    while idx != -1:
        segment = chunk[idx:]
        candidate = _extract_balanced_json(segment)
        if not candidate:
            break
        try:
            obj = json.loads(candidate)
        except json.JSONDecodeError:
            pass
        else:
            yield obj
        idx = chunk.find(prefix, idx + len(prefix))


def _normalize_brand(raw_brand: Any) -> tuple[list[str], Any]:
    if isinstance(raw_brand, dict):
        return [raw_brand.get('name')] if raw_brand.get('name') else [], raw_brand
    if isinstance(raw_brand, list):
        names = [item.get('name') for item in raw_brand if isinstance(item, dict) and item.get('name')]
        return names, raw_brand
    if isinstance(raw_brand, str):
        return [raw_brand], raw_brand
    return [], raw_brand


def _maybe_int(value: Any) -> Any:
    if value is None:
        return None
    try:
        return int(str(value).replace(',', '').strip())
    except (ValueError, TypeError):
        return value


def _flatten_car_entity(entity: dict[str, Any]) -> dict[str, Any]:
    """Flatten schema.org Car/Product entity into scalar fields."""
    if not isinstance(entity, dict):
        return {}

    flattened: dict[str, Any] = {}

    def put(key: str, value: Any) -> None:
        if value is None:
            return
        flattened[key] = value

    entity_types = entity.get('@type')
    if entity_types:
        if isinstance(entity_types, list):
            put('detail_entity_types', entity_types)
        else:
            put('detail_entity_types', [entity_types])

    put('detail_name', entity.get('name'))
    put('detail_vehicle_identification_number', entity.get('vehicleIdentificationNumber'))
    images = entity.get('image')
    if isinstance(images, list):
        put('detail_images', images)
    elif isinstance(images, str):
        put('detail_images', [images])

    put('detail_item_url', entity.get('url'))
    put('detail_description', entity.get('description'))
    put('detail_item_condition', entity.get('itemCondition'))

    brand_names, brand_raw = _normalize_brand(entity.get('brand'))
    if brand_names:
        put('detail_brand_names', brand_names)
    if brand_raw:
        put('detail_brand_raw', brand_raw)

    put('detail_model', entity.get('model'))
    put('detail_vehicle_configuration', entity.get('vehicleConfiguration'))
    put('detail_vehicle_model_date', entity.get('vehicleModelDate'))
    put('detail_vehicle_transmission', entity.get('vehicleTransmission'))
    put('detail_vehicle_seating_capacity', _maybe_int(entity.get('vehicleSeatingCapacity')))
    put('detail_color', entity.get('color'))
    put('detail_body_type', entity.get('bodyType'))
    put('detail_drive_wheel_configuration', entity.get('driveWheelConfiguration'))

    mileage = entity.get('mileageFromOdometer')
    if isinstance(mileage, dict):
        put('detail_mileage_value', _maybe_int(mileage.get('value')))
        put('detail_mileage_unit', mileage.get('unitCode'))
        put('detail_mileage_raw', mileage)

    engine = entity.get('vehicleEngine')
    if isinstance(engine, dict):
        put('detail_engine_fuel_type', engine.get('fuelType'))
        put('detail_engine_displacement', engine.get('engineDisplacement'))
        put('detail_engine_raw', engine)

    offers = entity.get('offers')
    if isinstance(offers, list) and offers:
        offer = offers[0]
    elif isinstance(offers, dict):
        offer = offers
    else:
        offer = None
    if isinstance(offer, dict):
        put('detail_offer_price', _maybe_int(offer.get('price')))
        put('detail_offer_currency', offer.get('priceCurrency'))
        put('detail_offer_availability', offer.get('availability'))
        put('detail_offer_raw', offer)

    main_entity_of_page = entity.get('mainEntityOfPage')
    if isinstance(main_entity_of_page, dict):
        put('detail_main_entity_of_page', main_entity_of_page)

    potential_action = entity.get('potentialAction')
    if isinstance(potential_action, dict):
        put('detail_potential_action', potential_action)

    return flattened


def _parse_item_pages(html_text: str) -> dict[str, dict[str, Any]]:
    """Extract schema.org ItemPage data from listing HTML and key by listing ID."""
    results: dict[str, dict[str, Any]] = {}
    for chunk in _iter_decoded_chunks(html_text):
        for obj in _extract_json_objects(chunk, _ITEM_LIST_PREFIX):
            if obj.get('@type') != 'ItemList':
                continue
            elements = obj.get('itemListElement') or []
            for el in elements:
                if not isinstance(el, dict):
                    continue
                url = el.get('url')
                if not url:
                    continue
                listing_id = url.rstrip('/').split('/')[-1]
                payload: dict[str, Any] = {}
                payload['detail_url'] = url
                position = el.get('position')
                if position is not None:
                    payload['detail_position'] = _maybe_int(position)
                main = el.get('mainEntity')
                payload.update(_flatten_car_entity(main or {}))
                if listing_id in results:
                    results[listing_id].update(payload)
                else:
                    results[listing_id] = payload
    return results


def parse_crswtch_listing_page(html_text: str) -> pd.DataFrame:
    """Parse the Crswtch listing page into a DataFrame of summary records."""
    summaries = _parse_listing_summary(html_text)
    item_meta = _parse_item_pages(html_text)

    rows: list[dict[str, Any]] = []
    for listing_id, summary in summaries.items():
        row = dict(summary)
        detail_meta = item_meta.get(listing_id, {})
        for key, value in detail_meta.items():
            if key not in row or row.get(key) in (None, '', [], {}):
                row[key] = value
            else:
                row[f'{key}_meta'] = value
        row.setdefault('detail_url', detail_meta.get('detail_url'))
        rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        df['id'] = df['id'].astype(str)
    return df


def parse_crswtch_detail_page(html_text: str) -> dict[str, Any]:
    """Parse detail page structured data (schema.org Car)."""
    for chunk in _iter_decoded_chunks(html_text):
        for obj in _extract_json_objects(chunk, _CAR_ENTITY_PREFIX):
            return _flatten_car_entity(obj)
    return {}


# ----------------------------------------------------------------------------
# Elasticsearch helpers
# ----------------------------------------------------------------------------

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
        logger.info("Connected to ES: %s", info.get('cluster_name', 'unknown-cluster'))
    except Exception as exc:  # pragma: no cover - connectivity failure is logged but not fatal
        logger.warning("Failed to fetch ES info: %s", exc)
    return es


def ensure_index(es: Elasticsearch, index: str) -> None:
    mapping = {
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0,
            'refresh_interval': '5s',
        },
        'mappings': {
            'dynamic': True,
            'dynamic_templates': [
                {'dates_iso': {'match': '*_iso', 'mapping': {'type': 'date', 'format': 'strict_date_time'}}},
                {'epochs': {'match': '*_epoch', 'mapping': {'type': 'long'}}},
                {'strings': {'match_mapping_type': 'string', 'mapping': {'type': 'keyword', 'ignore_above': 256}}},
                {'doubleNums': {'match_mapping_type': 'double', 'mapping': {'type': 'double'}}},
                {'longNums': {'match_mapping_type': 'long', 'mapping': {'type': 'long'}}},
            ],
            'properties': {
                'id': {'type': 'keyword'},
                'price': {'type': 'integer'},
                'detail_offer_price': {'type': 'integer'},
                'detail_mileage_value': {'type': 'integer'},
                'detail_url': {'type': 'keyword'},
                'detail_name': {'type': 'text', 'fields': {'kw': {'type': 'keyword', 'ignore_above': 256}}},
                'source_page': {'type': 'integer'},
                'source': {'type': 'keyword'},
            },
        },
    }
    if not es.indices.exists(index=index):
        es.indices.create(index=index, body=mapping)


def df_to_actions(df: pd.DataFrame, index: str) -> Iterable[dict[str, Any]]:
    clean = df.replace({np.nan: None})
    for record in clean.to_dict(orient='records'):
        doc_id = record.get('id')
        yield {
            '_index': index,
            '_id': doc_id,
            '_op_type': 'index',
            '_source': record,
        }


# ----------------------------------------------------------------------------
# Scraper task
# ----------------------------------------------------------------------------

def _absolute_url(url: str | None) -> str | None:
    if not url:
        return None
    if url.startswith('http'):
        return url
    return f'https://carswitch.com{url}'


def _fetch(session: requests.Session, url: str, *, retries: int = 1, timeout: int = 20) -> str:
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            response = session.get(url, headers=REQUEST_HEADERS, timeout=timeout)
            response.raise_for_status()
            return response.text
        except Exception as exc:  # pragma: no cover - networking best-effort
            last_exc = exc
            sleep_for = SETTINGS.min_delay_seconds * (attempt + 1)
            logger.warning("Request failed (%s). Retrying after %.1fs", exc, sleep_for)
            time.sleep(sleep_for)
    raise RuntimeError(f"Failed to fetch {url}: {last_exc}")


async def _fetch_detail_with_retry(session: requests.Session, url: str) -> dict[str, Any]:
    for attempt in range(SETTINGS.detail_retry_count):
        try:
            html_text = _fetch(session, url, retries=1)
            detail = parse_crswtch_detail_page(html_text)
            if detail:
                return detail
        except Exception as exc:  # pragma: no cover - network failures logged
            logger.warning("Detail fetch failed for %s (attempt %s/%s): %s", url, attempt + 1, SETTINGS.detail_retry_count, exc)
        await asyncio.sleep(random.uniform(SETTINGS.min_delay_seconds, SETTINGS.max_delay_seconds))
    return {}


@task
async def crswtch_car_data() -> None:
    session = requests.Session()
    es = es_client()
    ensure_index(es, SETTINGS.es_index)

    all_rows: list[dict[str, Any]] = []

    for page in range(1, SETTINGS.pages + 1):
        url = SETTINGS.listing_url_template.format(page=page)
        logger.info("Fetching Crswtch listing page %s", url)
        listing_html = _fetch(session, url, retries=2)
        df = parse_crswtch_listing_page(listing_html)
        if df.empty:
            raise ValueError(f"No listings parsed from {url}")
        logger.info("Parsed %d Crswtch listings from page %d", len(df), page)

        for record in df.to_dict(orient='records'):
            record['source_page'] = page
            detail_url = _absolute_url(record.get('detail_url'))
            if detail_url:
                detail_payload = await _fetch_detail_with_retry(session, detail_url)
            else:
                detail_payload = {}
            for key, value in detail_payload.items():
                # Detail data overrides summary when richer information exists
                if value is not None:
                    record[key] = value
            record['detail_url'] = detail_url
            record['source'] = 'crswtch'
            all_rows.append(record)
            await asyncio.sleep(random.uniform(SETTINGS.min_delay_seconds, SETTINGS.max_delay_seconds))

        out_dir = Path('saved_data/crswtch')
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / f'listings_page_{page}.csv'
        df.to_csv(csv_path, index=False)
        logger.info("Saved raw listings to %s", csv_path)

    final_df = pd.DataFrame(all_rows)
    if final_df.empty:
        raise ValueError("No Crswtch records collected")

    final_df = final_df.drop_duplicates(subset=['id'], keep='last').reset_index(drop=True)

    logger.info("Indexing %d Crswtch documents into ES index %s", len(final_df), SETTINGS.es_index)
    bulk_resp = helpers.bulk(
        es,
        df_to_actions(final_df, SETTINGS.es_index),
        chunk_size=500,
        request_timeout=120,
        raise_on_error=False,
        raise_on_exception=False,
    )
    logger.info("ES bulk response: %s", bulk_resp)


class InputParams(BaseModel):  # pragma: no cover - required by Register API but no runtime behaviour
    """Placeholder for pipeline parameters (none required)."""


# ----------------------------------------------------------------------------
# Export to Cloudflare R2
# ----------------------------------------------------------------------------

def fetch_all_docs(es: Elasticsearch, index: str, fields: list[str]):
    query = {"query": {"match_all": {}}, "_source": fields}
    for doc in helpers.scan(es, index=index, query=query, preserve_order=False, size=1000):
        yield doc.get("_source", {})


@task
async def export_crswtch_json_to_r2():
    cloudflare_config = config['cloudflare']

    FIELDS = [
        "id",
        "price",
        "detail_offer_price",
        "detail_name",
        "detail_brand_names",
        "detail_model",
        "detail_vehicle_model_date",
        "detail_vehicle_transmission",
        "detail_body_type",
        "detail_drive_wheel_configuration",
        "detail_mileage_value",
        "detail_mileage_unit",
        "detail_color",
        "detail_url",
        "detail_item_url",
        "source_page",
        "source",
    ]

    es = es_client()
    if not es.indices.exists(index=SETTINGS.es_index):
        raise SystemExit(f"Index not found: {SETTINGS.es_index}")

    rows = list(fetch_all_docs(es, SETTINGS.es_index, FIELDS))
    if not rows:
        raise SystemExit("No documents returned from ES.")

    df = pd.DataFrame.from_records(rows)
    present_cols = [c for c in FIELDS if c in df.columns]
    df = df[present_cols].copy()

    out_path = Path("crswth_listings.json")
    df.to_json(out_path, orient="records", force_ascii=False)

    ACCOUNT_ID = cloudflare_config["ACCOUNT_ID"]
    R2_ENDPOINT = cloudflare_config["R2_ENDPOINT"]
    BUCKET = cloudflare_config["BUCKET"]
    ACCESS_KEY_ID = cloudflare_config["ACCESS_KEY_ID"]
    SECRET_ACCESS_KEY = cloudflare_config["SECRET_ACCESS_KEY"]

    session = boto3.session.Session()
    s3 = session.client(
        service_name="s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY,
    )

    key = "data/crswth_listings.json"
    s3.upload_file(
        Filename=str(out_path),
        Bucket=BUCKET,
        Key=key,
        ExtraArgs={
            "ContentType": "application/json",
            "ACL": "public-read",
            "CacheControl": "public, max-age=60",
        },
    )

    logger.info("Uploaded to r2://%s/%s", BUCKET, key)


register_pipeline(
    id='crswtch_pipeline',
    description='Scrape Crswtch listings and index enriched data into Elasticsearch.',
    tasks=[crswtch_car_data, export_crswtch_json_to_r2],
    triggers=[
        Trigger(
            id='crswtch_daily',
            name='Crswtch Daily',
            description='Run Crswtch scraper daily at 06:00 Dubai time',
            params=InputParams(),
            schedule=CronTrigger(hour='5', minute='0', timezone='Asia/Dubai'),
        )
    ],
    params=InputParams,
)
