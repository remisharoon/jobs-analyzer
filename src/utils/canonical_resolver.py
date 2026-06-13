from __future__ import annotations

import hashlib
import html
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

from elasticsearch import Elasticsearch

from utils.llm_client import call_llm


logger = logging.getLogger(__name__)


DEFAULT_CANONICAL_INDEX = "canonical_mappings"


CANONICAL_MAPPING_INDEX_MAPPING = {
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
                    "mapping": {
                        "type": "date",
                        "format": "strict_date_optional_time||epoch_millis",
                    },
                }
            },
            {"epochs": {"match": "*_epoch", "mapping": {"type": "long"}}},
            {
                "strings": {
                    "match_mapping_type": "string",
                    "mapping": {"type": "keyword", "ignore_above": 512},
                }
            },
            {"doubleNums": {"match_mapping_type": "double", "mapping": {"type": "double"}}},
            {"longNums": {"match_mapping_type": "long", "mapping": {"type": "long"}}},
        ],
        "properties": {
            "id": {"type": "keyword"},
            "dimension": {"type": "keyword"},
            "scope_type": {"type": "keyword"},
            "scope_key": {"type": "keyword"},
            "country_guardrail": {"type": "keyword"},
            "source_value_raw": {"type": "keyword"},
            "source_value_norm": {"type": "keyword"},
            "source_value_hash": {"type": "keyword"},
            "canonical_value": {
                "type": "text",
                "fields": {"kw": {"type": "keyword", "ignore_above": 256}},
            },
            "canonical_key": {"type": "keyword"},
            "confidence": {"type": "double"},
            "resolution_method": {"type": "keyword"},
            "status": {"type": "keyword"},
            "seen_count": {"type": "long"},
            "first_seen_at": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis",
            },
            "last_seen_at": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis",
            },
            "source_indices": {"type": "keyword"},
            "source_systems": {"type": "keyword"},
            "evidence_urls": {"type": "keyword"},
            "model": {"type": "keyword"},
            "context_snapshot": {"type": "object", "enabled": True},
        },
    },
}


_COUNTRY_SYNONYMS = {
    "uae": "UAE",
    "u.a.e": "UAE",
    "united arab emirates": "UAE",
    "india": "India",
    "republic of india": "India",
    "ksa": "Saudi Arabia",
    "saudi": "Saudi Arabia",
    "saudi arabia": "Saudi Arabia",
    "qatar": "Qatar",
    "oman": "Oman",
}

_STATE_SYNONYMS = {
    "kerala": "Kerala",
    "karnataka": "Karnataka",
    "maharashtra": "Maharashtra",
    "tamil nadu": "Tamil Nadu",
    "new delhi": "Delhi",
    "delhi": "Delhi",
    "dubai": "Dubai",
    "abu dhabi": "Abu Dhabi",
}

_CITY_SYNONYMS = {
    "bangalore": "Bengaluru",
    "bengaluru": "Bengaluru",
    "kochin": "Kochi",
    "kochi": "Kochi",
    "new delhi": "Delhi",
    "delhi": "Delhi",
    "bombay": "Mumbai",
    "mumbai": "Mumbai",
    "dubai": "Dubai",
    "abudhabi": "Abu Dhabi",
    "abu dhabi": "Abu Dhabi",
}


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = _normalize_space(value)
        return text or None
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        if not value:
            return None
        return _as_text(value[0])
    if isinstance(value, dict):
        for key in ("name", "value", "label", "text"):
            if key in value and value[key] not in (None, "", []):
                return _as_text(value[key])
    return _normalize_space(str(value)) or None


def _unique(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _as_text(value)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _slugify(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def _source_norm(value: str) -> str:
    text = html.unescape(value).lower()
    text = re.sub(r"[\u2010-\u2015]", "-", text)
    text = re.sub(r"[^a-z0-9\s&.-]", " ", text)
    text = _normalize_space(text)
    return text


def _scope_from_key(scope_key: str) -> str:
    if scope_key == "global":
        return "global"
    if scope_key.startswith("country:") and "|" not in scope_key:
        return "country"
    if "|state:" in scope_key and "|city:" not in scope_key:
        return "country_state"
    if "|builder:" in scope_key and "|city:" in scope_key:
        return "country_city_builder"
    if "|builder:" in scope_key:
        return "country_builder"
    if "|city:" in scope_key:
        return "country_city"
    return "contextual"


def _mapping_id(dimension: str, scope_key: str, source_value_norm: str) -> str:
    payload = f"{dimension}|{scope_key}|{source_value_norm}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _normalize_country_rule(value: str) -> str:
    lower = _source_norm(value)
    if lower in _COUNTRY_SYNONYMS:
        return _COUNTRY_SYNONYMS[lower]
    return _normalize_space(value).title()


def _normalize_state_rule(value: str) -> str:
    lower = _source_norm(value)
    if lower in _STATE_SYNONYMS:
        return _STATE_SYNONYMS[lower]
    return _normalize_space(value).title()


def _normalize_city_rule(value: str) -> str:
    lower = _source_norm(value).replace("-", " ")
    if lower in _CITY_SYNONYMS:
        return _CITY_SYNONYMS[lower]
    return _normalize_space(value).title()


def _normalize_builder_rule(value: str, *, context: dict[str, Any] | None = None) -> str:
    text = html.unescape(value)
    text = re.sub(r"^\d+[\).\-\s]+", "", text)
    text = re.sub(r"\s*\|.*$", "", text)
    text = re.sub(r"\s+in\s+this\s+area.*$", "", text, flags=re.I)
    text = re.sub(r"\s+in\s+thi.*$", "", text, flags=re.I)
    text = re.sub(r"\s+in\s+the\s+area.*$", "", text, flags=re.I)
    text = re.sub(r"\s*[-,]\s*(new\s+projects?|projects?|launches?).*$", "", text, flags=re.I)
    ctx = context or {}
    city = _as_text(ctx.get("city"))
    state = _as_text(ctx.get("state"))
    country = _as_text(ctx.get("country"))
    for place in (city, state, country):
        if not place:
            continue
        text = re.sub(rf"\s+in\s+{re.escape(place)}$", "", text, flags=re.I)
    text = text.strip(" -,:|")
    text = _normalize_space(text)
    if not text:
        return value.strip()
    return text


def _normalize_project_rule(value: str, *, context: dict[str, Any] | None = None) -> str:
    text = html.unescape(value)
    text = re.sub(r"\s+in\s+this\s+area.*$", "", text, flags=re.I)
    text = re.sub(r"\s+in\s+thi.*$", "", text, flags=re.I)
    text = re.sub(r"\s*\|.*$", "", text)
    text = text.strip(" -,:|")
    return _normalize_space(text)


def _rule_canonical_value(dimension: str, raw_value: str, context: dict[str, Any] | None = None) -> tuple[str, float]:
    context = context or {}
    if dimension == "country":
        return _normalize_country_rule(raw_value), 0.95
    if dimension == "state":
        return _normalize_state_rule(raw_value), 0.90
    if dimension == "city":
        return _normalize_city_rule(raw_value), 0.90
    if dimension == "builder_name":
        cleaned = _normalize_builder_rule(raw_value, context=context)
        confidence = 0.85 if cleaned.lower() != raw_value.lower() else 0.70
        return cleaned, confidence
    if dimension == "project_name":
        cleaned = _normalize_project_rule(raw_value, context=context)
        confidence = 0.80 if cleaned.lower() != raw_value.lower() else 0.65
        return cleaned, confidence
    return _normalize_space(raw_value), 0.60


def _build_scope_chain(
    dimension: str,
    context: dict[str, Any] | None = None,
    country_guardrail: str | None = None,
) -> list[str]:
    context = context or {}
    country = _as_text(context.get("country") or country_guardrail)
    state = _as_text(context.get("state"))
    city = _as_text(context.get("city"))
    builder_key = _as_text(context.get("builder_key")) or _slugify(_as_text(context.get("builder_name")) or "")

    country_key = _slugify(country)
    state_key = _slugify(state)
    city_key = _slugify(city)
    builder_key = _slugify(builder_key)

    scopes: list[str] = []
    if dimension == "builder_name":
        if country_key:
            scopes.append(f"country:{country_key}")
        scopes.append("global")
        return _unique(scopes)

    if dimension == "city":
        if country_key and state_key:
            scopes.append(f"country:{country_key}|state:{state_key}")
        if country_key:
            scopes.append(f"country:{country_key}")
        scopes.append("global")
        return _unique(scopes)

    if dimension == "project_name":
        if country_key and city_key and builder_key:
            scopes.append(f"country:{country_key}|city:{city_key}|builder:{builder_key}")
        if country_key and builder_key:
            scopes.append(f"country:{country_key}|builder:{builder_key}")
        if builder_key:
            scopes.append(f"builder:{builder_key}")
        if country_key and city_key:
            scopes.append(f"country:{country_key}|city:{city_key}")
        if country_key:
            scopes.append(f"country:{country_key}")
        scopes.append("global")
        return _unique(scopes)

    if dimension == "state":
        if country_key:
            scopes.append(f"country:{country_key}")
        scopes.append("global")
        return _unique(scopes)

    if dimension == "country":
        return ["global"]

    if country_key:
        scopes.append(f"country:{country_key}")
    scopes.append("global")
    return _unique(scopes)


class CanonicalResolver:
    def __init__(
        self,
        es: Elasticsearch,
        *,
        index: str = DEFAULT_CANONICAL_INDEX,
        max_ai_calls: int = 100,
        ai_enabled: bool = True,
    ) -> None:
        self.es = es
        self.index = (index or DEFAULT_CANONICAL_INDEX).strip()
        self.max_ai_calls = max(0, int(max_ai_calls))
        self.ai_enabled = bool(ai_enabled)
        self.ai_calls = 0
        self._cache: dict[str, dict[str, Any] | None] = {}
        self.active = True
        self._disabled_reason: str | None = None
        self.ensure_index()

    def _disable(self, reason: str) -> None:
        if not self.active:
            return
        self.active = False
        self._disabled_reason = reason
        logger.warning("Canonical resolver disabled: %s", reason)

    def ensure_index(self) -> None:
        if not self.active:
            return
        try:
            if not self.es.indices.exists(index=self.index):
                self.es.indices.create(index=self.index, body=CANONICAL_MAPPING_INDEX_MAPPING)
        except Exception as exc:
            self._disable(f"ensure_index failed for {self.index}: {exc}")

    def _get_mapping_by_id(self, mapping_id: str) -> dict[str, Any] | None:
        if not self.active:
            return None
        if mapping_id in self._cache:
            return self._cache[mapping_id]
        try:
            doc = self.es.get(index=self.index, id=mapping_id)
            source = doc.get("_source", {}) if isinstance(doc, dict) else {}
            if source:
                source["id"] = mapping_id
                self._cache[mapping_id] = source
                return source
        except Exception as exc:
            if "404" not in str(exc):
                self._disable(f"lookup failed for {mapping_id}: {exc}")
        self._cache[mapping_id] = None
        return None

    def _save_mapping(self, mapping: dict[str, Any]) -> None:
        if not self.active:
            return
        mapping_id = mapping.get("id")
        if not mapping_id:
            return
        try:
            self.es.index(index=self.index, id=mapping_id, body=mapping)
            self._cache[mapping_id] = mapping
        except Exception as exc:
            self._disable(f"save failed for {mapping_id}: {exc}")

    def _merge_mapping_doc(self, existing: dict[str, Any] | None, incoming: dict[str, Any]) -> dict[str, Any]:
        now = _iso_now()
        if not existing:
            out = dict(incoming)
            out["first_seen_at"] = incoming.get("first_seen_at") or now
            out["last_seen_at"] = now
            out["seen_count"] = int(incoming.get("seen_count") or 1)
            out["source_indices"] = _unique(incoming.get("source_indices") or [])
            out["source_systems"] = _unique(incoming.get("source_systems") or [])
            out["evidence_urls"] = _unique(incoming.get("evidence_urls") or [])
            return out

        out = dict(existing)
        out["last_seen_at"] = now
        out["seen_count"] = int(existing.get("seen_count") or 0) + 1
        out["source_indices"] = _unique((existing.get("source_indices") or []) + (incoming.get("source_indices") or []))
        out["source_systems"] = _unique((existing.get("source_systems") or []) + (incoming.get("source_systems") or []))
        out["evidence_urls"] = _unique((existing.get("evidence_urls") or []) + (incoming.get("evidence_urls") or []))

        existing_status = _as_text(existing.get("status")) or "active"
        existing_canonical = _as_text(existing.get("canonical_value"))
        incoming_canonical = _as_text(incoming.get("canonical_value"))

        if existing_status == "locked":
            return out

        if not existing_canonical and incoming_canonical:
            out["canonical_value"] = incoming_canonical
            out["canonical_key"] = incoming.get("canonical_key")
            out["confidence"] = incoming.get("confidence")
            out["resolution_method"] = incoming.get("resolution_method")
            out["status"] = incoming.get("status", "active")
            out["model"] = incoming.get("model")
            return out

        if existing_canonical and incoming_canonical and existing_canonical.lower() != incoming_canonical.lower():
            out["status"] = "needs_review"
            out["confidence"] = max(float(existing.get("confidence") or 0.0), float(incoming.get("confidence") or 0.0))
            return out

        return out

    def _build_mapping_doc(
        self,
        *,
        mapping_id: str,
        dimension: str,
        scope_key: str,
        source_value_raw: str,
        source_value_norm: str,
        canonical_value: str,
        canonical_key: str,
        confidence: float,
        resolution_method: str,
        status: str,
        country_guardrail: str | None,
        source_index: str | None,
        source_system: str | None,
        evidence_url: str | None,
        context: dict[str, Any] | None,
        model: str | None,
    ) -> dict[str, Any]:
        now = _iso_now()
        context_snapshot: dict[str, Any] = {}
        for key, value in (context or {}).items():
            text = _as_text(value)
            if text:
                context_snapshot[key] = text
        return {
            "id": mapping_id,
            "dimension": dimension,
            "scope_type": _scope_from_key(scope_key),
            "scope_key": scope_key,
            "country_guardrail": country_guardrail,
            "source_value_raw": source_value_raw,
            "source_value_norm": source_value_norm,
            "source_value_hash": hashlib.sha1(source_value_norm.encode("utf-8")).hexdigest(),
            "canonical_value": canonical_value,
            "canonical_key": canonical_key,
            "confidence": float(max(0.0, min(1.0, confidence))),
            "resolution_method": resolution_method,
            "status": status,
            "first_seen_at": now,
            "last_seen_at": now,
            "seen_count": 1,
            "source_indices": _unique([source_index] if source_index else []),
            "source_systems": _unique([source_system] if source_system else []),
            "evidence_urls": _unique([evidence_url] if evidence_url else []),
            "model": model,
            "context_snapshot": context_snapshot,
        }

    def _touch_mapping(
        self,
        existing: dict[str, Any],
        *,
        source_index: str | None,
        source_system: str | None,
        evidence_url: str | None,
    ) -> dict[str, Any]:
        mapping_id = existing.get("id")
        if not mapping_id:
            return existing
        incoming = {
            "id": mapping_id,
            "source_indices": [source_index] if source_index else [],
            "source_systems": [source_system] if source_system else [],
            "evidence_urls": [evidence_url] if evidence_url else [],
        }
        merged = self._merge_mapping_doc(existing, incoming)
        self._save_mapping(merged)
        return merged

    def _ai_canonicalize(
        self,
        *,
        dimension: str,
        source_value_raw: str,
        context: dict[str, Any] | None,
        country_guardrail: str | None,
    ) -> tuple[str | None, float, str | None]:
        if not self.ai_enabled or self.ai_calls >= self.max_ai_calls:
            return None, 0.0, None

        prompt_context = {
            key: _as_text(value)
            for key, value in (context or {}).items()
            if _as_text(value)
        }

        system_prompt = (
            "You normalize entity names to canonical values. "
            "Return JSON only. No markdown."
        )
        user_prompt = (
            "Canonicalize the source value.\n"
            "Return a JSON object exactly with keys: "
            "canonical_value (string), confidence (number 0..1).\n"
            f"dimension: {dimension}\n"
            f"source_value: {source_value_raw}\n"
            f"country_guardrail: {country_guardrail or ''}\n"
            f"context: {json.dumps(prompt_context, ensure_ascii=True)}\n"
            "Rules:\n"
            "- Preserve original language and entity identity.\n"
            "- Remove obvious noise tokens like trailing 'in this area'.\n"
            "- For city/country/state use standard modern names.\n"
            "- For builder_name avoid cross-country merges unless clearly same brand.\n"
        )

        try:
            self.ai_calls += 1
            raw = call_llm(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                json_mode=True,
                temperature=0.0,
                max_tokens=350,
            )
            parsed = json.loads(raw)
            canonical_value = _as_text(parsed.get("canonical_value"))
            confidence = float(parsed.get("confidence") or 0.0)
            if canonical_value:
                return canonical_value, max(0.0, min(1.0, confidence)), "google/gemini-2.0-flash-001"
        except Exception as exc:
            logger.debug("AI canonicalization failed for %s='%s': %s", dimension, source_value_raw, exc)
        return None, 0.0, None

    def resolve(
        self,
        dimension: str,
        value: Any,
        *,
        context: dict[str, Any] | None = None,
        country_guardrail: str | None = None,
        source_index: str | None = None,
        source_system: str | None = None,
        evidence_url: str | None = None,
        allow_ai: bool = True,
    ) -> dict[str, Any]:
        raw = _as_text(value)
        if not raw:
            return {
                "canonical_value": None,
                "canonical_key": None,
                "found": False,
                "resolution_method": "empty",
                "confidence": 0.0,
                "mapping_id": None,
            }

        if not self.active:
            canonical_value, confidence = _rule_canonical_value(dimension, raw, context=context)
            canonical_value = canonical_value or raw
            return {
                "canonical_value": canonical_value,
                "canonical_key": _slugify(canonical_value),
                "found": False,
                "resolution_method": "rule",
                "confidence": confidence,
                "mapping_id": None,
                "scope_key": "global",
            }

        scope_chain = _build_scope_chain(
            dimension=dimension,
            context=context,
            country_guardrail=country_guardrail,
        )
        source_value_norm = _source_norm(raw)

        for scope_key in scope_chain:
            mapping_id = _mapping_id(dimension, scope_key, source_value_norm)
            mapping = self._get_mapping_by_id(mapping_id)
            if not mapping:
                continue
            if (mapping.get("status") or "active") not in {"active", "locked", "needs_review"}:
                continue

            mapping = self._touch_mapping(
                mapping,
                source_index=source_index,
                source_system=source_system,
                evidence_url=evidence_url,
            )
            canonical_value = _as_text(mapping.get("canonical_value")) or raw
            return {
                "canonical_value": canonical_value,
                "canonical_key": _as_text(mapping.get("canonical_key")) or _slugify(canonical_value),
                "found": True,
                "resolution_method": _as_text(mapping.get("resolution_method")) or "mapping",
                "confidence": float(mapping.get("confidence") or 0.0),
                "mapping_id": mapping_id,
                "scope_key": scope_key,
            }

        rule_value, rule_confidence = _rule_canonical_value(dimension, raw, context=context)
        canonical_value = rule_value or raw
        confidence = rule_confidence
        resolution_method = "rule"
        model = None

        if allow_ai and self.ai_enabled and self.ai_calls < self.max_ai_calls:
            ai_value, ai_confidence, model = self._ai_canonicalize(
                dimension=dimension,
                source_value_raw=raw,
                context=context,
                country_guardrail=country_guardrail,
            )
            if ai_value:
                canonical_value = _rule_canonical_value(dimension, ai_value, context=context)[0]
                confidence = max(confidence, ai_confidence)
                resolution_method = "ai"

        canonical_key = _slugify(canonical_value) or _slugify(raw)
        scope_key = scope_chain[0] if scope_chain else "global"
        mapping_id = _mapping_id(dimension, scope_key, source_value_norm)
        status = "active" if confidence >= 0.55 else "needs_review"

        incoming = self._build_mapping_doc(
            mapping_id=mapping_id,
            dimension=dimension,
            scope_key=scope_key,
            source_value_raw=raw,
            source_value_norm=source_value_norm,
            canonical_value=canonical_value,
            canonical_key=canonical_key,
            confidence=confidence,
            resolution_method=resolution_method,
            status=status,
            country_guardrail=country_guardrail,
            source_index=source_index,
            source_system=source_system,
            evidence_url=evidence_url,
            context=context,
            model=model,
        )
        existing = self._get_mapping_by_id(mapping_id)
        merged = self._merge_mapping_doc(existing, incoming)
        self._save_mapping(merged)

        if dimension == "builder_name" and scope_key != "global" and confidence >= 0.80:
            global_id = _mapping_id(dimension, "global", source_value_norm)
            global_existing = self._get_mapping_by_id(global_id)
            should_create_global = True
            if global_existing:
                existing_canonical = _as_text(global_existing.get("canonical_value"))
                should_create_global = bool(
                    existing_canonical and existing_canonical.lower() == canonical_value.lower()
                )
            if should_create_global:
                global_doc = dict(incoming)
                global_doc["id"] = global_id
                global_doc["scope_type"] = "global"
                global_doc["scope_key"] = "global"
                global_existing = self._get_mapping_by_id(global_id)
                global_merged = self._merge_mapping_doc(global_existing, global_doc)
                self._save_mapping(global_merged)

        return {
            "canonical_value": canonical_value,
            "canonical_key": canonical_key,
            "found": False,
            "resolution_method": resolution_method,
            "confidence": confidence,
            "mapping_id": mapping_id,
            "scope_key": scope_key,
        }
