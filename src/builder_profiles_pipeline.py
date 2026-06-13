"""Builder discovery and profile enrichment pipeline.

Discovers real-estate builders from existing project indices and web search,
creates/updates a unified builder profile, and merge-upserts profiles into
Elasticsearch so new details are preserved across runs.
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
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

import numpy as np
import pandas as pd
import requests
from apscheduler.triggers.cron import CronTrigger
from elasticsearch import Elasticsearch, helpers
from pydantic import BaseModel
from plombery import Trigger, register_pipeline, task

from config import read_config
from utils.canonical_resolver import CanonicalResolver, DEFAULT_CANONICAL_INDEX
from utils.llm_client import call_llm

try:
    import json_repair
except Exception:
    json_repair = None

try:
    import dirtyjson
except Exception:
    dirtyjson = None

try:
    import rapidjson
except Exception:
    rapidjson = None


logger = logging.getLogger(__name__)


DEFAULT_INDEX = "builder_profiles"
DEFAULT_SOURCE_LAUNCH_INDEX = "property_launches"
DEFAULT_SOURCE_COMPLETED_INDEX = "property_completed_projects"
DEFAULT_SOURCE_PROJECT_INDEX = "property_projects"

DEFAULT_DISCOVERY_CITIES = ("Kochi", "Bengaluru", "Dubai")
DISCOVERY_QUERY_TEMPLATES = (
    "top real estate builders in {city}",
    "best builders in {city}",
    '"{city}" "realtors" "official website"',
    '"{city}" "real estate developers" "projects"',
    '"{city}" "builder" "rera"',
    '"{city}" "real estate company" "official site"',
)

CITY_SEARCH_FALLBACK_MIN_DDG_RESULTS = 6
CITY_SEARCH_FALLBACK_RESULT_LIMIT = 12

DOMAIN_GUESS_TLDS = (".in", ".com", ".co.in")
DOMAIN_GUESS_MAX_LOOKUPS_PER_RUN = 60
DOMAIN_GUESS_FETCH_TIMEOUT_SECONDS = 8

REALESTATEINDIA_BUILDER_SOURCE = "realestateindia_builders"
REALESTATEINDIA_BUILDER_CITY_SLUG_OVERRIDES = {
    "bengaluru": "bangalore",
    "kozhikode": "calicut",
}
REALESTATEINDIA_BUILDER_CITY_SLUG_ALIASES = {
    "bengaluru": ("bangalore", "bengaluru"),
    "thiruvananthapuram": ("thiruvananthapuram", "trivandrum"),
    "kozhikode": ("calicut", "kozhikode"),
}
REALESTATEINDIA_BUILDER_MAX_LOAD_MORE_PAGES = 25

REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
}

USER_AGENTS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.60 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.60 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.122 Safari/537.36",
)

BLOCKED_MARKERS = (
    "captcha",
    "verify you are a human",
    "access denied",
    "unusual traffic",
    "blocked",
    "are you a robot",
)

DDG_FAILURE_STREAK_THRESHOLD = 5

DISCOVERY_SOURCE_WEIGHTS = {
    "property_projects": 80,
    "property_launches": 65,
    "property_completed_projects": 55,
    "realestateindia_builders": 45,
    "realestateindia": 35,
    "duckduckgo": 20,
    "bing": 18,
    "brave": 16,
    "bing_city": 22,
    "brave_city": 20,
    "domain_guess": 24,
}

CITY_DISCOVERY_NOISE_DOMAINS = {
    "support.microsoft.com",
    "microsoft.com",
    "cambridge.org",
    "merriam-webster.com",
    "dictionary.com",
    "wordreference.com",
    "collinsdictionary.com",
    "bestbuy.com",
}

CITY_DISCOVERY_HINT_PHRASES = (
    "builder",
    "builders",
    "developer",
    "developers",
    "real estate",
    "realty",
    "realtor",
    "properties",
    "construction",
    "rera",
)

AGGREGATOR_DOMAINS = {
    "duckduckgo.com",
    "kochiprojects.com",
    "magicbricks.com",
    "housing.com",
    "99acres.com",
    "realestateindia.com",
    "commonfloor.com",
    "squareyards.com",
    "propzilla.in",
    "proptiger.com",
    "nobroker.in",
    "makaan.com",
    "en.wikipedia.org",
    "wikipedia.org",
}

SEARCH_ENGINE_DOMAINS = {
    "duckduckgo.com",
    "search.brave.com",
    "brave.com",
    "bing.com",
    "r.bing.com",
}

WEBSITE_TOKEN_STOPWORDS = {
    "builder",
    "builders",
    "developer",
    "developers",
    "properties",
    "property",
    "realty",
    "group",
    "homes",
    "project",
    "projects",
    "estate",
    "estates",
    "infra",
    "construction",
    "constructions",
    "enterprise",
    "enterprises",
    "international",
    "india",
    "pvt",
    "ltd",
    "limited",
    "private",
    "company",
    "co",
    "realtor",
    "realtors",
}

LOW_TRUST_TLDS = {"ai", "io", "dev", "app", "tech", "cloud", "xyz"}

BUILDER_NAME_HINT_TOKENS = {
    "builder",
    "builders",
    "developer",
    "developers",
    "properties",
    "property",
    "realty",
    "group",
    "homes",
    "infra",
    "infrastructure",
    "construction",
    "constructions",
    "estates",
    "projects",
    "housing",
    "realtor",
    "realtors",
    "pvt",
    "ltd",
    "limited",
    "llp",
    "inc",
    "company",
}

BUILDER_NAME_NOISE_PHRASES = (
    "project offers",
    "project consists",
    "which has",
    "one of",
    "providing you",
    "municipal town",
    "near ",
    " while ",
    "independent kseb",
    "power backup",
    "good quality cable",
    "fascia wall",
    "composite marble",
)

BUILDER_NAME_LEADING_NOISE_PATTERNS = (
    r"^(?:by\s+)?(?:the\s+)?(?:renowned|reputed|well[-\s]?known|trusted|prominent)\s+(?:builder|builders|developer|developers)\s+",
    r"^(?:by\s+)?(?:the\s+)(?:renowned|reputed|well[-\s]?known|trusted|prominent|leading)\s+",
    r"^(?:the\s+)?(?:builder|builders|developer|developers)\s+",
)

BUILDER_NAME_NOISE_TOKENS = {
    "this",
    "which",
    "offers",
    "offering",
    "consists",
    "consist",
    "providing",
    "largest",
    "luxurious",
    "old",
    "space",
    "apart",
    "apartm",
    "residential",
    "commercial",
    "independent",
    "meter",
    "meters",
    "kseb",
    "switch",
    "switches",
    "cable",
    "cables",
    "lift",
    "lifts",
    "fascia",
    "wall",
    "floor",
    "marble",
    "modular",
    "backup",
    "generator",
    "quality",
    "bed",
    "rooms",
    "point",
    "points",
    "supply",
    "essential",
    "elcb",
    "mcb",
    "mcbs",
    "geser",
    "geyser",
    "pain",
}

BUILDER_NAME_CONNECTOR_TOKENS = {"and", "or", "with", "by"}

SOCIAL_DOMAINS = {
    "linkedin.com",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "youtube.com",
}

MULTIPART_PUBLIC_SUFFIXES = {
    "co.in",
    "co.uk",
    "co.ae",
    "co.nz",
    "co.za",
    "co.ke",
    "co.id",
    "co.th",
    "com.au",
    "com.sg",
    "com.my",
    "com.hk",
    "com.br",
    "com.tr",
    "com.mx",
    "org.in",
    "org.uk",
    "org.au",
    "net.in",
    "net.au",
    "net.uk",
}

GENERIC_BUILDER_NAMES = {
    "builders",
    "builder",
    "developers",
    "developer",
    "properties",
    "realty",
    "top builders",
    "best builders",
    "real estate developers",
    "real estate builders",
}

INDIA_CITY_HINTS = {"kochi", "bengaluru", "bangalore", "mumbai", "pune", "hyderabad", "chennai", "delhi"}
UAE_CITY_HINTS = {"dubai", "abu dhabi", "sharjah", "ajman", "al ain", "ras al khaimah", "fujairah"}
KSA_CITY_HINTS = {"riyadh", "jeddah", "dammam", "mecca", "makkah", "medina", "madinah"}
LOCATION_TOKENS = {
    "kochi",
    "thiruvananthapuram",
    "trivandrum",
    "kozhikode",
    "calicut",
    "bengaluru",
    "bangalore",
    "dubai",
    "riyadh",
    "jeddah",
    "dammam",
    "mecca",
    "makkah",
    "medina",
    "madinah",
    "ksa",
    "saudi",
    "uae",
    "india",
    "mumbai",
    "pune",
    "hyderabad",
    "chennai",
    "delhi",
}

CURATED_BUILDER_WEBSITES = {
    "artech": "https://artechrealtors.com/",
    "artech-realtors": "https://artechrealtors.com/",
    "prestige": "https://www.prestigeconstructions.com/",
    "asset": "https://www.assethomes.in/",
    "brigade": "https://www.brigadegroup.com/",
    "brigade-enterprises": "https://www.brigadegroup.com/",
    "sfs": "https://www.sfshomes.com/",
    "abad": "https://www.abadbuilders.com/",
    "adarsh": "https://www.adarshdevelopers.com/",
    "sobha": "https://www.sobha.com/",
    "puravankara": "https://www.puravankara.com/",
    "godrej": "https://www.godrejproperties.com/",
    "sumadhura": "https://www.sumadhuragroup.com/",
    "salarpuria-sattva": "https://www.sattvagroup.in/",
    "sowparnika-projects": "https://www.sowparnika.com/",
    "vaishnavi": "https://www.vaishnavigroup.com/",
    "mahaveer": "https://www.mahaveergroup.com/",
    "confident": "https://www.confident-group.com/",
    "desai": "https://www.desaihomes.com/",
    "mana-projects": "https://www.manaprojects.com/",
    "kent": "https://www.kenthomes.in/",
    "skyline-kochi": "https://www.skylinebuilders.com/",
    "skyline": "https://www.skylinebuilders.com/",
    "signature": "https://signaturedwellingsprojects.com/",
    "oraiyan-groups": "https://www.oraiyan.com/",
    "oraiyan": "https://www.oraiyan.com/",
    "ansal-buildwell": "https://ansalbuildwell.com/",
    "embassy": "https://embassygroup.com/",
    "house-of-hiranandani": "https://www.hiranandani.com/",
    "divyasree": "https://divyasree.com/",
    "mather-projects": "https://www.matherprojects.com/",
    "prime-meridian": "https://primemeridian.in/",
    "vinayaka": "https://www.vinayakahomes.com/",
    "aisshwarya": "https://www.aisshwarya.com/",
    "aisshwarya-in-jp-nagar": "https://www.aisshwarya.com/",
    "travancore": "https://www.travancorebuilders.com/",
    "siddhi": "https://www.siddhihomes.com/",
    "inspira-projects": "https://www.inspira-builders.com/",
    "aratt": "https://www.aratt.in/",
    "address-maker": "https://www.addressmaker.in/",
    "bavasons": "https://www.bavasons.in/",
    "abhee-ventures": "https://www.abhee-ventures.com/",
    "prestige-estates-projects": "https://www.prestigeconstructions.com/",
    "dream-flower": "https://www.dreamflower.in/",
    "evantha-sri-durga": "https://esd.in/",
    "buildiko": "https://www.buildiko.com/",
    "pariwar-housing-corporation": "https://pariwarhousing.com/",
    "peninsula": "https://peninsula.co.in/",
    "greennesto": "https://greennesto.com/",
    "relcon": "https://relcon.co.in/",
    "shravanthi": "https://www.shravanthigroup.com/",
    "nagpal": "https://www.nagpaldevelopers.com/",
    "national": "https://nationalbuilders.in/",
    "classic-ventures": "https://www.classicventures.in/",
    "serene-communities": "https://serenecommunities.in/",
    "sbr": "https://www.sbrgroup.in/",
    "jr-housing": "https://www.jrhousing.com/",
    "assetz-services": "https://www.assetzproperty.com/",
    "bren-corporation": "https://bren.com/",
    "mahidhara": "https://www.mahidharaprojects.com/",
    "mahidhara-projects": "https://www.mahidharaprojects.com/",
    "greentech": "https://www.greentechbuilders.in/",
    "konig": "https://konighomes.co.in/",
    "bhavisha": "https://www.bhavishahomes.com/",
    "anta": "https://www.antabuilders.com/",
    "ac-city-projects": "https://www.accitybuilders.in/",
    "ac-city": "https://www.accitybuilders.in/",
    "ac-city-projects-l": "https://www.accitybuilders.in/",
    "shriram": "https://www.shriramproperties.com/",
    "shriram-limite": "https://www.shriramproperties.com/",
    "divyasr": "https://divyasree.com/",
    "ncc-urban": "https://www.nccurban.com/",
    "surya": "https://suryadevelopers.in/",
    "sparkle": "https://sparklerealty.in/",
    "trinity-and": "https://trinitybuild.com/",
    "manjooran-housin": "https://www.manjooran.com/",
    "motif-and": "https://www.motifbuilders.com/",
    "olive": "https://www.olivebuilder.com/",
    "kp-varkey-vs": "https://www.kpvandvs.in/",
    "saranya": "https://saranyagroup.com/",
    "saritha": "https://sarithadevelopers.com/",
    "sra": "https://sragroup.in/",
    "reviva-projects": "https://revivaprojects.com/projects/",
    "mir-realtors": "https://www.mirrealtors.in/",
    "bcg": "https://bcgbuilders.com/new/",
    "bluejay-enterprises": "https://www.bluejay.in/",
    "pride": "https://www.pridegroup.net/",
    "elegant": "https://eleganthomes.in/",
    "heritage": "https://heritagebuilders.co.in/",
    "keya": "https://keyahomes.in/",
    "gokulam-engineers-india-i": "https://gokulam.com/",
    "galaxy": "https://galaxyhomes.com/",
    "galaxy-in-kaloor": "https://galaxyhomes.com/",
    "mana": "https://www.manaprojects.com/",
    "ncc-urban-infrastructure": "https://www.nccurban.com/",
    "kv": "https://kvhomes.in/",
    "lodha": "https://www.lodhagroup.com/",
    "lodha-gr": "https://www.lodhagroup.com/",
    "ace-ideal-india": "https://acehomes.in/",
    "alfa-ventures": "https://www.alfaventures.com/",
    "ameliorate-realtors": "https://www.haranorthone.in/",
    "amazing-ambiance": "https://www.aganiprojects.com/",
    "anasvara-souparnika": "https://www.anasvaraproperties.com/",
    "arihant-d": "https://www.arihantdevelopers.in/",
    "ars-infraa": "https://www.arszurich.in/",
    "arya-bhangy": "https://www.aryabhangybuilders.com/",
    "cadabams-senior-dwelling": "https://www.cadabamswenest.com/",
    "chakolas-habitat": "https://www.chakolas.com/",
    "clearway": "https://www.clearwaybuilders.com/",
    "cmm-groups": "https://cmminfra.co.in/",
    "fortune-one": "https://fortunegroup.in/",
    "fresh-air": "https://www.dlf.in/",
    "global-global-villas-has-been": "https://globaldevelopers.co.in/",
    "grc": "https://grcinfra.com/",
    "heera": "https://heeragroup.com/",
    "heera-at-kadavanthra": "https://heeragroup.com/",
    "indraprastha-shelters": "https://www.indraprastha.in/",
    "ittina": "https://ittinagroup.com/",
    "jeevanadi-estates": "https://www.jeevanadiestates.com/",
    "joyalukkas-lifestyle": "https://www.joyalukkasdevelopers.com/",
    "kristal": "https://kristalgroup.co.in/",
    "malabar-develope": "https://www.malabardevelopers.com/",
    "mps": "https://mpsbuilders.in/",
    "napa-valley-villas": "https://www.concorde.in/",
    "noel": "https://www.noelprojects.com/",
    "nvt-quality-lifestyle-projects": "https://www.nvtprojects.com/",
    "pashmina": "https://www.pashminadevelopers.com/",
    "paul-alukkas": "https://www.paulalukkasdevelopers.com/",
    "premium-design-coupled-with-a-contemporar": "https://slvdevelopers.in/",
    "provi": "https://www.providenthousing.com/",
    "raj-and": "https://www.rajbuilders.in/",
    "rare-earth": "https://www.rareearthdevelopers.com/",
    "reputed-promoters": "https://www.nccurban.com/",
    "royal-retreat": "https://royalretreathomes.in/",
    "sai-sumukha": "https://www.saisumukha.in/",
    "sapthagiri-who-are-one": "https://sapthagiridevelopers.com/",
    "shreeji-infrastructure-india": "https://shreejiindia.co.in/",
    "slv-structures": "https://www.slvstructures.com/",
    "sms": "https://smsbuilders.in/",
    "sms-ideally-situated": "https://smsbuilders.in/",
    "spad-classic": "https://www.nakshatrahomes.in/",
    "sri-chakra": "https://www.srichakrabuilders.com/",
    "sri-dwaraka-and": "https://www.sridwarakabuildersanddevelopers.com/",
    "sumadhura-is-a-thoughtfully-crafted": "https://www.sumadhuragroup.com/",
    "syama-dynamic": "https://www.syamadynamic.com/",
    "the-arya": "https://aryaprojects.com/",
    "the-prestige-off-sarjapur-road": "https://www.prestigeconstructions.com/",
    "the-renowned-bhartiya-city": "https://bhartiyacity.com/",
    "the-serenity-and-beauty-of-nature-rivier": "https://www.oxoniyabuilders.com/",
    "vani": "https://vaniproperties.in/",
    "gm-infinite": "https://infinitebuilders.in/",
    "good-earth-estates": "https://www.goodearthinfra.in/",
    "anna": "http://www.annadevelopers.com/",
    "radiant": "https://www.radiantproperties.in/",
    "royal-projects": "https://www.royalprojects.in/",
    "silver-castle": "https://silvercastle.co.in/",
    "southern-investments": "https://southernproperties.in/",
    "svr-farms-vaikuntam-farmland": "https://svrfarms.com/",
    "sv": "https://svinfra.com/",
    "shivaganga": "https://www.shivaganga.in/",
    "skylite": "https://skylitebuilders.com/",
    "virtue": "https://virtuebuilders.com/",
    "ar-ventures": "https://arventures.co.in/",
    "atmos": "https://atmoslifestyle.com/tropicalwoods/",
    "julphar-and": "https://www.julpharbuilders.com/",
    "nov": "https://www.ektadevelopers.com/",
    "an-american-architect": "https://www.enessen.com/",
    "the-colacos": "https://continentalbuilders.in/",
    "iidl": "https://iidlindia.com/",
    "ms-ramaiah-and": "https://msrdb.com/",
}

RESIDENTIAL_TYPES = {
    "apartment",
    "villa",
    "townhouse",
    "duplex",
    "studio",
    "plotted-development",
    "plot",
    "flat",
}
COMMERCIAL_TYPES = {"office", "retail", "commercial", "warehouse", "shop"}

PROFILE_LIST_FIELDS = {
    "aliases",
    "founders",
    "key_people",
    "operating_cities",
    "operating_states",
    "operating_countries",
    "specializations",
    "key_projects",
    "recent_launches",
    "known_websites",
    "source_names",
    "sources",
    "rera_numbers",
}

MIN_FIELDS = {"price_range_min", "price_per_sqft_min"}
MAX_FIELDS = {
    "price_range_max",
    "price_per_sqft_max",
    "total_projects",
    "total_projects_completed",
    "total_projects_ongoing",
    "google_reviews_count",
}

MANDATORY_FIELDS = {
    "id",
    "builder_name",
    "builder_slug",
    "website",
    "data_quality",
    "discovered_at",
    "updated_at",
    "last_enriched_at",
    "enrichment_count",
}

BUILDER_PROFILE_EXTRACT_PROMPT = """You are a real-estate company profiling engine.
Extract details for the given builder from search snippets and page text.

Return ONLY a valid JSON object with EXACTLY these keys:
- builder_name (string)
- aliases (array of strings)
- website (string)
- head_office_city (string)
- head_office_state (string)
- head_office_country (string)
- head_office_address (string)
- founded_year (integer)
- founders (array of strings)
- key_people (array of strings)
- company_type (string: private|public|listed|government|unknown)
- category (string: luxury|premium|mid-segment|affordable|mixed-use|residential|commercial|unknown)
- specializations (array of strings)
- main_operating_city (string)
- operating_cities (array of strings)
- operating_states (array of strings)
- operating_countries (array of strings)
- rera_reg_no (string)
- rera_numbers (array of strings)
- linkedin_url (string)
- twitter_url (string)
- youtube_url (string)
- instagram_url (string)
- facebook_url (string)
- google_reviews_rating (number)
- google_reviews_count (integer)
- reputation_summary (string)

Rules:
- If unknown, use null (or [] for arrays).
- Do not guess facts.
- No markdown, no prose, no code fences.
"""


BUILDER_PROFILE_INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "5s",
    },
    "mappings": {
        "dynamic": True,
        "dynamic_templates": [
            {"dates_iso": {"match": "*_at", "mapping": {"type": "date", "format": "strict_date_optional_time||epoch_millis"}}},
            {"dates": {"match": "*_date", "mapping": {"type": "date", "format": "strict_date_optional_time||epoch_millis"}}},
            {"epochs": {"match": "*_epoch", "mapping": {"type": "long"}}},
            {"strings": {"match_mapping_type": "string", "mapping": {"type": "keyword", "ignore_above": 512}}},
            {"doubleNums": {"match_mapping_type": "double", "mapping": {"type": "double"}}},
            {"longNums": {"match_mapping_type": "long", "mapping": {"type": "long"}}},
        ],
        "properties": {
            "id": {"type": "keyword"},
            "builder_name": {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}},
            "builder_slug": {"type": "keyword"},
            "aliases": {"type": "keyword"},
            "website": {"type": "keyword"},
            "website_source": {"type": "keyword"},
            "website_confidence": {"type": "float"},
            "known_websites": {"type": "keyword"},
            "logo_url": {"type": "keyword"},
            "head_office_city": {"type": "keyword"},
            "head_office_state": {"type": "keyword"},
            "head_office_country": {"type": "keyword"},
            "head_office_address": {"type": "text"},
            "head_office_location": {"type": "keyword"},
            "founded_year": {"type": "integer"},
            "founders": {"type": "keyword"},
            "key_people": {"type": "keyword"},
            "ceo": {"type": "keyword"},
            "managing_director": {"type": "keyword"},
            "company_type": {"type": "keyword"},
            "stock_ticker": {"type": "keyword"},
            "cin": {"type": "keyword"},
            "rera_reg_no": {"type": "keyword"},
            "rera_numbers": {"type": "keyword"},
            "rera_registered": {"type": "boolean"},
            "operating_cities": {"type": "keyword"},
            "operating_states": {"type": "keyword"},
            "operating_countries": {"type": "keyword"},
            "main_operating_city": {"type": "keyword"},
            "category": {"type": "keyword"},
            "specializations": {"type": "keyword"},
            "builder_tier": {"type": "keyword"},
            "total_projects": {"type": "integer"},
            "total_projects_completed": {"type": "integer"},
            "total_projects_ongoing": {"type": "integer"},
            "years_in_business": {"type": "integer"},
            "price_range_min": {"type": "long"},
            "price_range_max": {"type": "long"},
            "price_per_sqft_min": {"type": "long"},
            "price_per_sqft_max": {"type": "long"},
            "price_currency": {"type": "keyword"},
            "linkedin_url": {"type": "keyword"},
            "twitter_url": {"type": "keyword"},
            "youtube_url": {"type": "keyword"},
            "instagram_url": {"type": "keyword"},
            "facebook_url": {"type": "keyword"},
            "google_reviews_rating": {"type": "double"},
            "google_reviews_count": {"type": "integer"},
            "reputation_summary": {"type": "text"},
            "key_projects": {"type": "keyword"},
            "recent_launches": {"type": "keyword"},
            "source_names": {"type": "keyword"},
            "sources": {"type": "keyword"},
            "data_quality": {"type": "keyword"},
            "enrichment_count": {"type": "integer"},
            "discovered_at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
            "updated_at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
            "last_enriched_at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
        },
    },
}


@dataclass(slots=True)
class BuilderProfileSettings:
    es_index: str
    canonical_enabled: bool
    canonical_index: str
    canonical_max_ai_calls: int
    canonical_ai_enabled: bool
    source_launch_index: str
    source_completed_index: str
    source_project_index: str
    discovery_cities: list[str]
    realestateindia_enabled: bool
    realestateindia_max_load_more_pages: int
    duckduckgo_pages: int
    detail_pages_per_builder: int
    max_builders_per_run: int
    min_delay_seconds: float
    max_delay_seconds: float
    request_timeout_seconds: int
    schedule_hour: int
    schedule_minute: int
    schedule_timezone: str
    data_dir: Path


def _to_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cfg_get(section: Any, key: str, default: Any) -> Any:
    if section is None:
        return default
    try:
        return section.get(key, fallback=default)
    except TypeError:
        try:
            return section.get(key, default)
        except Exception:
            return default
    except Exception:
        return default


def _to_bool(value: Any, default: bool = False) -> bool:
    text = (_as_text(value) or "").strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "y", "on"}


def _get_section(config_obj: Any, section_name: str) -> Any:
    try:
        if hasattr(config_obj, "has_section") and config_obj.has_section(section_name):
            return config_obj[section_name]
    except Exception:
        pass
    try:
        return config_obj[section_name]
    except Exception:
        return {}


def _parse_es_hosts(hosts_value: str) -> list[str]:
    hosts: list[str] = []
    for raw in (hosts_value or "").split(","):
        host = raw.strip()
        if not host:
            continue
        if not host.startswith("http"):
            host = f"http://{host}"
        if ":" not in host.split("//", 1)[-1]:
            host += ":9200"
        hosts.append(host)
    return hosts


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(str(value).replace(",", "").strip()))
    except Exception:
        return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


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


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict, set, tuple)):
        return len(value) > 0
    return True


def _clean_list(values: Any) -> list[str]:
    if values is None:
        return []
    if not isinstance(values, list):
        values = [values]
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


def _merge_lists(existing: Any, incoming: Any) -> list[str]:
    merged = []
    seen: set[str] = set()
    for value in _clean_list(existing) + _clean_list(incoming):
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        merged.append(value)
    return merged


def _normalize_url(url: Any) -> str | None:
    text = _as_text(url)
    if not text:
        return None
    if text.startswith("//"):
        return f"https:{text}"
    if text.startswith("http://") or text.startswith("https://"):
        return text
    if "." in text and " " not in text:
        return f"https://{text}"
    return None


def _looks_like_asset_url(url: str | None) -> bool:
    norm = _normalize_url(url)
    if not norm:
        return False
    parsed = urlparse(norm)
    path = (parsed.path or "").lower()
    if not path:
        return False
    if path in {"/favicon.ico", "/robots.txt", "/sitemap.xml", "/opensearch_lite_v2.xml"}:
        return True
    if path.endswith((
        ".css",
        ".js",
        ".mjs",
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".svg",
        ".ico",
        ".webp",
        ".avif",
        ".xml",
        ".json",
        ".woff",
        ".woff2",
        ".ttf",
        ".eot",
        ".map",
    )):
        return True
    segments = [segment for segment in path.split("/") if segment]
    return any(segment in {"assets", "dist", "static", "images", "img", "css", "js", "fonts"} for segment in segments)


def _extract_domain(url: str | None) -> str:
    if not url:
        return ""
    try:
        host = urlparse(url).netloc.lower().strip()
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def _domain_root(domain: str | None) -> str:
    if not domain:
        return ""
    labels = [label for label in str(domain).lower().split(".") if label]
    if len(labels) < 2:
        return labels[0] if labels else ""
    suffix2 = ".".join(labels[-2:])
    if len(labels) >= 3 and suffix2 in MULTIPART_PUBLIC_SUFFIXES:
        return labels[-3]
    return labels[-2]


def _is_social_url(url: str | None) -> bool:
    domain = _extract_domain(url)
    if not domain:
        return False
    return any(domain == s or domain.endswith(f".{s}") for s in SOCIAL_DOMAINS)


def _is_aggregator_url(url: str | None) -> bool:
    domain = _extract_domain(url)
    if not domain:
        return False
    return any(domain == d or domain.endswith(f".{d}") for d in AGGREGATOR_DOMAINS)


def _is_candidate_builder_website(url: str | None) -> bool:
    norm = _normalize_url(url)
    if not norm:
        return False
    domain = _extract_domain(norm)
    if any(domain == d or domain.endswith(f".{d}") for d in SEARCH_ENGINE_DOMAINS):
        return False
    return not _is_social_url(norm) and not _is_aggregator_url(norm)


def _sanitize_known_websites(values: Any) -> list[str]:
    sanitized: list[str] = []
    seen: set[str] = set()
    for value in _clean_list(values):
        norm = _normalize_url(value)
        if not norm:
            continue
        if not _is_candidate_builder_website(norm):
            continue
        if _looks_like_asset_url(norm):
            continue
        key = norm.lower()
        if key in seen:
            continue
        seen.add(key)
        sanitized.append(norm)
    return sanitized


def _domain_guess_urls_for_builder(builder_name: str | None) -> list[str]:
    text = _normalize_builder_name(builder_name)
    if not text:
        return []

    tokens = [token for token in re.split(r"[^a-z0-9]+", text.lower()) if token]
    if not tokens:
        return []

    legal_tokens = {"pvt", "private", "limited", "ltd", "llp", "inc", "co", "company"}
    compact_tokens = [token for token in tokens if token not in legal_tokens]
    if not compact_tokens:
        compact_tokens = tokens[:]

    bases: list[str] = []

    compact = "".join(compact_tokens)
    if len(compact) >= 5:
        bases.append(compact)

    if len(compact_tokens) >= 2:
        first_two = f"{compact_tokens[0]}{compact_tokens[1]}"
        if len(first_two) >= 5:
            bases.append(first_two)

    first = compact_tokens[0]
    if len(first) >= 5:
        bases.append(first)
        suffix_hints = {"homes", "builders", "developers", "properties", "realtors", "realty", "infra", "group"}
        if len(compact_tokens) >= 2 and compact_tokens[1] in suffix_hints:
            bases.append(f"{first}{compact_tokens[1]}")

    uniq_bases: list[str] = []
    seen_bases: set[str] = set()
    for base in bases:
        cleaned = re.sub(r"[^a-z0-9]", "", base.lower())
        if len(cleaned) < 5 or cleaned in seen_bases:
            continue
        seen_bases.add(cleaned)
        uniq_bases.append(cleaned)

    out: list[str] = []
    seen_urls: set[str] = set()
    for base in uniq_bases:
        for tld in DOMAIN_GUESS_TLDS:
            for prefix in ("https://www.", "https://"):
                url = f"{prefix}{base}{tld}/"
                key = url.lower()
                if key in seen_urls:
                    continue
                seen_urls.add(key)
                out.append(url)
    return out


def _guess_builder_website(session: requests.Session, builder_name: str | None) -> str | None:
    slug = _builder_slug(_as_text(builder_name) or "")
    for candidate_url in _domain_guess_urls_for_builder(builder_name):
        try:
            response = _session_get(session, candidate_url, timeout=DOMAIN_GUESS_FETCH_TIMEOUT_SECONDS)
        except Exception:
            continue

        if int(getattr(response, "status_code", 0) or 0) >= 400:
            continue

        resolved = _normalize_url(getattr(response, "url", None)) or _normalize_url(candidate_url)
        if not resolved:
            continue
        if not _is_candidate_builder_website(resolved) or _looks_like_asset_url(resolved):
            continue

        confidence = _website_candidate_confidence(resolved, slug, builder_name)
        if confidence >= 0.62:
            return resolved
    return None


def _city_priority_index(city: str | None) -> int:
    city_text = (_as_text(city) or "").lower()
    if not city_text:
        return len(SETTINGS.discovery_cities)
    for idx, configured_city in enumerate(SETTINGS.discovery_cities):
        if city_text == (_as_text(configured_city) or "").lower():
            return idx
    return len(SETTINGS.discovery_cities)


def _apply_domain_guess_enrichment(session: requests.Session, discovered: list[dict[str, Any]]) -> None:
    if not discovered:
        return

    candidates: list[dict[str, Any]] = []
    for item in discovered:
        if _clean_list(item.get("known_websites")):
            continue
        if _has_value(item.get("website")):
            continue
        if REALESTATEINDIA_BUILDER_SOURCE not in {s.lower() for s in _clean_list(item.get("source_names"))}:
            continue
        if not _is_reliable_builder_name_for_website(item.get("builder_name")):
            continue
        candidates.append(item)

    if not candidates:
        return

    candidates.sort(
        key=lambda item: (
            _city_priority_index(_as_text(item.get("main_operating_city")) or (_clean_list(item.get("operating_cities")) or [None])[0]),
            int(item.get("_discovery_order") or 10**9),
            -_builder_name_score(item.get("builder_name")),
            str(item.get("builder_name") or "").lower(),
        )
    )

    cache: dict[str, str | None] = {}
    for item in candidates[: max(0, DOMAIN_GUESS_MAX_LOOKUPS_PER_RUN)]:
        builder_name = _as_text(item.get("builder_name"))
        if not builder_name:
            continue

        key = builder_name.lower()
        if key not in cache:
            cache[key] = _guess_builder_website(session, builder_name)

        guessed = cache.get(key)
        if not guessed:
            continue

        item["known_websites"] = _merge_lists(item.get("known_websites"), [guessed])
        item["sources"] = _merge_lists(item.get("sources"), [guessed])
        item["source_names"] = _merge_lists(item.get("source_names"), ["domain_guess"])


def _curated_website_for_builder(builder_name: str | None, builder_slug: str | None, aliases: Any = None) -> str | None:
    candidate_keys: list[str] = []

    if builder_slug:
        candidate_keys.append(str(builder_slug).strip().lower())

    normalized_name = _normalize_builder_name(builder_name)
    if normalized_name:
        candidate_keys.append(_builder_slug(normalized_name))

    for alias in _clean_list(aliases):
        normalized_alias = _normalize_builder_name(alias)
        if normalized_alias:
            candidate_keys.append(_builder_slug(normalized_alias))

    seen: set[str] = set()
    for key in candidate_keys:
        if not key or key in seen:
            continue
        seen.add(key)
        website = _normalize_url(CURATED_BUILDER_WEBSITES.get(key))
        if not website:
            continue
        if not _is_candidate_builder_website(website):
            continue
        return website
    return None


def _is_valid_discovery_url(url: str | None) -> bool:
    norm = _normalize_url(url)
    if not norm:
        return False
    domain = _extract_domain(norm)
    if not domain:
        return False
    if any(domain == d or domain.endswith(f".{d}") for d in SEARCH_ENGINE_DOMAINS):
        return False
    if _looks_like_asset_url(norm):
        return False
    return True


def _builder_website_tokens(builder_name: str | None, builder_slug: str | None) -> set[str]:
    tokens: set[str] = set()
    if builder_slug:
        for token in re.split(r"[^a-z0-9]+", builder_slug.lower()):
            if len(token) >= 3 and token not in WEBSITE_TOKEN_STOPWORDS and token not in LOCATION_TOKENS:
                tokens.add(token)
    if builder_name:
        for token in re.split(r"[^a-z0-9]+", builder_name.lower()):
            if len(token) >= 3 and token not in WEBSITE_TOKEN_STOPWORDS and token not in LOCATION_TOKENS:
                tokens.add(token)
    return tokens


def _token_matches_domain(token: str, domain: str) -> bool:
    labels = [label for label in re.split(r"[.-]", domain.lower()) if label]
    for label in labels:
        if label == token:
            return True
        if len(token) >= 5 and label.startswith(token):
            return True
        if len(token) >= 5 and token.startswith(label):
            return True
    return False


def _has_legal_entity_marker(builder_name: str | None) -> bool:
    text = (_as_text(builder_name) or "").lower()
    tokens = [token for token in re.split(r"[^a-z0-9]+", text) if token]
    if not tokens:
        return False
    markers = {"pvt", "ltd", "limited", "llp", "inc", "company", "co"}
    return any(token in markers for token in tokens)


def _root_token_alignment(root: str, token: str) -> str | None:
    if not root or not token:
        return None
    if root == token:
        return "exact"
    suffix_whitelist = {
        "group",
        "builders",
        "builder",
        "developer",
        "developers",
        "realtor",
        "realtors",
        "homes",
        "infra",
        "infrastructure",
        "properties",
        "property",
        "projects",
        "estates",
        "estate",
        "construction",
        "constructions",
    }
    if root.startswith(token):
        suffix = root[len(token) :]
        if suffix in suffix_whitelist:
            return "suffix"
    return None


def _is_reliable_builder_name_for_website(builder_name: str | None) -> bool:
    if not _looks_like_builder_name(builder_name):
        return False
    text = _normalize_builder_name(builder_name)
    if not text:
        text = _as_text(builder_name)
    if not text:
        return False
    lower = text.lower()
    if "..." in lower:
        return False
    if len(lower) > 48:
        return False
    tokens = [token for token in re.split(r"[^a-z0-9]+", lower) if token]
    if len(tokens) > 6:
        return False
    noisy_markers = {
        "this",
        "which",
        "has",
        "been",
        "offers",
        "offering",
        "project",
        "projects",
        "municipal",
        "town",
        "residential",
        "commercial",
    }
    if any(token in noisy_markers for token in tokens):
        return False
    return True


def _website_candidate_confidence(url: str | None, builder_slug: str | None, builder_name: str | None) -> float:
    norm = _normalize_url(url)
    if not norm or not _is_candidate_builder_website(norm) or _looks_like_asset_url(norm):
        return 0.0
    domain = _extract_domain(norm)
    if not domain:
        return 0.0
    root = _domain_root(domain)
    labels = [label for label in domain.split(".") if label]
    if labels and labels[-1] in LOW_TRUST_TLDS:
        return 0.2
    tokens = _builder_website_tokens(builder_name, builder_slug)
    if not tokens:
        return 0.25
    if not _is_reliable_builder_name_for_website(builder_name) and len(tokens) <= 1:
        return 0.25
    matched_tokens = [token for token in tokens if _token_matches_domain(token, domain)]
    if not matched_tokens:
        return 0.25

    matched_unique = sorted(set(matched_tokens), key=len, reverse=True)
    if len(matched_unique) >= 2:
        score = 0.9
    else:
        token = matched_unique[0]
        has_legal_marker = _has_legal_entity_marker(builder_name)
        alignment = _root_token_alignment(root, token)
        if len(tokens) == 1 and len(token) >= 5 and has_legal_marker:
            score = 0.82
        else:
            score = 0.62
        if alignment == "exact" and (len(tokens) > 1 or has_legal_marker):
            score += 0.14
        elif alignment == "suffix" and (len(tokens) > 1 or has_legal_marker):
            score += 0.08
        elif alignment == "suffix" and len(token) >= 5:
            score += 0.08
    return min(score, 0.99)


def _infer_website_from_evidence_url(url: str | None, builder_name: str | None, builder_slug: str | None) -> str | None:
    norm = _normalize_url(url)
    if not norm:
        return None
    if _website_candidate_confidence(norm, builder_slug, builder_name) < 0.9:
        return None
    return norm


def _url_in_values(url: str | None, values: Any) -> bool:
    norm = _normalize_url(url)
    if not norm:
        return False
    candidates = {_normalize_url(value) for value in _clean_list(values)}
    return norm in {value for value in candidates if value}


def _looks_like_official_site(url: str | None, builder_slug: str) -> bool:
    norm = _normalize_url(url)
    if not norm:
        return False
    if not _is_candidate_builder_website(norm):
        return False
    domain = _extract_domain(norm)
    if not domain:
        return False
    slug_tokens = _builder_website_tokens(None, builder_slug)
    if not slug_tokens:
        return False
    return any(_token_matches_domain(token, domain) for token in slug_tokens)


def _builder_identity_key(name: str) -> str:
    text = _normalize_space(name.lower())
    text = re.sub(r"[^a-z0-9\s&.-]", " ", text)
    text = _normalize_space(text)
    tokens = [
        tok
        for tok in re.split(r"[\s.-]+", text)
        if tok and tok
        not in {
            "pvt",
            "private",
            "limited",
            "ltd",
            "llp",
            "inc",
            "co",
            "company",
            "group",
            "builders",
            "builder",
            "developers",
            "developer",
            "properties",
            "property",
            "realty",
            "realtor",
            "realtors",
            "constructions",
            "construction",
            "infra",
            "homes",
        }
    ]
    if not tokens:
        tokens = re.split(r"[\s.-]+", text)
    return "-".join(tok for tok in tokens if tok)


def _builder_slug(name: str) -> str:
    identity = _builder_identity_key(name)
    slug = re.sub(r"[^a-z0-9]+", "-", identity).strip("-")
    return slug or "builder"


def _builder_doc_id(name: str) -> str:
    identity = _builder_identity_key(name) or name.lower().strip()
    return hashlib.sha1(identity.encode("utf-8")).hexdigest()


def _normalize_builder_name(value: Any) -> str | None:
    text = _as_text(value)
    if not text:
        return None
    text = html.unescape(text)
    text = re.sub(r"^\d+[\).\-\s]+", "", text)
    text = re.sub(r"\s*\|.*$", "", text)
    text = re.sub(r"\s*-\s*(?:realestateindia|commonfloor|housing|propertyfinder|bayut)\b.*$", "", text, flags=re.I)
    text = re.sub(r"\s+(?:search\s+from|search\s+over)\b.*$", "", text, flags=re.I)
    text = re.sub(r"\s*-\s*(projects|new projects|launches).*$", "", text, flags=re.I)
    for pattern in BUILDER_NAME_LEADING_NOISE_PATTERNS:
        text = re.sub(pattern, "", text, flags=re.I)
    text = re.sub(r"\.\s*(the|this|which|while|it)\b.*$", "", text, flags=re.I)
    text = re.sub(r"\.\s*project\b.*$", "", text, flags=re.I)
    text = re.sub(r"\s+in\s+(kochi|bengaluru|bangalore|dubai|uae|india)\b.*$", "", text, flags=re.I)
    text = re.sub(r"\s+(in|at|by)\s+(the|a|an)\b.*$", "", text, flags=re.I)
    text = re.sub(r"\s+(in|at|by)\s+[a-z]{1,3}$", "", text, flags=re.I)
    text = re.sub(r"\s+(in|at|by)$", "", text, flags=re.I)
    text = text.strip(" -,:|")
    text = _normalize_space(text)
    tail_noise = {"in", "at", "by", "the", "of", "for", "on", "to", "th"}
    tokens = [token for token in text.split(" ") if token]
    while tokens and tokens[-1].lower() in tail_noise:
        tokens.pop()
    text = " ".join(tokens)
    lowered = text.lower()
    if any(phrase in lowered for phrase in BUILDER_NAME_NOISE_PHRASES):
        return None
    if tokens and tokens[0].lower() in BUILDER_NAME_CONNECTOR_TOKENS:
        return None
    if not text or text.lower() in GENERIC_BUILDER_NAMES:
        return None
    if len(re.sub(r"[^A-Za-z]", "", text)) < 3:
        return None
    return text


def _looks_like_builder_name(value: Any, *, strict: bool = False) -> bool:
    text = _normalize_builder_name(value)
    if not text:
        return False
    lower = text.lower()
    if lower.startswith(("unknown", "no project", "none", "nil")):
        return False
    if "..." in lower or "&rsquo" in lower or "\u2019" in lower:
        return False
    if any(phrase in lower for phrase in BUILDER_NAME_NOISE_PHRASES):
        return False
    if len(lower) > 64:
        return False

    tokens = [token for token in re.split(r"[^a-z0-9]+", lower) if token]
    if not tokens or len(tokens) > 8:
        return False
    if tokens[0] in BUILDER_NAME_CONNECTOR_TOKENS:
        return False
    if tokens[-1] in {"in", "at", "by", "the", "of", "for", "on", "to", "th"}:
        return False

    hint_count = sum(1 for token in tokens if token in BUILDER_NAME_HINT_TOKENS)
    noise_count = sum(1 for token in tokens if token in BUILDER_NAME_NOISE_TOKENS)
    connector_count = sum(1 for token in tokens if token in BUILDER_NAME_CONNECTOR_TOKENS)
    has_legal_marker = _has_legal_entity_marker(text)

    if strict and hint_count == 0 and (len(tokens) == 1 or noise_count > 0):
        return False
    if strict and hint_count == 0 and connector_count > 0 and not has_legal_marker:
        return False
    if strict and hint_count == 0 and len(tokens) >= 4 and not has_legal_marker:
        return False
    if noise_count > 0 and hint_count <= 1:
        return False
    if strict and len(tokens) == 1 and hint_count == 0:
        return False
    return True


def _builder_name_score(value: Any) -> int:
    text = _normalize_builder_name(value)
    if not text or not _looks_like_builder_name(text):
        return 0
    lower = text.lower()
    tokens = [token for token in re.split(r"[^a-z0-9]+", lower) if token]
    hint_count = sum(1 for token in tokens if token in BUILDER_NAME_HINT_TOKENS)
    return len(text) + (hint_count * 5)


def _select_best_builder_name(*values: Any, strict: bool = False) -> str | None:
    best_name = None
    best_score = 0
    for value in values:
        text = _normalize_builder_name(value)
        if not text or not _looks_like_builder_name(text, strict=strict):
            continue
        score = _builder_name_score(text)
        if score > best_score:
            best_score = score
            best_name = text
    return best_name


def _normalize_launch_status(value: Any) -> str | None:
    text = (_as_text(value) or "").lower().replace("_", "-")
    text = re.sub(r"\s+", "-", text)
    if not text:
        return None
    if "ready-to-move" in text or "ready-for-occupancy" in text:
        return "ready-to-move"
    if "under-construction" in text:
        return "under-construction"
    if "pre-launch" in text:
        return "pre-launch"
    if "new-launch" in text:
        return "new-launch"
    if "upcoming" in text:
        return "upcoming"
    return None


def _infer_country(city: str | None, state: str | None) -> str | None:
    c = (city or "").strip().lower()
    s = (state or "").strip().lower()
    if c in UAE_CITY_HINTS:
        return "UAE"
    if c in KSA_CITY_HINTS or s in {"riyadh", "makkah", "medina", "eastern province", "saudi arabia"}:
        return "KSA"
    if c in INDIA_CITY_HINTS or s in {"kerala", "karnataka", "maharashtra", "delhi", "tamil nadu"}:
        return "India"
    return None


def _auto_discovery_cities_from_project_sections(config_obj: Any) -> list[str]:
    prefix = "property_projects."
    try:
        section_names = config_obj.sections()
    except Exception:
        return []

    discovered: list[str] = []
    for section_name in section_names:
        section_text = str(section_name).strip()
        if not section_text.lower().startswith(prefix):
            continue

        section = _get_section(config_obj, section_text)
        city_name = _as_text(_cfg_get(section, "city", ""))
        if city_name:
            discovered.append(city_name)
            continue

        city_key = section_text[len(prefix) :].strip()
        if not city_key:
            continue
        fallback_city = re.sub(r"[_-]+", " ", city_key).strip().title()
        if fallback_city:
            discovered.append(fallback_city)

    return _clean_list(discovered)


def _resolve_canonical_value(
    resolver: CanonicalResolver | None,
    dimension: str,
    value: Any,
    *,
    context: dict[str, Any] | None = None,
    country_guardrail: str | None = None,
    source_index: str | None = None,
    source_system: str | None = None,
    evidence_url: str | None = None,
    allow_ai: bool = True,
) -> str | None:
    text = _as_text(value)
    if not text:
        return None
    if resolver is None:
        return text
    try:
        resolved = resolver.resolve(
            dimension=dimension,
            value=text,
            context=context,
            country_guardrail=country_guardrail,
            source_index=source_index,
            source_system=source_system,
            evidence_url=evidence_url,
            allow_ai=allow_ai,
        )
        canonical = _as_text((resolved or {}).get("canonical_value"))
        return canonical or text
    except Exception as exc:
        logger.debug("Canonical resolve failed for dimension=%s value=%s: %s", dimension, text, exc)
        return text


def _preferred_website(urls: list[str], builder_slug: str, builder_name: str | None = None) -> str | None:
    candidates = _sanitize_known_websites(urls)
    for url in candidates:
        if _looks_like_official_site(url, builder_slug):
            return url
    scored = sorted(
        ((url, _website_candidate_confidence(url, builder_slug, builder_name)) for url in candidates),
        key=lambda item: item[1],
        reverse=True,
    )
    for url, confidence in scored:
        if confidence >= 0.7:
            return url
    return None


def _merge_min(existing: Any, incoming: Any) -> int | None:
    e = _safe_int(existing)
    i = _safe_int(incoming)
    if i is None:
        return e
    if e is None:
        return i
    return min(e, i)


def _merge_max(existing: Any, incoming: Any) -> int | None:
    e = _safe_int(existing)
    i = _safe_int(incoming)
    if i is None:
        return e
    if e is None:
        return i
    return max(e, i)


def _compute_data_quality(profile: dict[str, Any]) -> str:
    checks = [
        _has_value(profile.get("website")),
        _has_value(profile.get("head_office_city")),
        _has_value(profile.get("founders")),
        _safe_int(profile.get("founded_year")) is not None,
        bool(_clean_list(profile.get("operating_cities"))),
        _has_value(profile.get("category")),
        _safe_int(profile.get("total_projects")) is not None,
        bool(_clean_list(profile.get("sources"))),
    ]
    score = sum(1 for item in checks if item)
    if score >= 6:
        return "high"
    if score >= 3:
        return "medium"
    return "low"


def _strip_bom_and_fences(payload: str) -> str:
    text = payload.lstrip("\ufeff").strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_-]*\s*\n?", "", text, count=1)
        if text.endswith("```"):
            text = text[:-3].rstrip()
    text = re.sub(r"^\)\]\}',?\s*\n", "", text)
    return text


def _find_first_json_block(payload: str) -> str | None:
    text = payload.strip()
    start = None
    depth = 0
    in_str = False
    esc = False
    quote = ""
    for idx, ch in enumerate(text):
        if start is None:
            if ch in "[{":
                start = idx
                depth = 1
                continue
        else:
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
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
                        return text[start : idx + 1]
    return None


def parse_llm_json(payload: str) -> Any:
    text = _strip_bom_and_fences(payload)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    if json_repair is not None:
        try:
            return json_repair.loads(text)
        except Exception:
            pass
    if rapidjson is not None:
        try:
            return rapidjson.loads(
                text,
                parse_mode=getattr(rapidjson, "PM_COMMENTS", 0)
                | getattr(rapidjson, "PM_TRAILING_COMMAS", 0),
            )
        except Exception:
            pass
    if dirtyjson is not None:
        try:
            return dirtyjson.loads(text, search_for_first_object=True)
        except Exception:
            pass
    block = _find_first_json_block(text)
    if block:
        try:
            return parse_llm_json(block)
        except Exception:
            pass
    text = re.sub(r"\bNone\b", "null", text)
    text = re.sub(r"\bTrue\b", "true", text)
    text = re.sub(r"\bFalse\b", "false", text)
    text = "".join(ch for ch in text if (ord(ch) >= 32 or ch in "\n\r\t"))
    return json.loads(text)


def ai_extract_builder_profile(builder_name: str, context_text: str) -> dict[str, Any]:
    if not context_text.strip():
        return {}
    system_prompt = (
        "You are an extraction engine. Return ONE compact JSON object only. "
        "No markdown, no code fences, no explanatory text."
    )
    user_prompt = (
        f"{BUILDER_PROFILE_EXTRACT_PROMPT}\n\n"
        f"Builder to profile: {builder_name}\n\n"
        f"Source text:\n{context_text[:12000]}"
    )
    try:
        raw = call_llm(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            json_mode=True,
            temperature=0.0,
            max_tokens=2200,
        )
        parsed = parse_llm_json(raw)
        if isinstance(parsed, dict):
            return parsed
    except Exception as exc:
        logger.warning("Builder profile AI extraction failed for %s: %s", builder_name, exc)
    return {}


def _decode_ddg_url(raw_url: str) -> str | None:
    url = html.unescape(raw_url.strip())
    if not url:
        return None
    if url.startswith("//"):
        url = f"https:{url}"
    if "duckduckgo.com/l/?" in url or url.startswith("/l/?"):
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        uddg = query.get("uddg", [])
        if uddg:
            decoded = unquote(uddg[0]).strip()
            return decoded if decoded.startswith("http") else None
        if "uddg=" in url:
            encoded = url.split("uddg=", 1)[1].split("&", 1)[0]
            decoded = unquote(encoded).strip()
            return decoded if decoded.startswith("http") else None
    if url.startswith("http"):
        return url
    return None


def _html_to_text(html_text: str) -> str:
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html_text, flags=re.S | re.I)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text).replace("\xa0", " ")
    return _normalize_space(text)


def parse_duckduckgo_results(html_text: str) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    anchor_pattern = re.compile(r'<a[^>]+class="result__a"[^>]+href="([^"]+)"[^>]*>(.*?)</a>', re.I | re.S)
    for match in anchor_pattern.finditer(html_text):
        url = _decode_ddg_url(match.group(1))
        if not _is_valid_discovery_url(url):
            continue
        title = _normalize_space(re.sub(r"<[^>]+>", " ", html.unescape(match.group(2))))
        window = html_text[match.end() : match.end() + 1000]
        snippet_match = re.search(r'class="result__snippet"[^>]*>(.*?)</(?:a|div)>', window, re.I | re.S)
        snippet = ""
        if snippet_match:
            snippet = _normalize_space(re.sub(r"<[^>]+>", " ", html.unescape(snippet_match.group(1))))
        results.append({"title": title, "url": url, "snippet": snippet, "source": "duckduckgo"})

    if not results:
        hrefs = re.findall(r'href="([^"]+)"', html_text)
        for href in hrefs:
            url = _decode_ddg_url(href)
            if not _is_valid_discovery_url(url):
                continue
            results.append({"title": "", "url": url, "snippet": "", "source": "duckduckgo"})

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for result in results:
        key = result["url"].strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(result)
    return deduped


def parse_brave_results(html_text: str) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    pattern = re.compile(r'<a href="(https?://[^"]+)"[^>]*class="[^"]*\bl1\b[^"]*"[^>]*>(.*?)</a>', re.I | re.S)
    for match in pattern.finditer(html_text):
        url = _normalize_url(html.unescape(match.group(1)))
        if not _is_valid_discovery_url(url):
            continue
        title = _normalize_space(re.sub(r"<[^>]+>", " ", html.unescape(match.group(2))))
        results.append({"title": title, "url": str(url), "snippet": "", "source": "brave"})

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for result in results:
        key = result["url"].strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(result)
    return deduped


def _city_slug(value: Any) -> str:
    text = (_as_text(value) or "").lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text


def _realestateindia_city_slug(city: str | None) -> str:
    slug = _city_slug(city)
    if not slug:
        return ""
    return REALESTATEINDIA_BUILDER_CITY_SLUG_OVERRIDES.get(slug, slug)


def _realestateindia_city_slug_candidates(city: str | None) -> list[str]:
    slug = _city_slug(city)
    if not slug:
        return []

    aliases = REALESTATEINDIA_BUILDER_CITY_SLUG_ALIASES.get(slug)
    candidates: list[str] = []
    if aliases:
        candidates.extend(aliases)
    else:
        candidates.append(_realestateindia_city_slug(city))
        candidates.append(slug)
    return _clean_list(candidates)


def _realestateindia_city_tokens(city: str | None) -> set[str]:
    tokens: set[str] = set()
    city_text = (_as_text(city) or "").lower()
    if city_text:
        tokens.add(city_text)

    slug = _city_slug(city)
    rei_slug = _realestateindia_city_slug(city)
    for value in {slug, rei_slug}:
        if not value:
            continue
        tokens.add(value)
        tokens.add(value.replace("-", " "))
        for part in value.split("-"):
            if len(part) >= 3:
                tokens.add(part)

    if slug == "bengaluru" or rei_slug == "bangalore":
        tokens.update({"bengaluru", "bangalore"})

    if slug == "thiruvananthapuram" or rei_slug == "trivandrum":
        tokens.update({"thiruvananthapuram", "trivandrum"})

    if slug == "kozhikode" or rei_slug == "calicut":
        tokens.update({"kozhikode", "calicut"})

    return {token for token in tokens if token}


def _is_realestateindia_builder_city_match(result: dict[str, Any], city: str) -> bool:
    tokens = _realestateindia_city_tokens(city)
    if not tokens:
        return True

    location_text = (_as_text(result.get("location")) or "").lower()
    if "also deals in" in location_text:
        return False

    haystacks = [
        _as_text(result.get("url")) or "",
        _as_text(result.get("location")) or "",
    ]
    for token in tokens:
        pattern = rf"\b{re.escape(token)}\b"
        for text in haystacks:
            if not text:
                continue
            if re.search(pattern, text, flags=re.I):
                return True
    return False


def _score_city_discovery_result(result: dict[str, Any], city: str) -> int:
    score = 0
    source = (_as_text(result.get("source")) or "").lower()
    domain = _extract_domain(result.get("url"))
    title = (_as_text(result.get("title")) or "").lower()
    snippet = (_as_text(result.get("snippet")) or "").lower()
    combined = f"{title} {snippet}".strip()

    score += DISCOVERY_SOURCE_WEIGHTS.get(source, 10)

    if source == REALESTATEINDIA_BUILDER_SOURCE:
        score += 35
        if _is_realestateindia_builder_city_match(result, city):
            score += 25
        else:
            score -= 60

    if domain:
        if any(domain == blocked or domain.endswith(f".{blocked}") for blocked in CITY_DISCOVERY_NOISE_DOMAINS):
            score -= 80
        root = _domain_root(domain)
        if root and root not in {"duckduckgo", "bing", "brave", "realestateindia", "wikipedia"}:
            score += 8

    if combined:
        if any(phrase in combined for phrase in CITY_DISCOVERY_HINT_PHRASES):
            score += 12
        if "official" in combined and "website" in combined:
            score += 8
    if _builder_name_from_domain(result.get("url") or ""):
        score += 10
    if _is_candidate_builder_website(result.get("url")):
        score += 12
    return score


def _dedupe_discovery_results(results: list[dict[str, str]]) -> list[dict[str, str]]:
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for result in results:
        url = _normalize_url(result.get("url"))
        if url:
            key = f"url:{url.lower().rstrip('/')}"
        else:
            key = f"name:{(_normalize_builder_name(result.get('builder_name')) or _as_text(result.get('title')) or '').lower()}"
        if not key or key in seen:
            continue
        seen.add(key)
        out = dict(result)
        if url:
            out["url"] = url
        deduped.append(out)
    return deduped


def _prioritize_city_discovery_results(results: list[dict[str, str]], city: str, *, limit: int) -> list[dict[str, str]]:
    scored: list[tuple[int, dict[str, str]]] = []
    for result in _dedupe_discovery_results(results):
        score = _score_city_discovery_result(result, city)
        if score <= 0:
            continue
        scored.append((score, result))

    scored.sort(
        key=lambda item: (
            -item[0],
            str(item[1].get("builder_name") or item[1].get("title") or "").lower(),
        )
    )
    return [item[1] for item in scored[: max(1, limit)]]


def _extract_realestateindia_load_more_payload(html_text: str, listing_url: str) -> dict[str, str] | None:
    match = re.search(
        r"\$\.post\(\s*['\"](?:https?:)?/?Functions/fetch_service_classified_results\.php['\"]\s*,\s*\{(.*?)\}\s*,\s*function\s*\(\s*data\s*\)\s*\{",
        html_text,
        re.I | re.S,
    )
    if not match:
        return None

    payload_raw = match.group(1)
    pairs: dict[str, str] = {}
    for key, value in re.findall(r"'([^']+)'\s*:\s*'([^']*)'", payload_raw):
        pairs[key.strip()] = value.strip()
    if not pairs:
        for key, value in re.findall(r'"([^"]+)"\s*:\s*"([^"]*)"', payload_raw):
            pairs[key.strip()] = value.strip()
    if not pairs:
        return None

    if not pairs.get("location"):
        pairs["location"] = listing_url
    pairs.setdefault("people_also_search_for", "")
    pairs.setdefault("people_also_search_cat", "")
    return pairs


def parse_realestateindia_builder_results(html_text: str) -> list[dict[str, str]]:
    if not html_text:
        return []

    results: list[dict[str, str]] = []
    seen: set[str] = set()
    base_url = "https://www.realestateindia.com"
    item_pattern = re.compile(r"<li\b[^>]*>(.*?)</li>", re.I | re.S)

    for item_match in item_pattern.finditer(html_text):
        item_html = item_match.group(1)

        anchor_match = re.search(
            r'<a[^>]+href=["\']([^"\']*?/profile/[^"\']+)["\'][^>]*>\s*<h2[^>]*>(.*?)</h2>\s*</a>',
            item_html,
            re.I | re.S,
        )
        if anchor_match:
            href = html.unescape(anchor_match.group(1) or "").strip()
            raw_name = _normalize_space(re.sub(r"<[^>]+>", " ", html.unescape(anchor_match.group(2) or "")))
        else:
            data_url_match = re.search(
                r'<div[^>]+class=["\'][^"\']*agent_item[^"\']*["\'][^>]*\sdata-url=["\']([^"\']+)["\']',
                item_html,
                re.I | re.S,
            )
            name_match = re.search(r"<h2[^>]*>(.*?)</h2>", item_html, re.I | re.S)
            href = html.unescape(data_url_match.group(1) or "").strip() if data_url_match else ""
            raw_name = _normalize_space(re.sub(r"<[^>]+>", " ", html.unescape(name_match.group(1) or ""))) if name_match else ""

        if not href:
            continue
        profile_url = _normalize_url(urljoin(base_url, href))
        if not profile_url:
            continue

        builder_name = _normalize_builder_name(raw_name)
        if not builder_name or not _looks_like_builder_name(builder_name, strict=True):
            continue

        location = ""
        location_match = re.search(
            r'<div[^>]+class=["\'][^"\']*location-dc[^"\']*["\'][^>]*>(.*?)</div>',
            item_html,
            re.I | re.S,
        )
        if location_match:
            location = _normalize_space(re.sub(r"<[^>]+>", " ", html.unescape(location_match.group(1))))

        about = ""
        about_match = re.search(
            r'<div[^>]+class=["\'][^"\']*cssc-about[^"\']*["\'][^>]*>(.*?)</div>',
            item_html,
            re.I | re.S,
        )
        if about_match:
            about = _normalize_space(re.sub(r"<[^>]+>", " ", html.unescape(about_match.group(1))))

        website = None
        website_match = re.search(r"window\.open\('([^']+)'\s*,\s*'_blank'\)", item_html, re.I)
        if website_match:
            website = _normalize_url(website_match.group(1))
        if not website:
            website_text_match = re.search(
                r'<a[^>]+class=["\'][^"\']*web_link[^"\']*["\'][^>]*>.*?<span>(https?://[^<\s]+)</span>',
                item_html,
                re.I | re.S,
            )
            if website_text_match:
                website = _normalize_url(website_text_match.group(1))
        if website and (not _is_candidate_builder_website(website) or _looks_like_asset_url(website)):
            website = None

        snippet = _normalize_space(" ".join(part for part in (location, about) if part))
        key = (profile_url or builder_name).lower()
        if not key or key in seen:
            continue
        seen.add(key)

        results.append(
            {
                "title": builder_name,
                "builder_name": builder_name,
                "url": profile_url,
                "snippet": snippet,
                "location": location,
                "source": REALESTATEINDIA_BUILDER_SOURCE,
                "website": website,
            }
        )

    return results


def _search_realestateindia_builders(session: requests.Session, city: str) -> list[dict[str, str]]:
    country_hint = _infer_country(city, None)
    if country_hint and country_hint != "India":
        return []

    city_slugs = _realestateindia_city_slug_candidates(city)
    if not city_slugs:
        return []

    all_results: list[dict[str, str]] = []
    fetched_slug = None
    last_exc = None

    for city_slug in city_slugs:
        listing_url = f"https://www.realestateindia.com/builders-developers-in-{city_slug}.htm"
        try:
            html_text = _fetch(session, listing_url, retries=2)
        except Exception as exc:
            last_exc = exc
            continue

        fetched_slug = city_slug
        results = parse_realestateindia_builder_results(html_text)
        results = [item for item in results if _is_realestateindia_builder_city_match(item, city)]
        payload = _extract_realestateindia_load_more_payload(html_text, listing_url)
        if not payload:
            all_results = results
            break

        total_pages_match = re.search(r"var\s+total_pages\s*=\s*(\d+)", html_text, re.I)
        total_pages = _safe_int(total_pages_match.group(1)) if total_pages_match else None
        max_pages = max(1, SETTINGS.realestateindia_max_load_more_pages)
        if total_pages:
            max_pages = min(max_pages, int(total_pages))

        endpoint = urljoin(listing_url, "/Functions/fetch_service_classified_results.php")
        seen: set[str] = set()
        for item in results:
            key = (_normalize_url(item.get("url")) or _as_text(item.get("builder_name")) or "").lower()
            if key:
                seen.add(key)

        stale_pages = 0
        for page in range(1, max_pages + 1):
            post_data = dict(payload)
            post_data["pageno"] = str(page)
            try:
                fragment_html = _post(session, endpoint, post_data, retries=1, referer=listing_url)
            except Exception as exc:
                logger.warning(
                    "RealEstateIndia load-more failed for city=%s page=%d slug=%s: %s",
                    city,
                    page,
                    city_slug,
                    exc,
                )
                break

            page_results = parse_realestateindia_builder_results(fragment_html)
            page_results = [item for item in page_results if _is_realestateindia_builder_city_match(item, city)]
            new_items = 0
            for result in page_results:
                key = (_normalize_url(result.get("url")) or _as_text(result.get("builder_name")) or "").lower()
                if not key or key in seen:
                    continue
                seen.add(key)
                results.append(result)
                new_items += 1

            logger.info(
                "RealEstateIndia builders city=%s page=%d slug=%s extracted %d entries (%d new)",
                city,
                page,
                city_slug,
                len(page_results),
                new_items,
            )

            if new_items == 0:
                stale_pages += 1
            else:
                stale_pages = 0

            if stale_pages >= 2:
                break

            if page < max_pages:
                time.sleep(random.uniform(SETTINGS.min_delay_seconds, SETTINGS.max_delay_seconds))

        all_results = results
        if all_results:
            break

    if not all_results and last_exc is not None:
        logger.warning("RealEstateIndia builders listing failed for city=%s (tried slugs=%s): %s", city, city_slugs, last_exc)
        return []

    if fetched_slug and city_slugs and fetched_slug != city_slugs[0]:
        logger.info(
            "RealEstateIndia builders city=%s recovered via alternate slug '%s' (primary='%s')",
            city,
            fetched_slug,
            city_slugs[0],
        )

    return _prioritize_city_discovery_results(all_results, city, limit=max(120, len(all_results)))


def _extract_builder_names_from_text(text: str) -> list[str]:
    if not text:
        return []
    cleaned = _normalize_space(re.sub(r"\s+", " ", html.unescape(text)))
    candidates: list[str] = []
    patterns = (
        r"\b([A-Z][A-Za-z0-9&.\-']{1,40}(?:\s+[A-Z][A-Za-z0-9&.\-']{1,40}){0,5}\s+(?:Builders?|Developers?|Properties|Realty|Realtors?|Constructions?|Infra|Group|Homes))\b",
        r"\bby\s+([A-Z][A-Za-z0-9&.\-']{1,40}(?:\s+[A-Z][A-Za-z0-9&.\-']{1,40}){0,5}\s+(?:Builders?|Developers?|Properties|Realty|Realtors?|Group|Homes))\b",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, cleaned):
            name = _normalize_builder_name(match.group(1))
            if name and _looks_like_builder_name(name, strict=True):
                candidates.append(name)
    return _clean_list(candidates)


def _builder_name_from_domain(url: str) -> str | None:
    domain = _extract_domain(url)
    if not domain:
        return None
    parts = domain.split(".")
    if len(parts) < 2:
        return None
    base = parts[-2]
    if len(parts) >= 3 and parts[-2] in {"co", "com", "org", "net"}:
        base = parts[-3]
    if not any(token in base for token in ("builder", "develop", "propert", "realty", "realtor", "infra", "homes", "group")):
        return None

    suffixes = (
        "constructions",
        "construction",
        "developers",
        "developer",
        "properties",
        "property",
        "realtors",
        "realtor",
        "builders",
        "builder",
        "realty",
        "homes",
        "group",
        "infra",
    )

    compact = re.sub(r"[^a-z0-9]+", "", base.lower())
    guess = re.sub(r"[^a-zA-Z0-9]+", " ", base).strip()
    if " " not in guess:
        for suffix in suffixes:
            if not compact.endswith(suffix):
                continue
            prefix = compact[: -len(suffix)].strip()
            if len(prefix) < 3:
                continue
            guess = f"{prefix} {suffix}"
            break

    guess = _normalize_builder_name(guess.title())
    if guess and not _looks_like_builder_name(guess, strict=True):
        return None
    return guess


def _city_query_variants(city: str) -> list[str]:
    base = _as_text(city) or ""
    slug = _city_slug(city)
    variants: list[str] = [base]
    if slug in {"bengaluru", "bangalore"}:
        variants.extend(["Bengaluru", "Bangalore"])
    elif slug in {"thiruvananthapuram", "trivandrum"}:
        variants.extend(["Thiruvananthapuram", "Trivandrum"])
    elif slug in {"kozhikode", "calicut"}:
        variants.extend(["Kozhikode", "Calicut"])
    return _clean_list(variants)


def _search_city_on_bing_rss(session: requests.Session, query: str) -> list[dict[str, str]]:
    url = f"https://www.bing.com/search?q={quote_plus(query)}&format=rss&setlang=en"
    try:
        xml_text = _fetch(session, url, retries=1)
    except Exception as exc:
        logger.warning("Bing RSS city discovery failed for query=%s: %s", query, exc)
        return []

    results: list[dict[str, str]] = []
    try:
        root = ET.fromstring(xml_text)
    except Exception as exc:
        logger.warning("Failed to parse Bing RSS city discovery for query=%s: %s", query, exc)
        return []

    for item in root.findall("./channel/item"):
        link = _normalize_url(item.findtext("link"))
        if not _is_valid_discovery_url(link):
            continue
        title = _normalize_space(_as_text(item.findtext("title")) or "")
        snippet = _normalize_space(_as_text(item.findtext("description")) or "")
        results.append({"title": title, "url": str(link), "snippet": snippet, "source": "bing_city"})

    return _dedupe_discovery_results(results)


def _search_city_on_brave(session: requests.Session, query: str) -> list[dict[str, str]]:
    url = f"https://search.brave.com/search?q={quote_plus(query)}&source=web"
    try:
        html_text = _fetch(session, url, retries=1)
    except Exception as exc:
        logger.warning("Brave city discovery failed for query=%s: %s", query, exc)
        return []

    parsed = parse_brave_results(html_text)
    out: list[dict[str, str]] = []
    for item in parsed:
        row = dict(item)
        row["source"] = "brave_city"
        out.append(row)
    return _dedupe_discovery_results(out)


def _discover_city_web_results(session: requests.Session, city: str, query: str, page: int) -> list[dict[str, str]]:
    offset = page * 10
    search_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}&s={offset}"
    ddg_results: list[dict[str, str]] = []
    try:
        html_text = _fetch(session, search_url, retries=2)
        ddg_results = parse_duckduckgo_results(html_text)
    except Exception as exc:
        logger.warning("DDG city discovery failed for '%s' page=%d: %s", query, page + 1, exc)

    if len(ddg_results) >= CITY_SEARCH_FALLBACK_MIN_DDG_RESULTS:
        return _prioritize_city_discovery_results(ddg_results, city, limit=CITY_SEARCH_FALLBACK_RESULT_LIMIT)

    fallback_results: list[dict[str, str]] = list(ddg_results)
    fallback_results.extend(_search_city_on_bing_rss(session, query))
    if len(fallback_results) < CITY_SEARCH_FALLBACK_RESULT_LIMIT:
        fallback_results.extend(_search_city_on_brave(session, query))
    return _prioritize_city_discovery_results(fallback_results, city, limit=CITY_SEARCH_FALLBACK_RESULT_LIMIT)


def _seed_priority_score(seed: dict[str, Any]) -> int:
    score = 0

    total_projects = int(seed.get("total_projects") or 0)
    score += min(total_projects, 150) * 6

    source_names = _clean_list(seed.get("source_names"))
    for source_name in source_names:
        score += DISCOVERY_SOURCE_WEIGHTS.get(source_name.lower(), 8)

    if _clean_list(seed.get("known_websites")):
        score += 28
    if _has_value(seed.get("website")):
        score += 24

    rera_count = len(_clean_list(seed.get("rera_numbers")))
    if _has_value(seed.get("rera_reg_no")):
        rera_count += 1
    score += min(rera_count, 3) * 10

    city_count = len(_clean_list(seed.get("operating_cities")))
    score += min(city_count, 5) * 4

    source_url_count = len(_clean_list(seed.get("sources")))
    score += min(source_url_count, 12) * 2

    data_quality = (_as_text(seed.get("data_quality")) or "").lower()
    if data_quality == "high":
        score += 22
    elif data_quality == "medium":
        score += 12

    return score


def _normalize_property_types(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        value = [value]
    out: list[str] = []
    for item in value:
        text = (_as_text(item) or "").lower()
        if not text:
            continue
        for part in re.split(r"[,;/|&+]", text):
            part = part.strip()
            if not part:
                continue
            if "apartment" in part or "flat" in part:
                out.append("apartment")
            elif "villa" in part:
                out.append("villa")
            elif "town" in part:
                out.append("townhouse")
            elif "plot" in part:
                out.append("plotted-development")
            elif "studio" in part:
                out.append("studio")
            elif "commercial" in part:
                out.append("commercial")
            elif "office" in part:
                out.append("office")
            elif "retail" in part:
                out.append("retail")
            else:
                out.append(part)
    return _clean_list(out)


def _category_from_specializations(specializations: list[str]) -> str | None:
    specs = {s.lower() for s in specializations}
    residential = bool(specs & RESIDENTIAL_TYPES)
    commercial = bool(specs & COMMERCIAL_TYPES)
    if residential and commercial:
        return "mixed-use"
    if residential:
        return "residential"
    if commercial:
        return "commercial"
    return None


def _saved_data_dir() -> Path:
    SETTINGS.data_dir.mkdir(parents=True, exist_ok=True)
    return SETTINGS.data_dir


def _build_http_client() -> requests.Session:
    return requests.Session()


def _session_get(session: requests.Session, url: str, timeout: int):
    headers = dict(REQUEST_HEADERS)
    headers["User-Agent"] = random.choice(USER_AGENTS)
    return session.get(url, headers=headers, timeout=timeout)


def _session_post(
    session: requests.Session,
    url: str,
    payload: dict[str, Any],
    *,
    timeout: int,
    referer: str | None = None,
):
    headers = dict(REQUEST_HEADERS)
    headers["User-Agent"] = random.choice(USER_AGENTS)
    headers["X-Requested-With"] = "XMLHttpRequest"
    if referer:
        headers["Referer"] = referer
    parsed = urlparse(url)
    if parsed.scheme and parsed.netloc:
        headers["Origin"] = f"{parsed.scheme}://{parsed.netloc}"
    return session.post(url, data=payload, headers=headers, timeout=timeout)


def _looks_blocked(text: str) -> bool:
    lower = text.lower()
    return any(marker in lower for marker in BLOCKED_MARKERS)


def _fetch(session: requests.Session, url: str, *, retries: int = 1, timeout: int | None = None) -> str:
    tout = timeout or SETTINGS.request_timeout_seconds
    last_exc = None
    for attempt in range(retries):
        try:
            response = _session_get(session, url, timeout=tout)
            response.raise_for_status()
            text = response.text
            if _looks_blocked(text):
                raise RuntimeError("Received a bot-protection response")
            return text
        except Exception as exc:
            last_exc = exc
            sleep_for = SETTINGS.min_delay_seconds * (attempt + 1)
            logger.warning("Fetch failed for %s (%s). Retrying in %.1fs", url, exc, sleep_for)
            time.sleep(sleep_for)
    raise RuntimeError(f"Failed to fetch {url}: {last_exc}")


def _post(
    session: requests.Session,
    url: str,
    payload: dict[str, Any],
    *,
    retries: int = 1,
    timeout: int | None = None,
    referer: str | None = None,
) -> str:
    tout = timeout or SETTINGS.request_timeout_seconds
    last_exc = None
    for attempt in range(retries):
        try:
            response = _session_post(session, url, payload, timeout=tout, referer=referer)
            response.raise_for_status()
            text = response.text
            if _looks_blocked(text):
                raise RuntimeError("Received a bot-protection response")
            return text
        except Exception as exc:
            last_exc = exc
            sleep_for = SETTINGS.min_delay_seconds * (attempt + 1)
            logger.warning("POST failed for %s (%s). Retrying in %.1fs", url, exc, sleep_for)
            time.sleep(sleep_for)
    raise RuntimeError(f"Failed POST to {url}: {last_exc}")


def es_client() -> Elasticsearch:
    es = Elasticsearch(
        hosts=ES_HOSTS,
        http_auth=(ES_USER, ES_PASSWORD),
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


def build_canonical_resolver(es: Elasticsearch) -> CanonicalResolver | None:
    if not SETTINGS.canonical_enabled:
        return None
    return CanonicalResolver(
        es,
        index=SETTINGS.canonical_index,
        max_ai_calls=SETTINGS.canonical_max_ai_calls,
        ai_enabled=SETTINGS.canonical_ai_enabled,
    )


def ensure_index(es: Elasticsearch, index: str) -> None:
    if not es.indices.exists(index=index):
        es.indices.create(index=index, body=BUILDER_PROFILE_INDEX_MAPPING)


def _iter_project_builder_docs(es: Elasticsearch, index: str) -> list[dict[str, Any]]:
    try:
        if not es.indices.exists(index=index):
            logger.info("Index %s not found; skipping", index)
            return []
    except Exception as exc:
        logger.warning("Failed to verify index %s: %s", index, exc)
        return []

    query = {
        "query": {"bool": {"must": [{"exists": {"field": "builder_name"}}]}},
        "_source": [
            "id",
            "builder_name",
            "builder_url",
            "project_name",
            "project_url",
            "source_url",
            "source",
            "city",
            "state",
            "target_city",
            "launch_status",
            "property_types",
            "price_min",
            "price_max",
            "price_per_sqft",
            "builder_tier",
            "rera_number",
            "updated_at",
        ],
    }
    try:
        return [
            doc.get("_source", {})
            for doc in helpers.scan(es, index=index, query=query, preserve_order=False, size=1000)
        ]
    except Exception as exc:
        logger.warning("Failed to scan index %s: %s", index, exc)
        return []


def _new_seed(builder_name: str) -> dict[str, Any]:
    return {
        "builder_name": builder_name,
        "_name_score": _builder_name_score(builder_name),
        "_discovery_order": None,
        "aliases": [],
        "known_websites": [],
        "operating_cities": [],
        "operating_states": [],
        "operating_countries": [],
        "specializations": [],
        "category": None,
        "builder_tier": None,
        "key_projects": [],
        "recent_launches": [],
        "source_names": [],
        "sources": [],
        "rera_reg_no": None,
        "rera_numbers": [],
        "price_range_min": None,
        "price_range_max": None,
        "price_per_sqft_min": None,
        "price_per_sqft_max": None,
        "total_projects": 0,
        "total_projects_completed": 0,
        "total_projects_ongoing": 0,
        "_project_ids": set(),
        "_completed_ids": set(),
        "_ongoing_ids": set(),
        "_city_counts": {},
        "_tier_counts": {},
    }


def _merge_seed(seed: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
    merged = dict(seed)
    for key, value in update.items():
        if key in PROFILE_LIST_FIELDS:
            merged[key] = _merge_lists(merged.get(key), value)
        elif key in MIN_FIELDS:
            merged[key] = _merge_min(merged.get(key), value)
        elif key in MAX_FIELDS:
            merged[key] = _merge_max(merged.get(key), value)
        elif key in {"_project_ids", "_completed_ids", "_ongoing_ids"}:
            existing_ids = merged.get(key) or set()
            if not isinstance(existing_ids, set):
                existing_ids = set(existing_ids)
            incoming_ids = value if isinstance(value, set) else set(value or [])
            merged[key] = existing_ids | incoming_ids
        elif key == "_city_counts":
            counts = dict(merged.get(key) or {})
            for city, cnt in (value or {}).items():
                counts[city] = counts.get(city, 0) + int(cnt)
            merged[key] = counts
        elif key == "_tier_counts":
            counts = dict(merged.get(key) or {})
            for tier, cnt in (value or {}).items():
                counts[tier] = counts.get(tier, 0) + int(cnt)
            merged[key] = counts
        elif key == "_name_score":
            merged[key] = max(int(merged.get(key) or 0), int(value or 0))
        elif key in {"total_projects", "total_projects_completed", "total_projects_ongoing"}:
            merged[key] = max(int(merged.get(key) or 0), int(value or 0))
        else:
            if _has_value(value) and not _has_value(merged.get(key)):
                merged[key] = value
    return merged


def _serialize_for_json(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for record in records:
        clean: dict[str, Any] = {}
        for key, value in record.items():
            if isinstance(value, set):
                clean[key] = sorted(str(item) for item in value)
            else:
                clean[key] = value
        serialized.append(clean)
    return serialized


def _update_seed_from_project(
    seed: dict[str, Any],
    doc: dict[str, Any],
    source_index: str,
    *,
    resolver: CanonicalResolver | None = None,
    pre_canonical: dict[str, str | None] | None = None,
) -> dict[str, Any]:
    updated = dict(seed)
    pre = pre_canonical or {}
    source_system = _as_text(doc.get("source")) or source_index
    evidence_url = _normalize_url(doc.get("project_url") or doc.get("source_url"))

    raw_city = _as_text(doc.get("city") or doc.get("target_city"))
    raw_state = _as_text(doc.get("state"))
    raw_country = _as_text(doc.get("country")) or _infer_country(raw_city, raw_state)

    country = pre.get("country") or _resolve_canonical_value(
        resolver,
        "country",
        raw_country,
        context=None,
        source_index=source_index,
        source_system=source_system,
        evidence_url=evidence_url,
        allow_ai=False,
    )
    state = pre.get("state") or _resolve_canonical_value(
        resolver,
        "state",
        raw_state,
        context={"country": country},
        country_guardrail=country,
        source_index=source_index,
        source_system=source_system,
        evidence_url=evidence_url,
        allow_ai=False,
    )
    city = pre.get("city") or _resolve_canonical_value(
        resolver,
        "city",
        raw_city,
        context={"country": country, "state": state},
        country_guardrail=country,
        source_index=source_index,
        source_system=source_system,
        evidence_url=evidence_url,
        allow_ai=False,
    )

    raw_builder_name = _normalize_builder_name(doc.get("builder_name"))
    builder_name = pre.get("builder_name") or _resolve_canonical_value(
        resolver,
        "builder_name",
        raw_builder_name,
        context={"country": country, "state": state, "city": city},
        country_guardrail=country,
        source_index=source_index,
        source_system=source_system,
        evidence_url=evidence_url,
        allow_ai=True,
    )

    if builder_name and not _looks_like_builder_name(builder_name):
        builder_name = None

    if builder_name:
        current_name = _normalize_builder_name(updated.get("builder_name"))
        candidate_score = _builder_name_score(builder_name)
        current_score = int(updated.get("_name_score") or _builder_name_score(current_name))
        if not _has_value(current_name):
            updated["builder_name"] = builder_name
            updated["_name_score"] = candidate_score
        elif builder_name.lower() != str(current_name).lower():
            if candidate_score > current_score:
                updated["aliases"] = _merge_lists(updated.get("aliases"), [current_name])
                updated["builder_name"] = builder_name
                updated["_name_score"] = candidate_score
            else:
                updated["aliases"] = _merge_lists(updated.get("aliases"), [builder_name])

    if city:
        updated["operating_cities"] = _merge_lists(updated.get("operating_cities"), [city])
        city_counts = dict(updated.get("_city_counts") or {})
        city_counts[city] = city_counts.get(city, 0) + 1
        updated["_city_counts"] = city_counts

    if state:
        updated["operating_states"] = _merge_lists(updated.get("operating_states"), [state])

    if country:
        updated["operating_countries"] = _merge_lists(updated.get("operating_countries"), [country])

    website = _normalize_url(doc.get("builder_url"))
    if website:
        slug = _builder_slug(_as_text(updated.get("builder_name")) or builder_name or "builder")
        confidence = _website_candidate_confidence(website, slug, _as_text(updated.get("builder_name")) or builder_name)
        if confidence >= 0.7:
            updated["known_websites"] = _merge_lists(updated.get("known_websites"), [website])
        if not _looks_like_official_site(website, slug):
            updated["sources"] = _merge_lists(updated.get("sources"), [website])

    raw_project_name = _as_text(doc.get("project_name"))
    project_name = _resolve_canonical_value(
        resolver,
        "project_name",
        raw_project_name,
        context={
            "country": country,
            "state": state,
            "city": city,
            "builder_name": _as_text(updated.get("builder_name")) or builder_name,
            "builder_key": _builder_slug(_as_text(updated.get("builder_name")) or builder_name or ""),
        },
        country_guardrail=country,
        source_index=source_index,
        source_system=source_system,
        evidence_url=evidence_url,
        allow_ai=True,
    )
    if project_name:
        updated["key_projects"] = _merge_lists(updated.get("key_projects"), [project_name])

    status = _normalize_launch_status(doc.get("launch_status"))
    if status in {"pre-launch", "new-launch", "under-construction", "upcoming"} and project_name:
        updated["recent_launches"] = _merge_lists(updated.get("recent_launches"), [project_name])

    source_url = _normalize_url(doc.get("project_url") or doc.get("source_url"))
    if source_url:
        updated["sources"] = _merge_lists(updated.get("sources"), [source_url])
    updated["source_names"] = _merge_lists(updated.get("source_names"), [source_index, source_system])

    project_id = _as_text(doc.get("id")) or project_name
    if project_id:
        project_ids = set(updated.get("_project_ids") or set())
        project_ids.add(project_id)
        updated["_project_ids"] = project_ids
        if status == "ready-to-move":
            completed_ids = set(updated.get("_completed_ids") or set())
            completed_ids.add(project_id)
            updated["_completed_ids"] = completed_ids
        else:
            ongoing_ids = set(updated.get("_ongoing_ids") or set())
            ongoing_ids.add(project_id)
            updated["_ongoing_ids"] = ongoing_ids

    property_types = _normalize_property_types(doc.get("property_types"))
    if property_types:
        updated["specializations"] = _merge_lists(updated.get("specializations"), property_types)
        category = _category_from_specializations(_clean_list(updated.get("specializations")))
        if category:
            updated["category"] = category

    tier = _as_text(doc.get("builder_tier"))
    if tier:
        tier_key = tier.lower()
        tier_counts = dict(updated.get("_tier_counts") or {})
        tier_counts[tier_key] = tier_counts.get(tier_key, 0) + 1
        updated["_tier_counts"] = tier_counts

    rera_number = _as_text(doc.get("rera_number"))
    if rera_number:
        updated["rera_numbers"] = _merge_lists(updated.get("rera_numbers"), [rera_number])
        if not updated.get("rera_reg_no"):
            updated["rera_reg_no"] = rera_number

    updated["price_range_min"] = _merge_min(updated.get("price_range_min"), doc.get("price_min"))
    updated["price_range_max"] = _merge_max(updated.get("price_range_max"), doc.get("price_max"))
    updated["price_per_sqft_min"] = _merge_min(updated.get("price_per_sqft_min"), doc.get("price_per_sqft"))
    updated["price_per_sqft_max"] = _merge_max(updated.get("price_per_sqft_max"), doc.get("price_per_sqft"))
    updated["price_currency"] = "INR"

    updated["total_projects"] = len(set(updated.get("_project_ids") or set()))
    updated["total_projects_completed"] = len(set(updated.get("_completed_ids") or set()))
    updated["total_projects_ongoing"] = len(set(updated.get("_ongoing_ids") or set()))

    return updated


def _finalize_seed(seed: dict[str, Any]) -> dict[str, Any]:
    out = dict(seed)
    city_counts = out.get("_city_counts") or {}
    if city_counts and not out.get("main_operating_city"):
        out["main_operating_city"] = sorted(city_counts.items(), key=lambda kv: (-kv[1], kv[0].lower()))[0][0]

    tier_counts = out.get("_tier_counts") or {}
    if tier_counts and not out.get("builder_tier"):
        out["builder_tier"] = sorted(tier_counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]

    if not out.get("category"):
        out["category"] = _category_from_specializations(_clean_list(out.get("specializations")))

    out["aliases"] = [a for a in _clean_list(out.get("aliases")) if a.lower() != str(out.get("builder_name", "")).lower()]
    out["known_websites"] = _sanitize_known_websites(out.get("known_websites"))
    out["operating_cities"] = _clean_list(out.get("operating_cities"))
    out["operating_states"] = _clean_list(out.get("operating_states"))
    out["operating_countries"] = _clean_list(out.get("operating_countries"))
    out["specializations"] = _clean_list(out.get("specializations"))
    out["key_projects"] = _clean_list(out.get("key_projects"))[:40]
    out["recent_launches"] = _clean_list(out.get("recent_launches"))[:30]
    out["source_names"] = _clean_list(out.get("source_names"))
    out["sources"] = _clean_list(out.get("sources"))
    out["rera_numbers"] = _clean_list(out.get("rera_numbers"))
    if not out.get("rera_reg_no") and out["rera_numbers"]:
        out["rera_reg_no"] = out["rera_numbers"][0]

    out.pop("_project_ids", None)
    out.pop("_completed_ids", None)
    out.pop("_ongoing_ids", None)
    out.pop("_city_counts", None)
    out.pop("_tier_counts", None)
    out.pop("_name_score", None)
    return out


def _aggregate_builder_seeds_from_es(
    es: Elasticsearch,
    *,
    resolver: CanonicalResolver | None = None,
) -> dict[str, dict[str, Any]]:
    source_indices = [
        SETTINGS.source_launch_index,
        SETTINGS.source_completed_index,
        SETTINGS.source_project_index,
    ]
    seeds: dict[str, dict[str, Any]] = {}
    grouped_docs: dict[str, list[tuple[dict[str, Any], str, dict[str, str | None]]]] = {}

    for source_index in source_indices:
        docs = _iter_project_builder_docs(es, source_index)
        logger.info("Found %d project docs in %s for builder aggregation", len(docs), source_index)
        for doc in docs:
            source_system = _as_text(doc.get("source")) or source_index
            evidence_url = _normalize_url(doc.get("project_url") or doc.get("source_url"))

            raw_city = _as_text(doc.get("city") or doc.get("target_city"))
            raw_state = _as_text(doc.get("state"))
            raw_country = _as_text(doc.get("country")) or _infer_country(raw_city, raw_state)

            country = _resolve_canonical_value(
                resolver,
                "country",
                raw_country,
                source_index=source_index,
                source_system=source_system,
                evidence_url=evidence_url,
                allow_ai=False,
            )
            state = _resolve_canonical_value(
                resolver,
                "state",
                raw_state,
                context={"country": country},
                country_guardrail=country,
                source_index=source_index,
                source_system=source_system,
                evidence_url=evidence_url,
                allow_ai=False,
            )
            city = _resolve_canonical_value(
                resolver,
                "city",
                raw_city,
                context={"country": country, "state": state},
                country_guardrail=country,
                source_index=source_index,
                source_system=source_system,
                evidence_url=evidence_url,
                allow_ai=False,
            )

            builder_name = _resolve_canonical_value(
                resolver,
                "builder_name",
                _normalize_builder_name(doc.get("builder_name")),
                context={"country": country, "state": state, "city": city},
                country_guardrail=country,
                source_index=source_index,
                source_system=source_system,
                evidence_url=evidence_url,
                allow_ai=True,
            )
            if not builder_name or builder_name.lower() == "unknown" or not _looks_like_builder_name(builder_name, strict=True):
                continue

            doc_id = _builder_doc_id(builder_name)
            grouped_docs.setdefault(doc_id, []).append(
                (
                    doc,
                    source_index,
                    {
                        "builder_name": builder_name,
                        "city": city,
                        "state": state,
                        "country": country,
                    },
                )
            )

    for doc_id, items in grouped_docs.items():
        builder_name = _as_text(items[0][2].get("builder_name")) or "Unknown Builder"
        seed = _new_seed(builder_name)
        seed["builder_name"] = builder_name

        for doc, source_index, canonical in items:
            seed = _update_seed_from_project(
                seed,
                doc,
                source_index,
                resolver=resolver,
                pre_canonical=canonical,
            )
        seeds[doc_id] = seed

    finalized: dict[str, dict[str, Any]] = {}
    for doc_id, seed in seeds.items():
        profile = _finalize_seed(seed)
        profile["id"] = doc_id
        profile["builder_slug"] = _builder_slug(profile.get("builder_name") or "builder")
        finalized[doc_id] = profile
    return finalized


def _merge_web_discovery_into_seeds(
    seeds: dict[str, dict[str, Any]],
    city: str,
    results: list[dict[str, str]],
    *,
    resolver: CanonicalResolver | None = None,
) -> dict[str, dict[str, Any]]:
    merged = dict(seeds)
    city_country = _infer_country(city, None)
    city_canonical = _resolve_canonical_value(
        resolver,
        "city",
        city,
        context={"country": city_country},
        country_guardrail=city_country,
        source_index=SETTINGS.es_index,
        source_system="duckduckgo",
        allow_ai=False,
    ) or city

    for result in results:
        result_url = _normalize_url(result.get("url"))
        result_source = _as_text(result.get("source")) or "duckduckgo"
        combined_text = " ".join(
            [
                _as_text(result.get("title")) or "",
                _as_text(result.get("snippet")) or "",
            ]
        )
        explicit_name = _select_best_builder_name(result.get("builder_name"), strict=False)
        names = [explicit_name] if explicit_name else _extract_builder_names_from_text(combined_text)
        if not names:
            domain_name = _builder_name_from_domain(result.get("url") or "")
            if domain_name:
                names = [domain_name]
        for name in names:
            country_hint = _infer_country(city_canonical, None)
            canonical_name = _resolve_canonical_value(
                resolver,
                "builder_name",
                name,
                context={"country": country_hint, "city": city_canonical},
                country_guardrail=country_hint,
                source_index=SETTINGS.es_index,
                source_system=result_source,
                evidence_url=result_url,
                allow_ai=True,
            ) or name
            if not _looks_like_builder_name(canonical_name, strict=True):
                continue
            doc_id = _builder_doc_id(canonical_name)
            existing = merged.get(doc_id)
            if existing is None:
                seed = _new_seed(canonical_name)
            else:
                seed = _merge_seed(_new_seed(canonical_name), existing)
            if not _has_value(seed.get("_discovery_order")):
                seed["_discovery_order"] = len(merged) + 1
            if _as_text(seed.get("builder_name")) and _as_text(seed.get("builder_name")).lower() != canonical_name.lower():
                seed["aliases"] = _merge_lists(seed.get("aliases"), [seed.get("builder_name")])
            seed["builder_name"] = canonical_name
            seed["source_names"] = _merge_lists(seed.get("source_names"), [result_source])
            seed["sources"] = _merge_lists(seed.get("sources"), [result_url])
            discovered_website = _normalize_url(result.get("website"))
            if discovered_website:
                seed["known_websites"] = _merge_lists(seed.get("known_websites"), [discovered_website])
            if city_canonical:
                seed["operating_cities"] = _merge_lists(seed.get("operating_cities"), [city_canonical])
                city_counts = dict(seed.get("_city_counts") or {})
                city_counts[city_canonical] = city_counts.get(city_canonical, 0) + 1
                seed["_city_counts"] = city_counts
            if city_country:
                seed["operating_countries"] = _merge_lists(seed.get("operating_countries"), [city_country])
            website = result_url
            confidence = _website_candidate_confidence(website, _builder_slug(canonical_name), canonical_name)
            if confidence >= 0.7:
                seed["known_websites"] = _merge_lists(seed.get("known_websites"), [website])
            merged[doc_id] = _finalize_seed(seed)
            merged[doc_id]["id"] = doc_id
            merged[doc_id]["builder_slug"] = _builder_slug(merged[doc_id].get("builder_name") or canonical_name)
    return merged


def _lookup_existing_doc_ids(es: Elasticsearch, doc_ids: list[str]) -> set[str]:
    if not doc_ids:
        return set()
    try:
        if not es.indices.exists(index=SETTINGS.es_index):
            return set()
    except Exception:
        return set()

    found: set[str] = set()
    for i in range(0, len(doc_ids), 500):
        chunk = doc_ids[i : i + 500]
        try:
            result = es.mget(index=SETTINGS.es_index, body={"ids": chunk})
        except Exception as exc:
            logger.warning("mget failed while checking existing profiles: %s", exc)
            continue
        for doc in result.get("docs", []):
            if doc.get("found") and doc.get("_id"):
                found.add(doc["_id"])
    return found


def _fetch_existing_profiles(es: Elasticsearch, doc_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not doc_ids:
        return {}
    try:
        if not es.indices.exists(index=SETTINGS.es_index):
            return {}
    except Exception:
        return {}

    found: dict[str, dict[str, Any]] = {}
    for i in range(0, len(doc_ids), 500):
        chunk = doc_ids[i : i + 500]
        try:
            result = es.mget(index=SETTINGS.es_index, body={"ids": chunk})
        except Exception as exc:
            logger.warning("mget failed while loading existing profiles: %s", exc)
            continue
        for doc in result.get("docs", []):
            if doc.get("found") and doc.get("_id"):
                found[doc["_id"]] = doc.get("_source", {})
    return found


def _search_builder_on_brave(session: requests.Session, builder_name: str) -> list[dict[str, str]]:
    query = f'"{builder_name}" builder official website founder head office'
    url = f"https://search.brave.com/search?q={quote_plus(query)}&source=web"
    try:
        html_text = _fetch(session, url, retries=1)
    except Exception as exc:
        logger.warning("Brave builder web search failed for %s: %s", builder_name, exc)
        return []
    return parse_brave_results(html_text)[:10]


def _search_builder_official_site(session: requests.Session, builder_name: str) -> list[dict[str, str]]:
    query = f'"{builder_name}" "official site" "real estate"'
    ddg_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    try:
        html_text = _fetch(session, ddg_url, retries=1)
        results = parse_duckduckgo_results(html_text)
    except Exception:
        results = []

    if not results:
        results = _search_builder_on_bing_rss(session, builder_name)

    official: list[dict[str, str]] = []
    for result in results:
        url = _normalize_url(result.get("url"))
        if not url or not _is_candidate_builder_website(url):
            continue
        official.append(
            {
                "title": _as_text(result.get("title")) or "",
                "url": url,
                "snippet": _as_text(result.get("snippet")) or "",
                "source": _as_text(result.get("source")) or "duckduckgo",
            }
        )
        if len(official) >= 10:
            break
    return official


def _search_builder_on_bing_rss(session: requests.Session, builder_name: str) -> list[dict[str, str]]:
    query = f'"{builder_name}" builder official website founder head office'
    url = f"https://www.bing.com/search?q={quote_plus(query)}&format=rss&setlang=en"
    try:
        xml_text = _fetch(session, url, retries=1)
    except Exception as exc:
        logger.warning("Bing RSS builder web search failed for %s: %s", builder_name, exc)
        return []

    results: list[dict[str, str]] = []
    try:
        root = ET.fromstring(xml_text)
    except Exception as exc:
        logger.warning("Failed to parse Bing RSS for %s: %s", builder_name, exc)
        return []

    for item in root.findall("./channel/item"):
        link = _normalize_url(item.findtext("link"))
        if not _is_valid_discovery_url(link):
            continue
        title = _normalize_space(_as_text(item.findtext("title")) or "")
        snippet = _normalize_space(_as_text(item.findtext("description")) or "")
        results.append({"title": title, "url": str(link), "snippet": snippet, "source": "bing"})

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for result in results:
        key = result["url"].strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(result)
    return deduped[:10]


def _search_builder_on_web(session: requests.Session, builder_name: str, *, allow_ddg: bool = True) -> dict[str, Any]:
    query = f'"{builder_name}" builder official website founder head office'
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    payload: dict[str, Any] = {
        "results": [],
        "known_websites": [],
        "sources": [],
        "source_names": [],
        "context_blocks": [],
    }
    builder_slug = _builder_slug(builder_name)
    results: list[dict[str, str]] = []
    if allow_ddg:
        try:
            html_text = _fetch(session, url, retries=2)
            results = parse_duckduckgo_results(html_text)[:10]
            if results:
                payload["source_names"] = _merge_lists(payload.get("source_names"), ["duckduckgo"])
        except Exception as exc:
            logger.warning("Builder web search failed for %s: %s", builder_name, exc)
    if not results:
        bing_results = _search_builder_on_bing_rss(session, builder_name)
        if bing_results:
            results = bing_results
            payload["source_names"] = _merge_lists(payload.get("source_names"), ["bing"])
            logger.info("Using Bing RSS fallback search for builder=%s with %d results", builder_name, len(bing_results))

    if not results:
        brave_results = _search_builder_on_brave(session, builder_name)
        if brave_results:
            results = brave_results
            payload["source_names"] = _merge_lists(payload.get("source_names"), ["brave"])
            logger.info("Using Brave fallback search for builder=%s with %d results", builder_name, len(brave_results))

    if not results:
        return payload

    payload["results"] = results

    for result in results:
        rurl = _normalize_url(result.get("url"))
        if rurl:
            payload["sources"] = _merge_lists(payload.get("sources"), [rurl])
            payload["source_names"] = _merge_lists(payload.get("source_names"), [result.get("source")])
            confidence = _website_candidate_confidence(rurl, builder_slug, builder_name)
            if confidence >= 0.75:
                payload["known_websites"] = _merge_lists(payload.get("known_websites"), [rurl])

        title = _as_text(result.get("title")) or ""
        snippet = _as_text(result.get("snippet")) or ""
        if title or snippet:
            payload["context_blocks"].append(f"Title: {title}\nSnippet: {snippet}\nURL: {rurl or ''}")

    detail_urls: list[str] = []
    for result in results:
        rurl = _normalize_url(result.get("url"))
        if not rurl:
            continue
        if _is_social_url(rurl):
            continue
        if _looks_like_asset_url(rurl):
            continue
        if rurl.lower().endswith(".pdf"):
            continue
        detail_urls.append(rurl)
        if len(detail_urls) >= SETTINGS.detail_pages_per_builder:
            break

    for detail_url in detail_urls:
        try:
            page_html = _fetch(session, detail_url, retries=1)
            page_text = _html_to_text(page_html)
            if page_text:
                payload["context_blocks"].append(f"Page URL: {detail_url}\nPage Text: {page_text[:4500]}")
        except Exception as exc:
            logger.debug("Detail fetch failed for %s: %s", detail_url, exc)

    payload["known_websites"] = _sanitize_known_websites(payload.get("known_websites"))

    if not payload["known_websites"]:
        official_results = _search_builder_official_site(session, builder_name)
        if official_results:
            for result in official_results:
                payload["results"].append(result)
                payload["sources"] = _merge_lists(payload.get("sources"), [_normalize_url(result.get("url"))])
                payload["source_names"] = _merge_lists(payload.get("source_names"), [result.get("source")])
                confidence = _website_candidate_confidence(result.get("url"), builder_slug, builder_name)
                if confidence >= 0.62:
                    payload["known_websites"] = _merge_lists(payload.get("known_websites"), [result.get("url")])
            payload["known_websites"] = _sanitize_known_websites(payload.get("known_websites"))

    return payload


def _normalize_ai_profile(ai_profile: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for field in PROFILE_LIST_FIELDS:
        if field in ai_profile:
            normalized[field] = _clean_list(ai_profile.get(field))

    for field in (
        "builder_name",
        "website",
        "head_office_city",
        "head_office_state",
        "head_office_country",
        "head_office_address",
        "head_office_location",
        "company_type",
        "category",
        "main_operating_city",
        "rera_reg_no",
        "linkedin_url",
        "twitter_url",
        "youtube_url",
        "instagram_url",
        "facebook_url",
        "reputation_summary",
        "builder_tier",
        "ceo",
        "managing_director",
        "stock_ticker",
        "cin",
    ):
        if field in ai_profile:
            normalized[field] = _as_text(ai_profile.get(field))

    normalized["founded_year"] = _safe_int(ai_profile.get("founded_year"))
    normalized["google_reviews_count"] = _safe_int(ai_profile.get("google_reviews_count"))
    normalized["google_reviews_rating"] = _safe_float(ai_profile.get("google_reviews_rating"))

    for url_field in (
        "website",
        "linkedin_url",
        "twitter_url",
        "youtube_url",
        "instagram_url",
        "facebook_url",
    ):
        normalized[url_field] = _normalize_url(normalized.get(url_field))

    normalized["known_websites"] = _sanitize_known_websites(normalized.get("known_websites"))

    return {k: v for k, v in normalized.items() if _has_value(v)}


def _compose_incoming_profile(
    seed: dict[str, Any],
    web_payload: dict[str, Any],
    ai_payload: dict[str, Any],
    *,
    resolver: CanonicalResolver | None = None,
) -> dict[str, Any]:
    now = _to_iso_now()

    source_index = SETTINGS.es_index
    source_system = "builder_profiles_pipeline"
    evidence_url = (_clean_list(web_payload.get("sources") or []) or [None])[0]

    seed_country = (_clean_list(seed.get("operating_countries") or []) or [None])[0]
    seed_state = (_clean_list(seed.get("operating_states") or []) or [None])[0]
    seed_city = _as_text(seed.get("main_operating_city")) or (_clean_list(seed.get("operating_cities") or []) or [None])[0]

    raw_builder_name = (
        _select_best_builder_name(ai_payload.get("builder_name"), seed.get("builder_name"), strict=True)
        or _select_best_builder_name(ai_payload.get("builder_name"), seed.get("builder_name"), strict=False)
        or _normalize_builder_name(seed.get("builder_name"))
        or _normalize_builder_name(ai_payload.get("builder_name"))
        or "Unknown Builder"
    )
    builder_name = _resolve_canonical_value(
        resolver,
        "builder_name",
        raw_builder_name,
        context={"country": seed_country, "state": seed_state, "city": seed_city},
        country_guardrail=seed_country,
        source_index=source_index,
        source_system=source_system,
        evidence_url=evidence_url,
        allow_ai=True,
    ) or raw_builder_name
    builder_name = _select_best_builder_name(builder_name, raw_builder_name, strict=False) or raw_builder_name

    doc_id = seed.get("id") or _builder_doc_id(builder_name)
    slug = seed.get("builder_slug") or _builder_slug(builder_name)

    office_country = _resolve_canonical_value(
        resolver,
        "country",
        _as_text(ai_payload.get("head_office_country")) or seed_country,
        source_index=source_index,
        source_system=source_system,
        evidence_url=evidence_url,
        allow_ai=False,
    )
    office_state = _resolve_canonical_value(
        resolver,
        "state",
        _as_text(ai_payload.get("head_office_state")) or seed_state,
        context={"country": office_country},
        country_guardrail=office_country,
        source_index=source_index,
        source_system=source_system,
        evidence_url=evidence_url,
        allow_ai=False,
    )
    office_city = _resolve_canonical_value(
        resolver,
        "city",
        _as_text(ai_payload.get("head_office_city")) or seed_city,
        context={"country": office_country, "state": office_state},
        country_guardrail=office_country,
        source_index=source_index,
        source_system=source_system,
        evidence_url=evidence_url,
        allow_ai=False,
    )

    operating_countries_raw = _merge_lists(seed.get("operating_countries"), ai_payload.get("operating_countries"))
    operating_countries: list[str] = []
    for item in operating_countries_raw:
        canonical = _resolve_canonical_value(
            resolver,
            "country",
            item,
            source_index=source_index,
            source_system=source_system,
            evidence_url=evidence_url,
            allow_ai=False,
        )
        if canonical:
            operating_countries.append(canonical)
    operating_countries = _clean_list(operating_countries)

    operating_states_raw = _merge_lists(seed.get("operating_states"), ai_payload.get("operating_states"))
    operating_states: list[str] = []
    for item in operating_states_raw:
        canonical = _resolve_canonical_value(
            resolver,
            "state",
            item,
            context={"country": office_country or seed_country},
            country_guardrail=office_country or seed_country,
            source_index=source_index,
            source_system=source_system,
            evidence_url=evidence_url,
            allow_ai=False,
        )
        if canonical:
            operating_states.append(canonical)
    operating_states = _clean_list(operating_states)

    operating_cities_raw = _merge_lists(seed.get("operating_cities"), ai_payload.get("operating_cities"))
    operating_cities: list[str] = []
    for item in operating_cities_raw:
        canonical = _resolve_canonical_value(
            resolver,
            "city",
            item,
            context={"country": office_country or seed_country, "state": office_state or seed_state},
            country_guardrail=office_country or seed_country,
            source_index=source_index,
            source_system=source_system,
            evidence_url=evidence_url,
            allow_ai=False,
        )
        if canonical:
            operating_cities.append(canonical)
    operating_cities = _clean_list(operating_cities)

    main_operating_city = _resolve_canonical_value(
        resolver,
        "city",
        _as_text(ai_payload.get("main_operating_city")) or _as_text(seed.get("main_operating_city")) or (operating_cities[0] if operating_cities else None),
        context={"country": office_country or seed_country, "state": office_state or seed_state},
        country_guardrail=office_country or seed_country,
        source_index=source_index,
        source_system=source_system,
        evidence_url=evidence_url,
        allow_ai=False,
    )

    known_seed_websites = _sanitize_known_websites(seed.get("known_websites"))
    known_web_websites = _sanitize_known_websites(web_payload.get("known_websites"))
    known_ai_websites = _sanitize_known_websites(ai_payload.get("known_websites"))
    merged_aliases = _merge_lists(seed.get("aliases"), ai_payload.get("aliases"))
    curated_website = _curated_website_for_builder(builder_name, slug, merged_aliases)

    profile: dict[str, Any] = {
        "id": doc_id,
        "builder_name": builder_name,
        "builder_slug": slug,
        "aliases": merged_aliases,
        "known_websites": _merge_lists(
            _merge_lists(seed.get("known_websites"), web_payload.get("known_websites")),
            ai_payload.get("known_websites"),
        ),
        "operating_cities": operating_cities,
        "operating_states": operating_states,
        "operating_countries": operating_countries,
        "main_operating_city": main_operating_city,
        "specializations": _merge_lists(seed.get("specializations"), ai_payload.get("specializations")),
        "category": _as_text(ai_payload.get("category")) or _as_text(seed.get("category")),
        "builder_tier": _as_text(ai_payload.get("builder_tier")) or _as_text(seed.get("builder_tier")),
        "total_projects": _safe_int(seed.get("total_projects")) or 0,
        "total_projects_completed": _safe_int(seed.get("total_projects_completed")) or 0,
        "total_projects_ongoing": _safe_int(seed.get("total_projects_ongoing")) or 0,
        "key_projects": _merge_lists(seed.get("key_projects"), ai_payload.get("key_projects")),
        "recent_launches": _merge_lists(seed.get("recent_launches"), ai_payload.get("recent_launches")),
        "source_names": _merge_lists(seed.get("source_names"), web_payload.get("source_names")),
        "sources": _merge_lists(seed.get("sources"), web_payload.get("sources")),
        "rera_reg_no": _as_text(ai_payload.get("rera_reg_no")) or _as_text(seed.get("rera_reg_no")),
        "rera_numbers": _merge_lists(seed.get("rera_numbers"), ai_payload.get("rera_numbers")),
        "price_range_min": _merge_min(seed.get("price_range_min"), ai_payload.get("price_range_min")),
        "price_range_max": _merge_max(seed.get("price_range_max"), ai_payload.get("price_range_max")),
        "price_per_sqft_min": _merge_min(seed.get("price_per_sqft_min"), ai_payload.get("price_per_sqft_min")),
        "price_per_sqft_max": _merge_max(seed.get("price_per_sqft_max"), ai_payload.get("price_per_sqft_max")),
        "price_currency": _as_text(seed.get("price_currency")) or _as_text(ai_payload.get("price_currency")) or "INR",
        "head_office_city": office_city,
        "head_office_state": office_state,
        "head_office_country": office_country,
        "head_office_address": _as_text(ai_payload.get("head_office_address")),
        "head_office_location": _as_text(ai_payload.get("head_office_location")),
        "founded_year": _safe_int(ai_payload.get("founded_year")),
        "founders": _clean_list(ai_payload.get("founders")),
        "key_people": _clean_list(ai_payload.get("key_people")),
        "company_type": _as_text(ai_payload.get("company_type")),
        "stock_ticker": _as_text(ai_payload.get("stock_ticker")),
        "cin": _as_text(ai_payload.get("cin")),
        "ceo": _as_text(ai_payload.get("ceo")),
        "managing_director": _as_text(ai_payload.get("managing_director")),
        "website": _normalize_url(ai_payload.get("website")),
        "website_source": "ai" if _normalize_url(ai_payload.get("website")) else None,
        "website_confidence": _website_candidate_confidence(
            _normalize_url(ai_payload.get("website")),
            slug,
            builder_name,
        )
        if _normalize_url(ai_payload.get("website"))
        else None,
        "linkedin_url": _normalize_url(ai_payload.get("linkedin_url")),
        "twitter_url": _normalize_url(ai_payload.get("twitter_url")),
        "youtube_url": _normalize_url(ai_payload.get("youtube_url")),
        "instagram_url": _normalize_url(ai_payload.get("instagram_url")),
        "facebook_url": _normalize_url(ai_payload.get("facebook_url")),
        "google_reviews_rating": _safe_float(ai_payload.get("google_reviews_rating")),
        "google_reviews_count": _safe_int(ai_payload.get("google_reviews_count")),
        "reputation_summary": _as_text(ai_payload.get("reputation_summary")),
        "discovered_at": now,
        "updated_at": now,
        "last_enriched_at": now,
        "enrichment_count": 1,
    }

    if curated_website:
        profile["known_websites"] = _merge_lists(profile.get("known_websites"), [curated_website])

    profile["known_websites"] = _sanitize_known_websites(profile.get("known_websites"))

    if not profile.get("website"):
        website = curated_website
        if website:
            profile["website_source"] = "curated_map"
            profile["website_confidence"] = 0.99
        else:
            website = _preferred_website(profile.get("known_websites") or [], slug, builder_name)
        if not website:
            inferred_website = None
            inferred_confidence = 0.0
            for candidate in _clean_list(_merge_lists(profile.get("sources"), seed.get("sources"))):
                inferred = _infer_website_from_evidence_url(candidate, builder_name, slug)
                if not inferred:
                    continue
                confidence = _website_candidate_confidence(inferred, slug, builder_name)
                if confidence > inferred_confidence:
                    inferred_website = inferred
                    inferred_confidence = confidence
            if inferred_website:
                website = inferred_website
                profile["website_source"] = "inferred_evidence"
                profile["website_confidence"] = inferred_confidence
        if not website:
            logger.debug(
                "No preferred website candidate found for builder=%s slug=%s known_websites=%d",
                profile.get("builder_name"),
                slug,
                len(_clean_list(profile.get("known_websites"))),
            )
        profile["website"] = website
    else:
        website = _normalize_url(profile.get("website"))
        if website and not _looks_like_official_site(website, slug):
            logger.debug(
                "Demoting non-official website for builder=%s slug=%s website=%s",
                profile.get("builder_name"),
                slug,
                website,
            )
            profile["sources"] = _merge_lists(profile.get("sources"), [website])
            replacement = _preferred_website(profile.get("known_websites") or [], slug, builder_name)
            if not replacement:
                logger.debug(
                    "No replacement website candidate after demotion for builder=%s slug=%s",
                    profile.get("builder_name"),
                    slug,
                )
            profile["website"] = replacement
            if replacement:
                profile["website_source"] = "known_websites"
                profile["website_confidence"] = _website_candidate_confidence(replacement, slug, builder_name)
    if profile.get("website"):
        profile["known_websites"] = _merge_lists(profile.get("known_websites"), [profile.get("website")])
        website = _normalize_url(profile.get("website"))
        if website:
            if not _has_value(profile.get("website_source")):
                if _url_in_values(website, known_ai_websites):
                    profile["website_source"] = "ai_known_websites"
                elif _url_in_values(website, known_web_websites):
                    profile["website_source"] = "web_search"
                elif _url_in_values(website, known_seed_websites):
                    profile["website_source"] = "seed"
                elif _url_in_values(website, profile.get("sources")):
                    profile["website_source"] = "source_url_inference"
                else:
                    profile["website_source"] = "known_websites"
            if profile.get("website_source") == "curated_map":
                profile["website_confidence"] = 0.99
            else:
                confidence = _website_candidate_confidence(website, slug, builder_name)
                if confidence >= 0.75:
                    profile["website_confidence"] = confidence
                else:
                    profile["sources"] = _merge_lists(profile.get("sources"), [website])
                    profile["website"] = None
                    profile["website_source"] = None
                    profile["website_confidence"] = None
    else:
        logger.debug(
            "Website unresolved for builder=%s slug=%s known_websites=%d sources=%d",
            profile.get("builder_name"),
            slug,
            len(_clean_list(profile.get("known_websites"))),
            len(_clean_list(profile.get("sources"))),
        )
        profile["website_source"] = None
        profile["website_confidence"] = None

    profile["known_websites"] = _sanitize_known_websites(profile.get("known_websites"))

    if not profile.get("main_operating_city") and profile.get("operating_cities"):
        profile["main_operating_city"] = profile["operating_cities"][0]

    if not profile.get("head_office_country"):
        profile["head_office_country"] = _infer_country(profile.get("head_office_city"), profile.get("head_office_state"))

    if not profile.get("category"):
        profile["category"] = _category_from_specializations(profile.get("specializations") or [])

    if profile.get("founded_year"):
        current_year = datetime.now(timezone.utc).year
        years = current_year - int(profile["founded_year"])
        if years >= 0:
            profile["years_in_business"] = years

    profile["rera_registered"] = bool(profile.get("rera_reg_no") or profile.get("rera_numbers"))

    profile["aliases"] = [
        alias
        for alias in _clean_list(profile.get("aliases"))
        if alias.lower() != profile["builder_name"].lower()
    ]

    profile["data_quality"] = _compute_data_quality(profile)
    return {k: v for k, v in profile.items() if k in MANDATORY_FIELDS or _has_value(v)}


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = _as_text(value)
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _needs_web_refresh(existing: dict[str, Any] | None) -> bool:
    if not existing:
        return True
    if (existing.get("data_quality") or "").lower() != "high":
        return True

    core_missing = any(
        not _has_value(existing.get(field))
        for field in ("website", "head_office_city", "founders", "category")
    )
    if core_missing:
        return True

    last_enriched = _parse_iso_datetime(existing.get("last_enriched_at"))
    if last_enriched is None:
        return True
    if datetime.now(timezone.utc) - last_enriched > timedelta(days=14):
        return True
    return False


def merge_builder_profile(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    now = _to_iso_now()
    merged = dict(existing or {})

    merged["id"] = incoming.get("id") or existing.get("id")
    merged["builder_slug"] = incoming.get("builder_slug") or existing.get("builder_slug")

    incoming_name = _normalize_builder_name(incoming.get("builder_name"))
    existing_name = _normalize_builder_name(existing.get("builder_name"))
    incoming_score = _builder_name_score(incoming_name)
    existing_score = _builder_name_score(existing_name)
    if incoming_name and not existing_name:
        merged["builder_name"] = incoming_name
    elif incoming_name and existing_name:
        if incoming_score > existing_score:
            merged["builder_name"] = incoming_name
            merged["aliases"] = _merge_lists(existing.get("aliases"), [existing_name])
        elif existing_score > incoming_score:
            merged["builder_name"] = existing_name
            merged["aliases"] = _merge_lists(existing.get("aliases"), [incoming_name])
        else:
            merged["builder_name"] = incoming_name if len(incoming_name) > len(existing_name) else existing_name
            alias = existing_name if merged["builder_name"] == incoming_name else incoming_name
            merged["aliases"] = _merge_lists(existing.get("aliases"), [alias])
    else:
        merged["builder_name"] = existing_name or incoming.get("builder_name") or "Unknown Builder"

    for key in PROFILE_LIST_FIELDS:
        merged[key] = _merge_lists(existing.get(key), incoming.get(key))

    merged["known_websites"] = _sanitize_known_websites(merged.get("known_websites"))

    for key in MIN_FIELDS:
        merged[key] = _merge_min(existing.get(key), incoming.get(key))

    for key in MAX_FIELDS:
        merged[key] = _merge_max(existing.get(key), incoming.get(key))

    for key in (
        "head_office_city",
        "head_office_state",
        "head_office_country",
        "head_office_address",
        "head_office_location",
        "company_type",
        "category",
        "builder_tier",
        "main_operating_city",
        "rera_reg_no",
        "linkedin_url",
        "twitter_url",
        "youtube_url",
        "instagram_url",
        "facebook_url",
        "stock_ticker",
        "cin",
        "ceo",
        "managing_director",
    ):
        incoming_value = incoming.get(key)
        existing_value = existing.get(key)
        if _has_value(incoming_value) and not _has_value(existing_value):
            merged[key] = incoming_value
        elif _has_value(existing_value):
            merged[key] = existing_value
        elif _has_value(incoming_value):
            merged[key] = incoming_value

    incoming_founded_year = _safe_int(incoming.get("founded_year"))
    existing_founded_year = _safe_int(existing.get("founded_year"))
    if incoming_founded_year and (not existing_founded_year or incoming_founded_year < existing_founded_year):
        merged["founded_year"] = incoming_founded_year
    elif existing_founded_year:
        merged["founded_year"] = existing_founded_year

    incoming_summary = _as_text(incoming.get("reputation_summary"))
    existing_summary = _as_text(existing.get("reputation_summary"))
    if incoming_summary and (not existing_summary or len(incoming_summary) > len(existing_summary)):
        merged["reputation_summary"] = incoming_summary
    elif existing_summary:
        merged["reputation_summary"] = existing_summary

    incoming_rating = _safe_float(incoming.get("google_reviews_rating"))
    existing_rating = _safe_float(existing.get("google_reviews_rating"))
    if incoming_rating is not None and existing_rating is None:
        merged["google_reviews_rating"] = incoming_rating
    elif existing_rating is not None:
        merged["google_reviews_rating"] = existing_rating

    merged["google_reviews_count"] = _merge_max(existing.get("google_reviews_count"), incoming.get("google_reviews_count"))

    website_source = _as_text(existing.get("website_source")) or _as_text(incoming.get("website_source"))
    website_confidence = _safe_float(existing.get("website_confidence"))
    if website_confidence is None:
        website_confidence = _safe_float(incoming.get("website_confidence"))

    website = _normalize_url(existing.get("website")) or _normalize_url(incoming.get("website"))
    slug = _as_text(merged.get("builder_slug")) or _builder_slug(_as_text(merged.get("builder_name")) or "")
    curated_website = _curated_website_for_builder(merged.get("builder_name"), slug, merged.get("aliases"))
    if curated_website:
        merged["known_websites"] = _merge_lists(merged.get("known_websites"), [curated_website])
    if website and not _looks_like_official_site(website, slug):
        logger.debug(
            "Demoting non-official merged website for builder=%s slug=%s website=%s",
            merged.get("builder_name"),
            slug,
            website,
        )
        merged["sources"] = _merge_lists(merged.get("sources"), [website])
        website = None
    if curated_website and (not website or website != curated_website):
        current_conf = _website_candidate_confidence(website, slug, merged.get("builder_name")) if website else 0.0
        if current_conf < 0.9:
            website = curated_website
            website_source = "curated_map"
            website_confidence = 0.99
    if not website:
        websites = _merge_lists(merged.get("known_websites"), [])
        website = _preferred_website(websites, slug, _as_text(merged.get("builder_name")))
        if not website:
            logger.debug(
                "No preferred merged website candidate for builder=%s slug=%s known_websites=%d",
                merged.get("builder_name"),
                slug,
                len(websites),
            )
    if not website:
        inferred_website = None
        inferred_confidence = 0.0
        for candidate in _clean_list(merged.get("sources")):
            inferred = _infer_website_from_evidence_url(candidate, merged.get("builder_name"), slug)
            if not inferred:
                continue
            confidence = _website_candidate_confidence(inferred, slug, merged.get("builder_name"))
            if confidence > inferred_confidence:
                inferred_website = inferred
                inferred_confidence = confidence
        if inferred_website:
            website = inferred_website
            website_source = "inferred_evidence"
            website_confidence = inferred_confidence

    merged["website"] = website
    if website:
        merged["known_websites"] = _merge_lists(merged.get("known_websites"), [website])
        if not website_source:
            if _url_in_values(website, incoming.get("known_websites")) or _url_in_values(website, incoming.get("sources")):
                website_source = _as_text(incoming.get("website_source")) or "incoming"
            elif _url_in_values(website, existing.get("known_websites")) or _url_in_values(website, existing.get("sources")):
                website_source = _as_text(existing.get("website_source")) or "existing"
            else:
                website_source = "known_websites"
        if website_source == "curated_map":
            website_confidence = 0.99
        else:
            confidence = _website_candidate_confidence(website, slug, merged.get("builder_name"))
            if confidence >= 0.75:
                website_confidence = confidence
            else:
                merged["sources"] = _merge_lists(merged.get("sources"), [website])
                merged["website"] = None
                website = None
                website_source = None
                website_confidence = None
    else:
        logger.debug(
            "Website unresolved after merge for builder=%s slug=%s known_websites=%d sources=%d",
            merged.get("builder_name"),
            slug,
            len(_clean_list(merged.get("known_websites"))),
            len(_clean_list(merged.get("sources"))),
        )
        website_source = None
        website_confidence = None

    merged["website_source"] = website_source
    merged["website_confidence"] = website_confidence

    merged["known_websites"] = _sanitize_known_websites(merged.get("known_websites"))

    if not merged.get("main_operating_city") and merged.get("operating_cities"):
        merged["main_operating_city"] = merged["operating_cities"][0]

    if not merged.get("head_office_country"):
        merged["head_office_country"] = _infer_country(merged.get("head_office_city"), merged.get("head_office_state"))

    if not merged.get("category"):
        merged["category"] = _category_from_specializations(_clean_list(merged.get("specializations")))

    if merged.get("founded_year"):
        current_year = datetime.now(timezone.utc).year
        years = current_year - int(merged["founded_year"])
        if years >= 0:
            merged["years_in_business"] = years

    merged["total_projects_completed"] = _merge_max(existing.get("total_projects_completed"), incoming.get("total_projects_completed"))
    merged["total_projects_ongoing"] = _merge_max(existing.get("total_projects_ongoing"), incoming.get("total_projects_ongoing"))
    merged["total_projects"] = _merge_max(existing.get("total_projects"), incoming.get("total_projects"))
    if not merged.get("total_projects"):
        merged["total_projects"] = (merged.get("total_projects_completed") or 0) + (merged.get("total_projects_ongoing") or 0)

    merged["rera_registered"] = bool(merged.get("rera_reg_no") or _clean_list(merged.get("rera_numbers")))

    merged["discovered_at"] = existing.get("discovered_at") or incoming.get("discovered_at") or now
    merged["updated_at"] = now
    merged["last_enriched_at"] = incoming.get("last_enriched_at") or now
    merged["enrichment_count"] = int(_safe_int(existing.get("enrichment_count")) or 0) + 1

    merged["aliases"] = [
        alias
        for alias in _clean_list(merged.get("aliases"))
        if alias.lower() != str(merged.get("builder_name", "")).lower()
    ]

    merged["data_quality"] = _compute_data_quality(merged)
    return {k: v for k, v in merged.items() if k in MANDATORY_FIELDS or _has_value(v)}


def _profiles_to_actions(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for profile in profiles:
        doc_id = profile.get("id")
        if not doc_id:
            continue
        payload = {k: (None if isinstance(v, float) and np.isnan(v) else v) for k, v in profile.items()}
        actions.append(
            {
                "_index": SETTINGS.es_index,
                "_id": doc_id,
                "_op_type": "index",
                "_source": payload,
            }
        )
    return actions


class InputParams(BaseModel):
    force_web_refresh: bool = False
    max_builders: int | None = None


@task
async def discover_builders(params: InputParams = None) -> list[dict[str, Any]]:
    es = es_client()
    ensure_index(es, SETTINGS.es_index)
    resolver = build_canonical_resolver(es)

    seeds = _aggregate_builder_seeds_from_es(es, resolver=resolver)
    logger.info("Discovered %d unique builders from source indices", len(seeds))

    session = _build_http_client()
    for city in SETTINGS.discovery_cities:
        city_queries = _city_query_variants(city)
        if SETTINGS.realestateindia_enabled:
            try:
                rei_results = _search_realestateindia_builders(session, city)
                if rei_results:
                    seeds = _merge_web_discovery_into_seeds(
                        seeds,
                        city,
                        rei_results,
                        resolver=resolver,
                    )
                logger.info(
                    "RealEstateIndia discovery: city=%s yielded %d builder profiles",
                    city,
                    len(rei_results),
                )
            except Exception as exc:
                logger.warning("RealEstateIndia discovery failed for city=%s: %s", city, exc)

        for city_query in city_queries:
            for template in DISCOVERY_QUERY_TEMPLATES:
                query = template.format(city=city_query)
                for page in range(SETTINGS.duckduckgo_pages):
                    results = _discover_city_web_results(session, city, query, page)
                    if results:
                        seeds = _merge_web_discovery_into_seeds(
                            seeds,
                            city,
                            results,
                            resolver=resolver,
                        )
                    logger.info(
                        "Web discovery: city=%s query='%s' page=%d yielded %d links",
                        city,
                        query,
                        page + 1,
                        len(results),
                    )
                    await asyncio.sleep(random.uniform(SETTINGS.min_delay_seconds, SETTINGS.max_delay_seconds))

    discovered = list(seeds.values())
    _apply_domain_guess_enrichment(session, discovered)
    existing_ids = _lookup_existing_doc_ids(es, [item.get("id") for item in discovered if item.get("id")])
    for item in discovered:
        item["_is_existing"] = item.get("id") in existing_ids
        item["_priority_score"] = _seed_priority_score(item)

    discovered.sort(
        key=lambda item: (
            item.get("_is_existing", False),
            -int(item.get("_priority_score") or 0),
            -(int(item.get("total_projects") or 0)),
            int(item.get("_discovery_order") or 10**9),
            str(item.get("builder_name", "")).lower(),
        )
    )

    max_builders = params.max_builders if params and params.max_builders else SETTINGS.max_builders_per_run
    if max_builders and max_builders > 0:
        discovered = discovered[:max_builders]

    for item in discovered:
        item.pop("_is_existing", None)
        item.pop("_priority_score", None)
        item.pop("_discovery_order", None)

    out_dir = _saved_data_dir()
    pd.DataFrame(_serialize_for_json(discovered)).to_json(
        out_dir / "discovered_builders.json",
        orient="records",
        force_ascii=False,
        indent=2,
    )
    logger.info("Prepared %d builders for enrichment", len(discovered))
    return discovered


@task
async def enrich_builder_profiles(discovered: list[dict[str, Any]], params: InputParams = None) -> list[dict[str, Any]]:
    if not discovered:
        logger.info("No builders to enrich")
        return []

    force_web_refresh = bool(params.force_web_refresh) if params else False

    es = es_client()
    ensure_index(es, SETTINGS.es_index)
    resolver = build_canonical_resolver(es)
    existing_profiles = _fetch_existing_profiles(es, [item.get("id") for item in discovered if item.get("id")])

    session = _build_http_client()
    enriched: list[dict[str, Any]] = []
    ddg_failure_streak = 0
    ddg_circuit_open = False

    for idx, seed in enumerate(discovered, start=1):
        builder_name = seed.get("builder_name") or "Unknown Builder"
        doc_id = seed.get("id")
        existing = existing_profiles.get(doc_id or "", {})
        should_refresh = force_web_refresh or _needs_web_refresh(existing)

        logger.info(
            "Enriching builder %d/%d: %s (web_refresh=%s)",
            idx,
            len(discovered),
            builder_name,
            should_refresh,
        )

        web_payload: dict[str, Any] = {}
        ai_payload: dict[str, Any] = {}
        if should_refresh:
            if ddg_circuit_open:
                logger.warning(
                    "DDG circuit is open (failure_streak=%d); using Brave-only search for %s",
                    ddg_failure_streak,
                    builder_name,
                )
            web_payload = _search_builder_on_web(session, builder_name, allow_ddg=not ddg_circuit_open)
            results_count = len(web_payload.get("results") or [])
            context_blocks = web_payload.get("context_blocks") or []
            context_text = "\n\n".join(_clean_list(context_blocks))
            used_ddg = "duckduckgo" in _clean_list(web_payload.get("source_names"))
            if not ddg_circuit_open:
                if results_count == 0 or not used_ddg:
                    ddg_failure_streak += 1
                    if ddg_failure_streak >= DDG_FAILURE_STREAK_THRESHOLD:
                        ddg_circuit_open = True
                        logger.warning(
                            "Opening DDG circuit after %d consecutive empty/failing DDG searches; continuing with Brave fallback for the rest of this run",
                            ddg_failure_streak,
                        )
                else:
                    ddg_failure_streak = 0

            if context_text:
                ai_payload = _normalize_ai_profile(ai_extract_builder_profile(builder_name, context_text))
            await asyncio.sleep(random.uniform(SETTINGS.min_delay_seconds, SETTINGS.max_delay_seconds))

        incoming = _compose_incoming_profile(
            seed=seed,
            web_payload=web_payload,
            ai_payload=ai_payload,
            resolver=resolver,
        )
        if existing.get("discovered_at"):
            incoming["discovered_at"] = existing["discovered_at"]
        enriched.append(incoming)

    out_dir = _saved_data_dir()
    pd.DataFrame(enriched).to_json(out_dir / "enriched_builder_profiles.json", orient="records", force_ascii=False, indent=2)
    logger.info("Enriched %d builder profiles", len(enriched))
    return enriched


@task
async def merge_and_index_profiles(enriched: list[dict[str, Any]]) -> int:
    if not enriched:
        logger.info("No enriched builder profiles to index")
        return 0

    es = es_client()
    ensure_index(es, SETTINGS.es_index)

    existing_profiles = _fetch_existing_profiles(es, [item.get("id") for item in enriched if item.get("id")])
    merged_docs: list[dict[str, Any]] = []
    for incoming in enriched:
        doc_id = incoming.get("id")
        if not doc_id:
            continue
        existing = existing_profiles.get(doc_id, {})
        merged = merge_builder_profile(existing, incoming)
        merged_docs.append(merged)

    actions = _profiles_to_actions(merged_docs)
    if not actions:
        logger.info("No builder profile actions generated for indexing")
        return 0

    try:
        indexed, errors = helpers.bulk(
            es,
            actions,
            chunk_size=500,
            request_timeout=120,
            raise_on_error=False,
            raise_on_exception=False,
        )
    except Exception:
        logger.exception("Bulk indexing failed for builder profiles")
        raise

    if errors:
        logger.warning("Builder profile indexing completed with %d errors", len(errors))

    out_dir = _saved_data_dir()
    pd.DataFrame(merged_docs).to_json(out_dir / "indexed_builder_profiles.json", orient="records", force_ascii=False, indent=2)
    logger.info("Indexed %d builder profiles into %s", indexed, SETTINGS.es_index)
    return indexed


_config = read_config()
_es_config = _get_section(_config, "elasticsearch")
_builder_config = _get_section(_config, "builder_profiles")
_canonical_config = _get_section(_config, "canonical_mapping")

ES_HOSTS = _parse_es_hosts(_cfg_get(_es_config, "host", "http://localhost:9200"))
ES_USER = _cfg_get(_es_config, "username", "")
ES_PASSWORD = _cfg_get(_es_config, "password", "")

_cities_raw = _cfg_get(_builder_config, "discovery_cities", ",".join(DEFAULT_DISCOVERY_CITIES))
_cities = [city.strip() for city in str(_cities_raw).split(",") if city.strip()]
_auto_cities = _auto_discovery_cities_from_project_sections(_config)
if _auto_cities:
    _cities = _merge_lists(_cities, _auto_cities)
if not _cities:
    _cities = list(DEFAULT_DISCOVERY_CITIES)

SETTINGS = BuilderProfileSettings(
    es_index=str(_cfg_get(_builder_config, "es_index", DEFAULT_INDEX)).strip() or DEFAULT_INDEX,
    canonical_enabled=_to_bool(_cfg_get(_canonical_config, "enabled", "true"), True),
    canonical_index=str(_cfg_get(_canonical_config, "index", DEFAULT_CANONICAL_INDEX)).strip() or DEFAULT_CANONICAL_INDEX,
    canonical_max_ai_calls=max(0, int(_cfg_get(_canonical_config, "max_ai_calls_per_run", 150))),
    canonical_ai_enabled=_to_bool(_cfg_get(_canonical_config, "ai_enabled", "true"), True),
    source_launch_index=str(_cfg_get(_builder_config, "source_launch_index", DEFAULT_SOURCE_LAUNCH_INDEX)).strip() or DEFAULT_SOURCE_LAUNCH_INDEX,
    source_completed_index=str(_cfg_get(_builder_config, "source_completed_index", DEFAULT_SOURCE_COMPLETED_INDEX)).strip() or DEFAULT_SOURCE_COMPLETED_INDEX,
    source_project_index=str(_cfg_get(_builder_config, "source_project_index", DEFAULT_SOURCE_PROJECT_INDEX)).strip() or DEFAULT_SOURCE_PROJECT_INDEX,
    discovery_cities=_cities,
    realestateindia_enabled=_to_bool(_cfg_get(_builder_config, "realestateindia_enabled", "true"), True),
    realestateindia_max_load_more_pages=max(
        1,
        int(
            _cfg_get(
                _builder_config,
                "realestateindia_max_load_more_pages",
                REALESTATEINDIA_BUILDER_MAX_LOAD_MORE_PAGES,
            )
        ),
    ),
    duckduckgo_pages=max(1, int(_cfg_get(_builder_config, "duckduckgo_pages", 1))),
    detail_pages_per_builder=max(1, int(_cfg_get(_builder_config, "detail_pages_per_builder", 2))),
    max_builders_per_run=max(1, int(_cfg_get(_builder_config, "max_builders_per_run", 150))),
    min_delay_seconds=float(_cfg_get(_builder_config, "min_delay_seconds", 1.5)),
    max_delay_seconds=float(_cfg_get(_builder_config, "max_delay_seconds", 3.5)),
    request_timeout_seconds=max(5, int(_cfg_get(_builder_config, "request_timeout_seconds", 25))),
    schedule_hour=int(_cfg_get(_builder_config, "schedule_hour", 6)),
    schedule_minute=int(_cfg_get(_builder_config, "schedule_minute", 0)),
    schedule_timezone=str(_cfg_get(_builder_config, "schedule_timezone", "Asia/Dubai")),
    data_dir=Path(str(_cfg_get(_builder_config, "data_dir", "saved_data/builder_profiles"))),
)


register_pipeline(
    id="builder_profiles_pipeline",
    description="Discover new builders, enrich builder profiles, and merge-upsert profiles into Elasticsearch.",
    tasks=[discover_builders, enrich_builder_profiles, merge_and_index_profiles],
    triggers=[
        Trigger(
            id="builder_profiles_daily",
            name="Builder Profiles Daily",
            description="Daily builder discovery and profile enrichment run",
            params=InputParams(),
            schedule=CronTrigger(
                hour=SETTINGS.schedule_hour,
                minute=SETTINGS.schedule_minute,
                timezone=SETTINGS.schedule_timezone,
            ),
        )
    ],
    params=InputParams,
)
