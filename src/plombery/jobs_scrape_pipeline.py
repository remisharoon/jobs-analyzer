from __future__ import annotations
from asyncio import sleep
from datetime import datetime
import enum
from typing import Optional
from dateutil import tz
from requests_html import HTMLSession
from apscheduler.triggers.interval import IntervalTrigger

from pydantic import BaseModel, Field

from plombery import register_pipeline, task, Trigger, get_logger
import random
import mmh3
import requests
import time
import json
from jobspy import scrape_jobs
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select
import urllib.request
from config import read_config
import unicodedata
import re
import random

# llm_json_utils.py
# Robust JSON extraction/parsing for messy LLM outputs.


from typing import Any, Optional
import json
import re

# Optional dependencies: install any subset of these.
#   pip install json-repair dirtyjson python-rapidjson
try:
    import json_repair  # type: ignore
except Exception:
    json_repair = None

try:
    import dirtyjson  # type: ignore
except Exception:
    dirtyjson = None

try:
    import rapidjson  # python-rapidjson  # type: ignore
except Exception:
    rapidjson = None


def _strip_bom_and_fences(s: str) -> str:
    s = s.lstrip("\ufeff").strip()
    if s.startswith("```"):
        # remove leading ```(lang)?\n and trailing ```
        s = re.sub(r"^```[a-zA-Z0-9_-]*\s*\n?", "", s, count=1)
        if s.endswith("```"):
            s = s[: -3].rstrip()
    # kill common XSSI guards like )]}',
    s = re.sub(r"^\)\]\}',?\s*\n", "", s)
    return s


def _find_first_json_block(s: str) -> Optional[str]:
    """
    Return first balanced {...} or [...] segment.
    Honors string/escape context; won't break on braces inside strings.
    """
    s = s.strip()
    start_idx = None
    opener = None
    depth = 0
    in_str = False
    esc = False
    quote = None  # '"' or "'"

    for i, ch in enumerate(s):
        if start_idx is None:
            if ch in "{[":
                start_idx = i
                opener = ch
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
                # else stay in string
            else:
                if ch in ('"', "'"):
                    in_str = True
                    quote = ch
                elif ch in "{[":
                    depth += 1
                elif ch in "}]":
                    depth -= 1
                    if depth == 0:
                        return s[start_idx : i + 1]
                # else normal char
        # allow strings before first opener; ignore
    return None


def _pre_sanitize_for_strict(s: str) -> str:
    """
    Very conservative pre-sanitizer for strict json.loads fallback path.
    Only normalizes Python literals to JSON and strips control chars.
    Avoids risky regex surgery on structure.
    """
    # Replace Python None/True/False -> JSON null/true/false
    s = re.sub(r"\bNone\b", "null", s)
    s = re.sub(r"\bTrue\b", "true", s)
    s = re.sub(r"\bFalse\b", "false", s)

    # Remove lone control characters (keep \n \r \t)
    s = "".join(ch for ch in s if (ord(ch) >= 32 or ch in "\n\r\t"))
    return s


def parse_llm_json(text: str, *, max_chars: int = 2_000_000) -> Any:
    """
    Parse possibly-messy LLM output to a Python object.
    Order:
      1) strict json.loads(text)
      2) json_repair.loads(text)            (if installed)
      3) rapidjson with permissive parse    (if installed)
      4) dirtyjson.loads(search_first=True) (if installed)
      5) extract first JSON block and retry the stack
      6) strict json on a conservative pre-sanitized string
    Raises ValueError if nothing works.
    """
    if not isinstance(text, str):
        raise TypeError("parse_llm_json expects a string")
    if len(text) > max_chars:
        raise ValueError(f"Refusing to parse >{max_chars} characters")

    t = _strip_bom_and_fences(text)

    # 1) strict JSON
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        pass

    # 2) repairer: best for broken commas/quotes/braces
    if json_repair is not None:
        try:
            return json_repair.loads(t)
        except Exception:
            pass

    # 3) permissive JSON (comments/trailing commas)
    if rapidjson is not None:
        try:
            return rapidjson.loads(
                t,
                parse_mode=getattr(rapidjson, "PM_COMMENTS", 0)
                | getattr(rapidjson, "PM_TRAILING_COMMAS", 0),
            )
        except Exception:
            pass

    # 4) dirtyjson can skip prose and accept single quotes/unquoted keys
    if dirtyjson is not None:
        try:
            return dirtyjson.loads(t, search_for_first_object=True)
        except Exception:
            pass

    # 5) extract the first JSON block and retry the stack on that slice
    block = _find_first_json_block(t)
    if block:
        try:
            return parse_llm_json(block, max_chars=max_chars)
        except Exception:
            pass

    # 6) ultra-conservative pre-sanitize then strict
    t2 = _pre_sanitize_for_strict(t)
    try:
        return json.loads(t2)
    except json.JSONDecodeError as e:
        # Provide context for debugging
        snippet = t[:500].replace("\n", "\\n")
        raise ValueError(f"Could not parse/repair JSON. Starts with: {snippet}") from e


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text

def remove_control_characters(s):
    s = remove_prefix(s, "python")
    s = "".join(ch for ch in s if unicodedata.category(ch)[0]!="C")
    return s



class InputParams(BaseModel):
    """Showcase all the available input types in Plombery"""


# locations = ["United Arab Emirates", "Saudi Arabia", "Qatar", "Oman", "Kuwait", "Bahrain", "Turkey", "Malaysia"]
locations = ["United Arab Emirates", "Saudi Arabia", "Qatar"]
# Map location to the country name used in 'indeed'
# country_indeed_mapping = {
#     "United Arab Emirates": "united arab emirates",
#     "Saudi Arabia": "saudi arabia",
#     "Qatar": "qatar",
#     "Oman": "oman",
#     "Kuwait": "kuwait",
#     "Bahrain": "bahrain",
#     "Turkey": "turkey",
#     "Malaysia": "malaysia"
# }

country_indeed_mapping = {
    "United Arab Emirates": "united arab emirates",
    "Saudi Arabia": "saudi arabia",
    "Qatar": "qatar"
}


# Set up the API request
# url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent"
GEMINI_V2_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
GEMINI_V1_5_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"

MAX_RETRIES_V2 = 3          # how many 429s before trying 1 .5
MAX_RETRIES_V1_5 = 3        # optional: cap total tries

# params = {'key': 'your_api_key'}  # Replace 'your_api_key' with the actual API key
gemini_config = read_config()['GeminiPro']

GEMINI_API_KEY = random.choice([gemini_config['API_KEY_RH'], gemini_config['API_KEY_RHA']])

openrouter_config = read_config()['openrouter']
# OR_API_KEY = openrouter_config['API_KEY']

HEADERS = {
    'Content-Type': 'application/json',
    'X-goog-api-key': GEMINI_API_KEY
}


PARAMS = {'key': GEMINI_API_KEY}  # Use the actual API key provided
# headers = {'Content-Type': 'application/json'}

neondb_config = read_config()['PostgresDB']
connection_string = neondb_config['connection_string']

# ----------------------------------------------------------------------------
def call_gemini(payload: dict):
    """
    Try 2.0-flash first.  On HTTP 429 switch to 1.5-pro.
    Returns the parsed JSON response (raises after final failure).
    """

    # ---- first: hit 2.0-flash ---------------------------------------------
    for attempt in range(1, MAX_RETRIES_V2 + 1):
        resp = requests.post(GEMINI_V2_URL, json=payload,
                             headers=HEADERS, params=PARAMS, timeout=30)
        if resp.status_code == 200:
            return resp        # success
        if resp.status_code != 429:
            resp.raise_for_status()   # hard failure – bubble up
        time.sleep(2 ** attempt)      # 429 → back-off then retry

    # ---- still 429: fall back to 1.5-pro -----------------------------------
    for attempt in range(1, MAX_RETRIES_V1_5 + 1):
        resp = requests.post(GEMINI_V1_5_URL, json=payload,
                             headers=HEADERS, params=PARAMS, timeout=30)
        if resp.status_code == 200:
            return resp
        if resp.status_code != 429:
            resp.raise_for_status()
        time.sleep(2 ** attempt)

    # ---- nothing worked ----------------------------------------------------
    raise RuntimeError("Gemini API: exhausted retries on both 2.0-flash and 1.5-pro")

def save_to_db(table_name, df: pd.DataFrame):
    engine = create_engine(connection_string)
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    engine.dispose()

def query_to_df(query) -> pd.DataFrame:
    engine = create_engine(connection_string)
    df = pd.read_sql_query(query, con=engine)
    engine.dispose()
    return df


def hash_url(url):
    return mmh3.hash(url, signed=False)

import json, re, ast

def fix_unescaped_newlines(txt: str) -> str:
    """
    Replace naked \n or \r inside quoted strings with \\n.
    Quick heuristic: we’re inside a string if the number of double
    quotes seen so far is odd.
    """
    out, in_string = [], False
    for ch in txt:
        if ch == '"' and (not out or out[-1] != '\\'):
            in_string = not in_string
        if ch in '\r\n' and in_string:
            out.append('\\n')
        else:
            out.append(ch)
    return ''.join(out)

def escape_newlines_inside_strings(json_like: str) -> str:
    """
    Replace literal \\n / \\r that occur *inside* a quoted JSON string with \\n.
    Leaves newlines *between* tokens untouched (they're legal in JSON).
    """
    out, in_string, prev_backslash = [], False, False

    for ch in json_like:
        if ch == '"' and not prev_backslash:
            in_string = not in_string
        if in_string and ch in '\r\n':
            out.append('\\n')
        else:
            out.append(ch)
        prev_backslash = (ch == '\\') and not prev_backslash

    return ''.join(out)

def safe_json_loads(text: str) -> dict:
    text = escape_newlines_inside_strings(text)
    return json.loads(text)

def find_balanced_json(text: str) -> str:
    """
    Grab the FIRST balanced {...} block in `text`.
    Works even if there are paragraphs before/after, or if the model
    returned two JSON objects back-to-back.
    """
    # Strip code-fence prefix/suffix quickly
    text = text.strip()

    text = fix_unescaped_newlines(text.lstrip("```").rstrip("```"))

    if text.startswith("```"):
        # remove ```json or ``` plus the closing ```
        text = re.sub(r'^```[a-zA-Z0-9]*\n?', '', text)
        text = text.rsplit("```", 1)[0]

    # locate first '{'
    start = text.find("{")
    if start == -1:
        raise ValueError("No '{' found in response")

    # scan forward, tracking brace depth
    depth = 0
    for i, ch in enumerate(text[start:], start=start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:           # balanced!
                return text[start : i + 1]

    raise ValueError("No balanced JSON object found")

def coerce_to_valid_json(raw_json: str):
    """
    1. convert None/True/False → null/true/false
    2. quote unquoted keys (best-effort)
    3. parse with json, fall back to ast.literal_eval
    """
    # python literals → JSON
    raw_json = re.sub(r'\bNone\b', 'null', raw_json)
    raw_json = re.sub(r'\bTrue\b', 'true', raw_json)
    raw_json = re.sub(r'\bFalse\b', 'false', raw_json)

    # unquoted keys -> "key": (very rough, handles most simple cases)
    # raw_json = re.sub(r'([{,]\s*)([A-Za-z0-9_ ]+?)(\s*):',
    #                   lambda m: f'{m.group(1)}"{m.group(2).strip()}"{m.group(3)}:',
    #                   raw_json)

    try:
        return safe_json_loads(raw_json)
    except json.JSONDecodeError:
        # last resort: python-style dicts
        return ast.literal_eval(raw_json)

def parse_gemini(payload_response_text: str) -> dict:
    blob = find_balanced_json(payload_response_text)
    return coerce_to_valid_json(blob)



def get_raw_data() -> pd.DataFrame:
    # Get the current hour of the day
    current_hour = datetime.now().hour

    # Divide the day into 3 segments (24 hours / 3 segments = 8 hours per segment)
    segment = current_hour // 8

    # Choose location based on the current segment
    location = locations[segment % len(locations)]
    # location = locations[0]
    country_indeed = country_indeed_mapping[location]

    try:
        jobs: pd.DataFrame = scrape_jobs(
            # site_name=["indeed", "linkedin", "zip_recruiter", "glassdoor"],
            # site_name=["indeed", "linkedin", "zip_recruiter"],
            site_name=["linkedin"],
            # search_term="data",
            search_term=' Data Engineer OR Data Architect ',
            location=location,
            results_wanted=100,
            hours_old=168,
            # be wary the higher it is, the more likey you'll get blocked (rotating proxy can help tho)
            country_indeed=country_indeed,
            # proxy="http://jobspy:5a4vpWtj8EeJ2hoYzk@ca.smartproxy.com:20001",
            linkedin_fetch_description=True,
        )

        jobs['job_hash'] = jobs['job_url'].apply(hash_url)
        # Ensure date_posted is in datetime format
        jobs['date_posted'] = pd.to_datetime(jobs['date_posted'])

        # Populate null date_posted with current date
        jobs['date_posted'] = jobs['date_posted'].fillna(pd.Timestamp.now().normalize())
        jobs['is_deleted'] = 'N'

        #Filter out duplicates
        df = query_to_df("SELECT distinct job_hash FROM (SELECT job_hash FROM ja_jobs_raw_new union all SELECT job_hash FROM ja_jobs_raw) T")
        existing_job_hashes = set(df['job_hash'])
        jobs = jobs[~jobs['job_hash'].isin(existing_job_hashes)]

        # formatting for pandas
        pd.set_option("display.max_columns", None)
        pd.set_option("display.max_rows", None)
        pd.set_option("display.width", None)
        pd.set_option("display.max_colwidth", 50)  # set to 0 to see full job url / desc

        # 1: output to console
        # print(jobs)
    except Exception as e:
        print("Error: " + str(e))
        print(f"Error: {type(e)}")
        jobs = pd.DataFrame()

    return jobs



@task
async def get_jobs_data(params: InputParams) -> pd.DataFrame:
    try:
        jobs: pd.DataFrame = get_raw_data()
        save_to_db('ja_jobs_raw_new', jobs)

        # inferred_jobs = infer_from_rawdata()
        # save_to_db('ja_jobs_raw', inferred_jobs)

    except Exception as e:
        print(e)



def infer_from_rawdata(batch_size=5) -> pd.DataFrame:
    jobs = query_to_df("SELECT distinct * FROM ja_jobs_raw_new where job_hash not in (SELECT job_hash FROM ja_jobs_raw) order by date_posted desc limit {0}".format(batch_size))
    print("jobs count = ", str(len(jobs)))

    # Define key fields to check
    key_fields = [
        'country_inferred',
        'job_title_inferred',
        'company_name_inferred',
        'desired_tech_skills_inferred'
    ]

    # Iterate over each row in the DataFrame
    for index, row in jobs.iterrows():
        # Extract relevant fields
        title = row['title']
        company = row['company']
        location = row['location']
        description = row['description']
        company_url = row['company_url']

        input_text = (str(title) + " " + str(company) + " " + str(location) + " " + str(description) + " " + str(
            company_url)).replace("  ", " ").replace("\n", " ").replace("\t", " ")

        input_text = re.sub('\s+', ' ', input_text)

        # Set up the API request
        # Call Gemini API
        start_time = time.time()  # Start timing

        payload = {
            # <--- system-level rules go here
            "system_instruction": {
                "parts": [
                    {
                        "text": (
                            "You are an extraction engine. "
                            "Return ONE compact JSON object with exactly the keys I list. "
                            "Strings must NOT contain literal line-breaks – escape them as \\n. "
                            "No markdown, no code fences, no explanatory text. "
                            "If a value is missing use null."
                        )
                    }
                ]
            },
            "contents": [{"parts": [{"text": f"""  Extract and return these fields in a dictionary:
                    1. country
                    2.state
                    3.city
                    4.desired tech skills (as a list)
                    5.desired soft skills (as a list)
                    6.desired domain skills (as a list)
                    7. domains (as a list)
                    8.company sector
                    9.position seniority level
                    10. job type
                    11. job title
                    12. job description
                    13. job requirements
                    14. job responsibilities
                    15. job benefits
                    16. salary (if mentioned)
                    18. company name (if mentioned)
                    19. company description (if mentioned)
                    20. company website (if mentioned)
                    21. company size (if mentioned)
                    22. company industry (if mentioned)
                    23. company headquarters (if mentioned)
                    24. company employees (if mentioned)
                    25. company revenue (if mentioned)
                    , from this text  - {input_text} """}]}],
            # optional but very useful: tell Gemini you **only want JSON**
            "generation_config": {
                "response_mime_type": "application/json",
                "temperature": 0.0  # deterministic
            }
        }

        try_count = 1
        retry_delay = 5  # sleep for 5 seconds before retrying
        while try_count < 7:
            result_json_str = ""
            try:
                # response = requests.post(url, json=payload, headers=headers, params=params)
                response = call_gemini(payload)
                if response.status_code == 200:
                    result = response.json()
                    # print("result - ", result)
                    result_json_str = result['candidates'][0]['content']['parts'][0]['text']

                    # print("result_dict - ", result_json_str)
                    # 1️⃣  Remove BOM if it’s there (cheap and safe)
                    # result_json_str = result_json_str.lstrip('\ufeff')
                    #
                    # result_json_str = result_json_str.lstrip("```").rstrip("```")
                    #
                    # # 2️⃣  Lop off anything that appears before the first '{' or '['
                    # #     (covers unknown-length XSSI prefixes).
                    # result_json_str = re.sub(r'^[^\[{]*', '', result_json_str, count=1)
                    #
                    # result_json_str = remove_control_characters(result_json_str)
                    #
                    # # replace every ": None" (with optional spaces) by ": null"
                    # result_json_str = re.sub(r':\s*None\b', ': null', result_json_str)
                    #
                    # # print("result_json_str 2 - ", result_json_str)
                    # # result_dict = json.loads(result_json_str)
                    # result_dict = parse_gemini(result_json_str)

                    result_dict = parse_llm_json(result_json_str)

                    # print("result_dict - ", result_dict)
                    end_time = time.time()  # End timing
                    print(f"get time: {end_time - start_time} seconds")

                    fields_list = [
                        'country',
                        'state',
                        'city',
                        'desired tech skills',
                        'desired soft skills',
                        'desired domain skills',
                        'domains',
                        'company sector',
                        'position seniority level',
                        'job type',
                        'job title',
                        'job description',
                        'job requirements',
                        'job responsibilities',
                        'job benefits',
                        'salary',
                        'company name',
                        'company description',
                        'company website',
                        'company size',
                        'company industry',
                        'company headquarters',
                        'company employees',
                        'company revenue'
                    ]
                    field_suffix_list = []
                    for field in fields_list:
                        field_suffix = field.replace(" ", "_") + "_inferred"
                        if field not in result_dict:
                            jobs.at[index, field_suffix] = ""
                        else:
                            value = result_dict[field]
                            # Check if the value is a list
                            if isinstance(value, list):
                                # Convert the list to a string representation
                                try:
                                    value_str = ", ".join(str(item) for item in value)
                                except Exception as e:
                                    print(f"Error converting list {value} to string: {e}")
                                    value_str = "Unknown"
                                jobs.at[index, field_suffix] = value_str
                            else:
                                jobs.at[index, field_suffix] = str(value)
                        field_suffix_list.append(field_suffix)
                    # print(jobs[field_suffix_list])
                    break
                else:
                    print(f"API request failed with status code {response.status_code}. Retrying...")
                    try_count += 1
            except Exception as e:
                print(f"API request failed with exception {e}. Retrying...")
                print("PARSE ERROR:", e)
                print("RAW RESPONSE --------")
                print(result_json_str)
                print("----------------------")
                time.sleep(retry_delay)
                try_count += 1
                retry_delay *= 2

    # Filter out records where any key field is not inferred
    # for field in key_fields:
    #     jobs = jobs[jobs[field].astype(bool)]

    jobs = jobs[jobs['job_title_inferred'].str.len() > 0]
    return jobs



@task
async def ai_infer_raw_data():
    for i in range(1):
        inferred_jobs = infer_from_rawdata(batch_size=10)
        save_to_db('ja_jobs_raw', inferred_jobs)



@task
async def load_jobs_analyzer_site():
    print("Loading Jobs Analyzer site...")
    try:
        session = HTMLSession()
        response = session.get("https://jobs-analyzer.streamlit.app/")

        # Render the JavaScript. The timeout can be adjusted or removed.
        response.html.arender(timeout=20)

        print("Page loaded successfully!")
        session.close()
    except Exception as e:
        print(f"Error: {e}")

register_pipeline(
    id="jobs_pipeline",
    description="""This is a very jobby pipeline""",
    tasks=[get_jobs_data, ai_infer_raw_data, load_jobs_analyzer_site],
    # tasks= [ai_infer_raw_data],
    # tasks=[load_jobs_analyzer_site],
    triggers=[
        Trigger(
            id="daily3",
            name="Daily3",
            description="Run the pipeline 3 times daily",
            params=InputParams(),
            schedule=IntervalTrigger(
                # days=1,
                hours=8,
                # start_date=datetime(
                #     2023, 1, 1, 22, 30, tzinfo=tz.gettz("Europe/Brussels")
                # ),
            ),
        )
    ],
    params=InputParams,
)
