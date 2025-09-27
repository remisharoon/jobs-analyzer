import requests
import time
import json
import random
import re
from typing import Any
from config import read_config

try:
    from json_repair import repair_json  # type: ignore
except Exception:  # pragma: no cover - optional dependency guard
    repair_json = None

try:
    import dirtyjson  # type: ignore
except Exception:  # pragma: no cover - optional dependency guard
    dirtyjson = None

# Set up the API request
# url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent"

# Updated endpoint for Gemini 1.5 Flash
# model_name = "models/gemini-1.5-flash"
url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"



# params = {'key': 'your_api_key'}  # Replace 'your_api_key' with the actual API key
gemini_config = read_config()['GeminiPro']
# API_KEY = gemini_config['API_KEY']
API_KEY = random.choice([gemini_config['API_KEY_RH'], gemini_config['API_KEY_RHA']])

headers = {
    'Content-Type': 'application/json',
    'X-goog-api-key': API_KEY
}


params = {'key': API_KEY}  # Use the actual API key provided


def _strip_code_fences(text: str) -> str:
    # remove ```json ... ``` or ``` ... ``` fences safely
    text = re.sub(r"^\s*```(?:json)?\s*", "", text, flags=re.I)
    text = re.sub(r"\s*```\s*$", "", text)
    return text


def _extract_json_snippet(text: str) -> str:
    # find first { and matching last } to reduce hallucinated prefix/suffix
    start = text.find('{')
    end = text.rfind('}')
    if start != -1 and end != -1 and end > start:
        return text[start:end + 1]
    # fallback to original
    return text


def _safe_parse_json(text: str) -> Any:
    # Try strict parse
    try:
        return json.loads(text)
    except Exception:
        pass
    # Try substring between braces
    snippet = _extract_json_snippet(text)
    if snippet != text:
        try:
            return json.loads(snippet)
        except Exception:
            pass
    # Try JSON repair if available
    if repair_json is not None:
        try:
            repaired = repair_json(text)
            return json.loads(repaired)
        except Exception:
            pass
    # Try dirtyjson if available
    if dirtyjson is not None:
        try:
            return dirtyjson.loads(text)
        except Exception:
            pass
    # Last attempt: remove trailing commas (common error)
    try:
        cleaned = re.sub(r",\s*([}\]])", r"\1", snippet)
        return json.loads(cleaned)
    except Exception:
        return None


def infer(payload):
    result_dict: dict = {}
    try_count = 1
    retry_delay = 5  # sleep for 5 seconds before retrying

    while try_count < 5:
        try:
            # Call Gemini API
            start_time = time.time()  # Start timing

            response = requests.post(url, json=payload, headers=headers, params=params)
            if response.status_code == 200:
                result = response.json()
                text = result['candidates'][0]['content']['parts'][0]['text']
                # Clean code fences but keep structural whitespace
                text = _strip_code_fences(text)
                parsed = _safe_parse_json(text)
                if not isinstance(parsed, dict):
                    raise ValueError("Model did not return a JSON object")
                result_dict = parsed
                end_time = time.time()  # End timing
                print(f"get time: {end_time - start_time} seconds")
                break
            else:
                print(f"API request failed with status code {response.status_code}. Retrying...")
                try_count += 1
        except Exception as e:
            print(f"API request failed with exception {e}. Retrying...")
            time.sleep(retry_delay)
            try_count += 1
            retry_delay *= 2

    return result_dict
