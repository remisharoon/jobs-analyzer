import json
import logging
import time

import requests

from config import read_config

logger = logging.getLogger(__name__)

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_MODEL = "google/gemini-2.0-flash-001"
MAX_RETRIES = 3
LLM_DISABLED_DUE_TO_BILLING = False

_config = read_config()
_openrouter_config = _config["openrouter"]
API_KEY = _openrouter_config["API_KEY"]

_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}",
}


def call_llm(
    system_prompt: str,
    user_prompt: str,
    model: str = DEFAULT_MODEL,
    temperature: float = 0.0,
    json_mode: bool = True,
    max_tokens: int = 4096,
) -> str:
    global LLM_DISABLED_DUE_TO_BILLING

    if LLM_DISABLED_DUE_TO_BILLING:
        raise RuntimeError("OpenRouter disabled due to 402 billing error; using regex-only fallback")

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    if json_mode:
        messages.append({"role": "system", "content": "You MUST respond with valid JSON only. No markdown, no code fences, no explanatory text outside the JSON object."})
    messages.append({"role": "user", "content": user_prompt})

    body: dict = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                OPENROUTER_URL,
                json=body,
                headers=_HEADERS,
                timeout=60,
            )
            if resp.status_code == 200:
                data = resp.json()
                choices = data.get("choices") if isinstance(data, dict) else None
                if choices and isinstance(choices, list) and choices[0].get("message", {}).get("content") is not None:
                    return choices[0]["message"]["content"]
                logger.warning("OpenRouter returned 200 without choices; payload keys=%s", list(data.keys()) if isinstance(data, dict) else type(data))
                if attempt == MAX_RETRIES:
                    raise RuntimeError(f"OpenRouter malformed success response: {str(data)[:500]}")
                time.sleep(2 ** attempt)
                continue
            if resp.status_code == 429:
                wait = 2 ** attempt
                logger.warning("OpenRouter 429, retrying in %ds (attempt %d/%d)", wait, attempt, MAX_RETRIES)
                time.sleep(wait)
                continue
            if resp.status_code == 402:
                LLM_DISABLED_DUE_TO_BILLING = True
                logger.error("OpenRouter 402 Payment Required detected; disabling LLM calls for this run")
                raise RuntimeError("OpenRouter 402 Payment Required")
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("OpenRouter request failed (attempt %d/%d): %s", attempt, MAX_RETRIES, exc)
            if attempt == MAX_RETRIES:
                raise
            time.sleep(2 ** attempt)

    raise RuntimeError(f"OpenRouter: exhausted {MAX_RETRIES} retries for model {model}")
