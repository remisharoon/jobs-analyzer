import json
import time

from utils.llm_client import call_llm


def infer(payload):
    system_text = ""
    user_text = ""

    if "system_instruction" in payload:
        parts = payload["system_instruction"].get("parts", [])
        system_text = parts[0].get("text", "") if parts else ""

    if "contents" in payload:
        parts = payload["contents"][0].get("parts", []) if payload["contents"] else []
        user_text = parts[0].get("text", "") if parts else ""

    json_mode = False
    gen_config = payload.get("generation_config", {})
    if gen_config.get("response_mime_type") == "application/json":
        json_mode = True

    start_time = time.time()
    result_text = call_llm(
        system_prompt=system_text,
        user_prompt=user_text,
        json_mode=json_mode,
        temperature=gen_config.get("temperature", 0.0),
    )
    end_time = time.time()
    print(f"get time: {end_time - start_time} seconds")

    result_text = result_text.lstrip("```").rstrip("```").replace("json", "").replace("\n", "").replace("\t", "").replace("\r", "")
    result_dict = json.loads(result_text)
    return result_dict
