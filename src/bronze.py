import os
import json
import time
import requests
from datetime import datetime, timezone
from utils import get_api_key, get_base_paths

BASE_URL = "https://api.electricitymap.org/v3"
ZONE = "FR"


def fetch_data(url, headers, retries=3):
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception:
            print(f"[Bronze] Attempt {attempt + 1} failed for {url}")
            time.sleep(2)

    print(f"[Bronze] All retries failed for {url}")
    return None


def fetch_electricity_flows(api_key):
    url = f"{BASE_URL}/power-breakdown/latest?zone={ZONE}"
    headers = {"auth-token": api_key}
    return fetch_data(url, headers), url


def fetch_electricity_mix(api_key):
    url = f"{BASE_URL}/carbon-intensity/latest?zone={ZONE}"
    headers = {"auth-token": api_key}
    return fetch_data(url, headers), url


def save_raw(data, source_url, stream_name, ingestion_ts, paths):
    year = ingestion_ts.strftime("%Y")
    month = ingestion_ts.strftime("%m")
    day = ingestion_ts.strftime("%d")

    folder = os.path.join(
        paths["bronze"],
        stream_name,
        f"year={year}",
        f"month={month}",
        f"day={day}",
    )
    os.makedirs(folder, exist_ok=True)

    payload = {
        "ingestion_timestamp": ingestion_ts.isoformat(),
        "source_url": source_url,
        "data": data,
    }

    filename = f"{stream_name}_{ingestion_ts.strftime('%H%M%S')}.json"
    filepath = os.path.join(folder, filename)

    with open(filepath, "w") as f:
        json.dump(payload, f)

    print(f"[Bronze] Saved {stream_name} to {filepath}")


def run_bronze():
    api_key = get_api_key()
    paths = get_base_paths()
    ingestion_ts = datetime.now(timezone.utc)

    print(f"[Bronze] Starting ingestion at {ingestion_ts.isoformat()}")

    flows_data, flows_url = fetch_electricity_flows(api_key)
    if flows_data:
        save_raw(flows_data, flows_url, "electricity_flows", ingestion_ts, paths)

    mix_data, mix_url = fetch_electricity_mix(api_key)
    if mix_data:
        save_raw(mix_data, mix_url, "electricity_mix", ingestion_ts, paths)

    print("[Bronze] Ingestion complete")


if __name__ == "__main__":
    run_bronze()