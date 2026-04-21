#!/usr/bin/env python3
"""
dexscreener_tracker.py

Pulls tokens with DexScreener profiles on Solana/Ethereum/BSC/Base,
filters by liquidity >= $25k, market cap $100k-$15M, 24h volume >= $100k.

Outputs:
  1. ./data/dexscreener_tracker.csv  (committed back to repo by Actions)
  2. POSTs {chain, ca} pairs to Apps Script Web App for ingestion into
     `alon's playbook` Sheet1. Existing CRM enrichment pipeline fills the rest.

Env vars:
  APPS_SCRIPT_URL  - Google Apps Script Web App URL (required)
"""

import csv
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

# ---------- config ----------

CHAINS = {"solana", "ethereum", "bsc", "base"}

MIN_LIQUIDITY = 25_000
MIN_MCAP = 100_000
MAX_MCAP = 15_000_000
MIN_VOLUME_24H = 100_000

# relative path - writes into repo, gets committed by the workflow
CSV_PATH = Path("data/dexscreener_tracker.csv")

APPS_SCRIPT_URL = os.environ.get("APPS_SCRIPT_URL", "").strip()

API_BASE = "https://api.dexscreener.com"
REQUEST_TIMEOUT = 20
RATE_LIMIT_SLEEP = 0.25

CHAIN_DISPLAY = {
    "solana": "Solana",
    "ethereum": "Ethereum",
    "base": "Base",
    "bsc": "BSC",
}

CSV_COLUMNS = [
    "first_seen", "last_seen", "chain", "ca", "symbol", "name",
    "liquidity_usd", "market_cap", "volume_24h", "price_usd", "dex_url",
]

# ---------- logging ----------

def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)

# ---------- http ----------

def http_get(url):
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))

def http_post_json(url, payload):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        return resp.read().decode("utf-8")

# ---------- dexscreener ----------

def fetch_profiles():
    log("fetching token profiles")
    try:
        data = http_get(f"{API_BASE}/token-profiles/latest/v1")
        return data if isinstance(data, list) else []
    except Exception as e:
        log(f"error fetching profiles: {e}")
        return []

def fetch_pair_data(chain, address):
    try:
        data = http_get(f"{API_BASE}/tokens/v1/{chain}/{address}")
        return data if isinstance(data, list) else []
    except urllib.error.HTTPError as e:
        if e.code == 429:
            log(f"rate limited on {chain}/{address}, sleeping 5s")
            time.sleep(5)
        return []
    except Exception as e:
        log(f"error fetching {chain}/{address}: {e}")
        return []

def best_pair(pairs):
    valid = [p for p in pairs if p.get("liquidity", {}).get("usd")]
    if not valid:
        return None
    return max(valid, key=lambda p: p["liquidity"]["usd"])

# ---------- filtering ----------

def passes_filters(pair):
    if not pair:
        return False
    if pair.get("chainId") not in CHAINS:
        return False
    liq = (pair.get("liquidity") or {}).get("usd") or 0
    if liq < MIN_LIQUIDITY:
        return False
    mcap = pair.get("marketCap") or 0
    if mcap < MIN_MCAP or mcap > MAX_MCAP:
        return False
    vol_24h = (pair.get("volume") or {}).get("h24") or 0
    if vol_24h < MIN_VOLUME_24H:
        return False
    return True

def row_from_pair(pair, now_iso):
    return {
        "first_seen": now_iso,
        "last_seen": now_iso,
        "chain": pair.get("chainId", ""),
        "ca": pair.get("baseToken", {}).get("address", ""),
        "symbol": pair.get("baseToken", {}).get("symbol", ""),
        "name": pair.get("baseToken", {}).get("name", ""),
        "liquidity_usd": round((pair.get("liquidity") or {}).get("usd") or 0, 2),
        "market_cap": round(pair.get("marketCap") or 0, 2),
        "volume_24h": round((pair.get("volume") or {}).get("h24") or 0, 2),
        "price_usd": pair.get("priceUsd", ""),
        "dex_url": pair.get("url", ""),
    }

# ---------- csv i/o ----------

def load_existing_csv():
    if not CSV_PATH.exists():
        return {}
    existing = {}
    try:
        with open(CSV_PATH, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (row.get("chain", ""), row.get("ca", ""))
                if key[1]:
                    existing[key] = row
    except Exception as e:
        log(f"error reading existing CSV: {e}")
    return existing

def write_csv(rows_by_key):
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = CSV_PATH.with_suffix(".csv.tmp")
    with open(tmp_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        sorted_rows = sorted(
            rows_by_key.values(),
            key=lambda r: r.get("first_seen", ""),
            reverse=True,
        )
        for row in sorted_rows:
            writer.writerow({col: row.get(col, "") for col in CSV_COLUMNS})
    tmp_path.replace(CSV_PATH)

# ---------- apps script push ----------

def push_to_apps_script(new_rows):
    if not APPS_SCRIPT_URL:
        log("APPS_SCRIPT_URL env var not set, skipping push")
        return
    if not new_rows:
        log("no new rows to push")
        return

    payload_rows = []
    for r in new_rows:
        chain_key = (r.get("chain") or "").lower()
        chain_display = CHAIN_DISPLAY.get(chain_key, chain_key)
        ca = r.get("ca", "")
        if not ca or not chain_display:
            continue
        payload_rows.append({"chain": chain_display, "ca": ca})

    if not payload_rows:
        log("no valid rows to push after filtering")
        return

    try:
        resp = http_post_json(
            APPS_SCRIPT_URL,
            {"source": "dexscreener_tracker", "rows": payload_rows},
        )
        log(f"pushed {len(payload_rows)} rows to Apps Script: {resp[:300]}")
    except Exception as e:
        log(f"error pushing to Apps Script: {e}")

# ---------- main ----------

def main():
    started = time.time()
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    profiles = fetch_profiles()
    log(f"got {len(profiles)} profiles")

    profiles = [p for p in profiles if p.get("chainId") in CHAINS]
    log(f"{len(profiles)} profiles on target chains")

    seen = set()
    candidates = []
    for prof in profiles:
        chain = prof.get("chainId")
        ca = prof.get("tokenAddress")
        if not chain or not ca:
            continue
        key = (chain, ca)
        if key in seen:
            continue
        seen.add(key)

        pairs = fetch_pair_data(chain, ca)
        time.sleep(RATE_LIMIT_SLEEP)
        pair = best_pair(pairs)
        if passes_filters(pair):
            candidates.append(pair)

    log(f"{len(candidates)} pairs passed filters")

    candidates.sort(
        key=lambda p: (p.get("volume") or {}).get("h24") or 0,
        reverse=True,
    )
    candidates = candidates[:100]

    existing = load_existing_csv()
    new_rows_for_push = []

    for pair in candidates:
        row = row_from_pair(pair, now_iso)
        key = (row["chain"], row["ca"])
        if key in existing:
            prev = existing[key]
            row["first_seen"] = prev.get("first_seen", now_iso)
            existing[key] = row
        else:
            existing[key] = row
            new_rows_for_push.append(row)

    write_csv(existing)
    log(f"CSV: {len(existing)} total rows, {len(new_rows_for_push)} new this run")

    push_to_apps_script(new_rows_for_push)

    log(f"done in {time.time() - started:.1f}s")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"FATAL: {e}")
        sys.exit(1)
