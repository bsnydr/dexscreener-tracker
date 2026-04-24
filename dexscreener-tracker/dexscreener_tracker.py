#!/usr/bin/env python3
"""
DexScreener tracker - v3 (multi-endpoint fan-out + filtering)

Fans out across multiple DexScreener feeders (search by chain, latest
profiles, latest boosts, top boosts), dedupes, enriches each candidate
with /latest/dex/tokens/{address} for full liquidity/mcap/volume data,
applies the user's filter thresholds, then POSTs new rows to the
Apps Script ingest endpoint.

Filters (matching DexScreener website screener):
  - Chains: Solana, Ethereum, BSC, Base
  - Liquidity USD >= $25,000
  - Market cap: $100,000 - $15,000,000
  - 24h volume USD >= $100,000

Env vars (set in GitHub Actions workflow):
  - APPS_SCRIPT_URL: webapp URL of the Apps Script /exec endpoint
"""

import csv
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

# ---------- CONFIG ----------

API_BASE = "https://api.dexscreener.com"
APPS_SCRIPT_URL = os.environ.get("APPS_SCRIPT_URL", "").strip()
CSV_PATH = "data/dexscreener_tracker.csv"
REQUEST_TIMEOUT = 30

# Filter thresholds (match the screener UI)
TARGET_CHAINS = {"solana", "ethereum", "bsc", "base"}
MIN_LIQUIDITY_USD = 25_000
MIN_MARKET_CAP = 100_000
MAX_MARKET_CAP = 15_000_000
MIN_VOLUME_24H = 100_000

# Polite pacing - DexScreener rate limits are 60/min for profile/boost
# endpoints and 300/min for search/tokens. Sleep between calls to stay safe.
SLEEP_BETWEEN_CALLS = 0.5  # seconds
SLEEP_BETWEEN_ENRICH = 0.25

# DexScreener 403s default Python user-agents from cloud IPs
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


# ---------- LOGGING ----------

def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)


# ---------- HTTP ----------

def http_get(url):
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": BROWSER_UA,
        },
    )
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def safe_get(url, label):
    """GET with one retry on 403/429."""
    for attempt in (1, 2):
        try:
            return http_get(url)
        except urllib.error.HTTPError as e:
            if e.code in (403, 429) and attempt == 1:
                log(f"  {label}: HTTP {e.code}, retrying in 3s")
                time.sleep(3)
                continue
            log(f"  {label}: HTTP {e.code}")
            return None
        except Exception as e:
            log(f"  {label}: {type(e).__name__} {e}")
            return None
    return None


# ---------- FEEDERS ----------

def feeder_token_profiles():
    log("feeder: /token-profiles/latest/v1")
    data = safe_get(f"{API_BASE}/token-profiles/latest/v1", "profiles")
    if not isinstance(data, list):
        return []
    out = [{"chainId": p.get("chainId", "").lower(),
            "tokenAddress": p.get("tokenAddress", "")}
           for p in data if p.get("tokenAddress")]
    log(f"  -> {len(out)} candidates")
    return out


def feeder_boosts(endpoint, label):
    log(f"feeder: {endpoint}")
    data = safe_get(f"{API_BASE}{endpoint}", label)
    if not isinstance(data, list):
        return []
    out = [{"chainId": p.get("chainId", "").lower(),
            "tokenAddress": p.get("tokenAddress", "")}
           for p in data if p.get("tokenAddress")]
    log(f"  -> {len(out)} candidates")
    return out


def feeder_search(query):
    log(f"feeder: /latest/dex/search?q={query}")
    data = safe_get(f"{API_BASE}/latest/dex/search?q={query}", f"search-{query}")
    if not data or not isinstance(data, dict):
        return []
    pairs = data.get("pairs") or []
    out = []
    seen = set()
    for p in pairs:
        chain = (p.get("chainId") or "").lower()
        addr = (p.get("baseToken") or {}).get("address") or ""
        if not addr:
            continue
        key = (chain, addr.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append({"chainId": chain, "tokenAddress": addr})
    log(f"  -> {len(out)} candidates")
    return out


# ---------- ENRICHMENT + FILTER ----------

def enrich_and_filter(candidate):
    """
    Fetch full pair data for one candidate. Returns dict if it passes
    filters, None otherwise. Picks the highest-liquidity pair for the
    token (mirrors what the website does).
    """
    chain = candidate["chainId"]
    addr = candidate["tokenAddress"]

    if chain not in TARGET_CHAINS:
        return None

    url = f"{API_BASE}/latest/dex/tokens/{addr}"
    data = safe_get(url, f"enrich {addr[:8]}")
    if not data or not isinstance(data, dict):
        return None

    pairs = data.get("pairs") or []
    if not pairs:
        return None

    # Filter to pairs on the candidate's chain (a token can exist on multiple)
    pairs = [p for p in pairs if (p.get("chainId") or "").lower() == chain]
    if not pairs:
        return None

    # Pick the most liquid pair
    pairs.sort(
        key=lambda p: ((p.get("liquidity") or {}).get("usd") or 0),
        reverse=True,
    )
    best = pairs[0]

    liq = (best.get("liquidity") or {}).get("usd") or 0
    mcap = best.get("marketCap") or best.get("fdv") or 0
    vol24 = (best.get("volume") or {}).get("h24") or 0

    # Apply filters
    if liq < MIN_LIQUIDITY_USD:
        return None
    if mcap < MIN_MARKET_CAP or mcap > MAX_MARKET_CAP:
        return None
    if vol24 < MIN_VOLUME_24H:
        return None

    return {
        "chain": chain,
        "ca": addr,
        "symbol": (best.get("baseToken") or {}).get("symbol") or "",
        "name": (best.get("baseToken") or {}).get("name") or "",
        "liquidity_usd": liq,
        "market_cap": mcap,
        "volume_24h": vol24,
    }


# ---------- CSV ----------

def load_existing_csv():
    """Returns set of seen (chain, ca_lower) tuples."""
    seen = set()
    if not os.path.exists(CSV_PATH):
        return seen
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            chain = (row.get("chain") or "").lower()
            ca = (row.get("ca") or "").lower()
            if chain and ca:
                seen.add((chain, ca))
    return seen


def append_to_csv(rows):
    os.makedirs(os.path.dirname(CSV_PATH) or ".", exist_ok=True)
    file_exists = os.path.exists(CSV_PATH)
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        fieldnames = [
            "first_seen_utc", "chain", "ca", "symbol", "name",
            "liquidity_usd", "market_cap", "volume_24h",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        for r in rows:
            writer.writerow({
                "first_seen_utc": ts,
                "chain": r["chain"],
                "ca": r["ca"],
                "symbol": r["symbol"],
                "name": r["name"],
                "liquidity_usd": int(r["liquidity_usd"]),
                "market_cap": int(r["market_cap"]),
                "volume_24h": int(r["volume_24h"]),
            })


# ---------- APPS SCRIPT POST ----------

def push_to_apps_script(rows):
    if not APPS_SCRIPT_URL:
        log("no APPS_SCRIPT_URL set, skipping push")
        return
    if not rows:
        log("no new rows to push")
        return

    # Map chain to display format the script expects
    chain_display = {
        "solana": "Solana",
        "ethereum": "Ethereum",
        "bsc": "BSC",
        "base": "Base",
    }
    payload_rows = [
        {"chain": chain_display.get(r["chain"], r["chain"]), "ca": r["ca"]}
        for r in rows
    ]
    payload = {"source": "dexscreener_tracker", "rows": payload_rows}

    try:
        req = urllib.request.Request(
            APPS_SCRIPT_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            body = resp.read().decode("utf-8")
            log(f"pushed {len(rows)} rows -> {body[:200]}")
    except Exception as e:
        log(f"push failed: {type(e).__name__} {e}")


# ---------- MAIN ----------

def main():
    t0 = time.time()
    log("starting dexscreener tracker v3")

    # Step 1: fan out across feeders
    candidates = []
    candidates += feeder_token_profiles()
    time.sleep(SLEEP_BETWEEN_CALLS)
    candidates += feeder_boosts("/token-boosts/latest/v1", "boosts-latest")
    time.sleep(SLEEP_BETWEEN_CALLS)
    candidates += feeder_boosts("/token-boosts/top/v1", "boosts-top")
    time.sleep(SLEEP_BETWEEN_CALLS)
    for chain in ("solana", "base", "bsc", "ethereum"):
        candidates += feeder_search(chain)
        time.sleep(SLEEP_BETWEEN_CALLS)

    # Step 2: dedupe and filter to target chains BEFORE expensive enrichment
    seen_keys = set()
    unique = []
    for c in candidates:
        chain = c["chainId"]
        addr = c["tokenAddress"]
        if chain not in TARGET_CHAINS:
            continue
        key = (chain, addr.lower())
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique.append(c)
    log(f"after dedupe + chain filter: {len(unique)} unique candidates")

    # Step 3: drop ones already in our CSV - no need to re-enrich them
    existing = load_existing_csv()
    fresh = [c for c in unique if (c["chainId"], c["tokenAddress"].lower()) not in existing]
    log(f"after CSV dedupe: {len(fresh)} fresh candidates ({len(unique) - len(fresh)} already tracked)")

    # Step 4: enrich each fresh candidate, apply numeric filters
    log(f"enriching {len(fresh)} candidates...")
    passed = []
    for i, c in enumerate(fresh, 1):
        result = enrich_and_filter(c)
        if result:
            passed.append(result)
            log(f"  [{i}/{len(fresh)}] PASS {result['chain']} {result['symbol']:<12} "
                f"liq=${int(result['liquidity_usd']):>10,} "
                f"mcap=${int(result['market_cap']):>11,} "
                f"vol=${int(result['volume_24h']):>11,}")
        time.sleep(SLEEP_BETWEEN_ENRICH)

    log(f"{len(passed)} candidates passed filters")

    # Step 5: write to CSV
    if passed:
        append_to_csv(passed)
        log(f"appended {len(passed)} rows to {CSV_PATH}")

    # Step 6: push to Apps Script
    push_to_apps_script(passed)

    elapsed = time.time() - t0
    log(f"done in {elapsed:.1f}s")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"fatal: {type(e).__name__} {e}")
        sys.exit(1)
