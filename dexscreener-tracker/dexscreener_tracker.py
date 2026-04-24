#!/usr/bin/env python3
"""
DexScreener tracker - v4 (template matching + ATH tracking + vol baseline)

What's new vs v3:
  - Loads templates.json from repo
  - Captures vol_at_ingest, price_at_ingest, change_h1/h6/h24, age at ingest
  - Evaluates template rules, picks A/B/C or fallback
  - POSTs richer payload to Apps Script (chain, ca, vol_at_ingest, price_at_ingest, template, template_flag)

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
TEMPLATES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "templates.json")
REQUEST_TIMEOUT = 30

TARGET_CHAINS = {"solana", "ethereum", "bsc", "base"}
MIN_LIQUIDITY_USD = 25_000
MIN_MARKET_CAP = 100_000
MAX_MARKET_CAP = 15_000_000
MIN_VOLUME_24H = 100_000

SLEEP_BETWEEN_CALLS = 0.5
SLEEP_BETWEEN_ENRICH = 0.25

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
        headers={"Accept": "application/json", "User-Agent": BROWSER_UA},
    )
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def safe_get(url, label):
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


# ---------- TEMPLATES ----------

def load_templates():
    try:
        with open(TEMPLATES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log(f"WARNING: failed to load {TEMPLATES_PATH}: {e}")
        return None


def evaluate_rules(token_data, templates_config):
    """
    Apply template rules to token data.
    Returns dict {template: 'A'|'B'|'C', flag: '' or 'FALLBACK_NO_MATCH'}.
    """
    if not templates_config:
        return {"template": "A", "flag": "NO_TEMPLATES_CONFIG"}

    for rule in templates_config.get("rules", []):
        if rule_matches(rule.get("conditions", {}), token_data):
            return {"template": rule["template"], "flag": ""}

    fallback = templates_config.get("fallback", {})
    return {
        "template": fallback.get("template", "A"),
        "flag": fallback.get("flag", "FALLBACK_NO_MATCH"),
    }


def rule_matches(conditions, td):
    age = td.get("age_days")
    mcap = td.get("market_cap") or 0
    vol = td.get("volume_24h") or 0
    vol_ingest = td.get("vol_at_ingest") or 0
    price = td.get("price") or 0
    price_max = td.get("price_max_seen") or price
    ch_h1 = td.get("change_h1")
    ch_h6 = td.get("change_h6")
    ch_h24 = td.get("change_h24")

    if "min_age_days" in conditions:
        if age is None or age < conditions["min_age_days"]:
            return False
    if "max_age_days" in conditions:
        if age is None or age >= conditions["max_age_days"]:
            return False

    if "min_market_cap" in conditions and mcap < conditions["min_market_cap"]:
        return False

    if "min_vol_ratio_vs_ingest" in conditions:
        if vol_ingest <= 0:
            return False
        ratio = vol / vol_ingest
        if ratio < conditions["min_vol_ratio_vs_ingest"]:
            return False

    if "min_drawdown_from_max_pct" in conditions:
        if price_max <= 0:
            return False
        drawdown = ((price_max - price) / price_max) * 100
        if drawdown < conditions["min_drawdown_from_max_pct"]:
            return False

    if "min_change_h1_pct" in conditions:
        if ch_h1 is None or ch_h1 < conditions["min_change_h1_pct"]:
            return False
    if "min_change_h6_pct" in conditions:
        if ch_h6 is None or ch_h6 < conditions["min_change_h6_pct"]:
            return False
    if "min_change_h24_pct" in conditions:
        if ch_h24 is None or ch_h24 < conditions["min_change_h24_pct"]:
            return False

    return True


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

    pairs = [p for p in pairs if (p.get("chainId") or "").lower() == chain]
    if not pairs:
        return None

    pairs.sort(
        key=lambda p: ((p.get("liquidity") or {}).get("usd") or 0),
        reverse=True,
    )
    best = pairs[0]

    liq = (best.get("liquidity") or {}).get("usd") or 0
    mcap = best.get("marketCap") or best.get("fdv") or 0
    vol24 = (best.get("volume") or {}).get("h24") or 0

    if liq < MIN_LIQUIDITY_USD:
        return None
    if mcap < MIN_MARKET_CAP or mcap > MAX_MARKET_CAP:
        return None
    if vol24 < MIN_VOLUME_24H:
        return None

    price_usd = float(best.get("priceUsd") or 0)
    change = best.get("priceChange") or {}
    pair_created_ms = best.get("pairCreatedAt") or 0

    age_days = None
    if pair_created_ms > 0:
        age_ms = (datetime.now(timezone.utc).timestamp() * 1000) - pair_created_ms
        age_days = int(age_ms / (1000 * 60 * 60 * 24))

    return {
        "chain": chain,
        "ca": addr,
        "symbol": (best.get("baseToken") or {}).get("symbol") or "",
        "name": (best.get("baseToken") or {}).get("name") or "",
        "liquidity_usd": liq,
        "market_cap": mcap,
        "volume_24h": vol24,
        "price": price_usd,
        "change_h1": change.get("h1"),
        "change_h6": change.get("h6"),
        "change_h24": change.get("h24"),
        "age_days": age_days,
    }


# ---------- CSV ----------

def load_existing_csv():
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
            "vol_at_ingest", "price_at_ingest", "age_days_at_ingest",
            "template", "template_flag",
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
                "vol_at_ingest": int(r["volume_24h"]),
                "price_at_ingest": r.get("price") or 0,
                "age_days_at_ingest": r.get("age_days") if r.get("age_days") is not None else "",
                "template": r.get("template", ""),
                "template_flag": r.get("template_flag", ""),
            })


# ---------- APPS SCRIPT POST ----------

def push_to_apps_script(rows):
    if not APPS_SCRIPT_URL:
        log("no APPS_SCRIPT_URL set, skipping push")
        return
    if not rows:
        log("no new rows to push")
        return

    chain_display = {
        "solana": "Solana",
        "ethereum": "Ethereum",
        "bsc": "BSC",
        "base": "Base",
    }
    payload_rows = []
    for r in rows:
        payload_rows.append({
            "chain": chain_display.get(r["chain"], r["chain"]),
            "ca": r["ca"],
            "vol_at_ingest": int(r["volume_24h"]),
            "price_at_ingest": r.get("price") or 0,
            "template": r.get("template", ""),
            "template_flag": r.get("template_flag", ""),
        })
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
    log("starting dexscreener tracker v4")

    templates_config = load_templates()
    if templates_config:
        log(f"loaded {len(templates_config.get('rules', []))} rules from {TEMPLATES_PATH}")
    else:
        log("WARNING: no templates loaded, all rows will get fallback")

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

    existing = load_existing_csv()
    fresh = [c for c in unique if (c["chainId"], c["tokenAddress"].lower()) not in existing]
    log(f"after CSV dedupe: {len(fresh)} fresh candidates ({len(unique) - len(fresh)} already tracked)")

    log(f"enriching {len(fresh)} candidates...")
    passed = []
    template_counts = {"A": 0, "B": 0, "C": 0, "FALLBACK": 0}

    for i, c in enumerate(fresh, 1):
        result = enrich_and_filter(c)
        if not result:
            time.sleep(SLEEP_BETWEEN_ENRICH)
            continue

        td_for_rules = dict(result)
        td_for_rules["vol_at_ingest"] = result["volume_24h"]
        td_for_rules["price_max_seen"] = result["price"]

        decision = evaluate_rules(td_for_rules, templates_config)
        result["template"] = decision["template"]
        result["template_flag"] = decision["flag"]

        if decision["flag"]:
            template_counts["FALLBACK"] += 1
        else:
            template_counts[decision["template"]] += 1

        passed.append(result)
        flag_str = f" [{decision['flag']}]" if decision["flag"] else ""
        log(f"  [{i}/{len(fresh)}] PASS {result['chain']} {result['symbol']:<12} "
            f"liq=${int(result['liquidity_usd']):>10,} "
            f"mcap=${int(result['market_cap']):>11,} "
            f"vol=${int(result['volume_24h']):>11,} "
            f"-> {decision['template']}{flag_str}")
        time.sleep(SLEEP_BETWEEN_ENRICH)

    log(f"{len(passed)} candidates passed filters")
    log(f"templates: A={template_counts['A']} B={template_counts['B']} "
        f"C={template_counts['C']} fallback={template_counts['FALLBACK']}")

    if passed:
        append_to_csv(passed)
        log(f"appended {len(passed)} rows to {CSV_PATH}")

    push_to_apps_script(passed)

    elapsed = time.time() - t0
    log(f"done in {elapsed:.1f}s")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"fatal: {type(e).__name__} {e}")
        sys.exit(1)
