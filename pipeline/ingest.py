"""
Production-ready Polymarket ingestion pipeline.

Features:
- Pagination over Gamma API
- Retry + exponential backoff
- Safe JSON parsing
- Batch upserts (fast)
- Structured logging
- Run metrics
"""

import os
import json
import time
import logging
import requests
from datetime import datetime, timezone
from supabase import create_client, Client
from dotenv import load_dotenv

# ============================================================
# Setup
# ============================================================
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

GAMMA_API = "https://gamma-api.polymarket.com"


# ============================================================
# Helpers
# ============================================================
def safe_json_load(value):
    try:
        return json.loads(value) if isinstance(value, str) else value
    except Exception:
        return []


# ============================================================
# Fetch with retry
# ============================================================
def fetch_active_markets(limit=500, offset=0, retries=3):
    url = f"{GAMMA_API}/markets"
    params = {
        "active": "true",
        "closed": "false",
        "order": "volume_24hr",
        "ascending": "false",
        "limit": limit,
        "offset": offset,
    }

    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Fetch failed (attempt {attempt+1}): {e}")
            time.sleep(2 * (attempt + 1))

    logger.error("Failed to fetch markets after retries")
    return []


# ============================================================
# Parsing
# ============================================================
def parse_outcomes_with_prices(market: dict):
    outcomes_raw = market.get("outcomes", "[]")
    prices_raw   = market.get("outcomePrices", "[]")

    outcomes_list = safe_json_load(outcomes_raw) or []
    prices_list   = safe_json_load(prices_raw) or []

    # Convert to float safely
    parsed_prices = []
    for p in prices_list:
        try:
            parsed_prices.append(float(p))
        except Exception:
            parsed_prices.append(0.0)

    return [
        {"name": o, "price": p}
        for o, p in zip(outcomes_list, parsed_prices)
    ]

def parse_market(raw_market: dict) -> dict:
    """
    Convert a raw Polymarket market into our DB schema format.
    Captures previous outcome price for probability change tracking.
    """
    market_id = raw_market["id"]
    new_outcomes = parse_outcomes_with_prices(raw_market)

    # Fetch previous outcome price for momentum scoring
    prev_price = None
    try:
        existing = supabase.table("markets").select("outcomes").eq("id", market_id).execute()
        if existing.data:
            old_outcomes = existing.data[0].get("outcomes", "[]")
            if isinstance(old_outcomes, str):
                old_outcomes = json.loads(old_outcomes)
            if old_outcomes and len(old_outcomes) > 0:
                prev_price = float(old_outcomes[0].get("price", 0))
    except Exception:
        prev_price = None

    return {
        "id": market_id,
        "slug": raw_market.get("slug"),
        "question": raw_market["question"],
        "category": raw_market.get("category"),
        "tags": json.dumps(raw_market.get("tags", [])),
        "outcomes": json.dumps(new_outcomes),
        "volume_24hr": float(raw_market.get("volume24Hr", 0) or 0),
        "volume_total": float(raw_market.get("volume", 0) or 0),
        "liquidity": float(raw_market.get("liquidity", 0) or 0),
        "end_date": raw_market.get("endDate") or None,
        "start_date": raw_market.get("startDate") or None,
        "active": raw_market.get("active", True),
        "closed": raw_market.get("closed", False),
        "prev_outcome_price": prev_price,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

# ============================================================
# Batch upsert
# ============================================================
def upsert_markets_batch(records: list[dict]):
    if not records:
        return

    try:
        supabase.table("markets").upsert(
            records,
            on_conflict="id"
        ).execute()
    except Exception as e:
        logger.error(f"Batch upsert failed: {e}")


# ============================================================
# Main ingestion
# ============================================================
def ingest_all_markets(max_markets=10000):
    offset = 0
    total_ingested = 0
    total_failed = 0

    start_time = time.time()
    logger.info("Starting ingestion pipeline...")

    while True:
        markets_page = fetch_active_markets(limit=500, offset=offset)

        if not markets_page:
            break

        records = []

        for raw in markets_page:
            try:
                record = parse_market(raw)
                if record["id"]:  # ensure valid
                    records.append(record)
                else:
                    total_failed += 1
            except Exception as e:
                logger.warning(f"Parse failed for {raw.get('id')}: {e}")
                total_failed += 1

        # Batch insert
        upsert_markets_batch(records)

        total_ingested += len(records)
        offset += len(markets_page)

        logger.info(f"Ingested: {total_ingested} | Failed: {total_failed}")

        # Respect rate limits
        time.sleep(0.5)

        # Safety cap
        if total_ingested >= max_markets:
            break

    elapsed = time.time() - start_time
    logger.info(f"Ingestion complete: {total_ingested} markets")
    logger.info(f"Failures: {total_failed}")
    logger.info(f"Time taken: {elapsed:.2f}s")

    return total_ingested


# ============================================================
# Entry
# ============================================================
if __name__ == "__main__":
    ingest_all_markets()