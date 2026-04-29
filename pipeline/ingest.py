"""
pipeline/ingest.py

Production Polymarket ingestion pipeline.

What this does:
- Scrapes ALL active, unresolved markets from Polymarket
- Saves to Supabase: events table + markets table
- Only removes: expired markets + fully resolved (0%/100%) markets
- NO relevance filtering here — that is filter.py's job

Run:   uv run pipeline/ingest.py
Cron:  every 30 min via scheduler.py
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import time
import requests
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

from utils.supabase_client import get_service_client
supabase = get_service_client()

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── API ───────────────────────────────────────────────────────
GAMMA_EVENTS = "https://gamma-api.polymarket.com/events"
GAMMA_MKTS   = "https://gamma-api.polymarket.com/markets"
MIN_VOLUME   = 500       # very low — filter.py decides relevance

# ── Tag config ────────────────────────────────────────────────
TAG_CONFIG = {
    "Macro/Fed": [
        129, 100478, 100488, 100196, 103360, 132,
        702, 370, 102000, 101248, 101250, 102973,
        100486, 159, 103176,
    ],
    "Tariffs/Trade": [
        101758, 311, 102012, 101759, 101760, 776, 102358,
    ],
    "Tech/AI": [
        439, 835, 537, 22, 102038, 238, 483, 101999,
        555, 441, 662,
    ],
    "Crypto": [
        21, 235, 833, 101798, 744, 1312,
        101935, 102115, 800,
    ],
    "Stocks": [
        602, 604, 737,
        100266, 102679, 102678, 102681, 102680,
        103211, 103450, 103244,
        103571, 103584, 103452,
    ],
    "Holdings": [
        663,  102823,        # GOOGL
        1014,                # RDDT
        101318, 103211,      # HOOD
        1330,  100266,       # NVDA
        101647,              # META
        1098,  102679,       # MSFT
        824,   102681,       # AMZN
        800,   103210,       # COIN
        103452,              # AMD
        103450,              # HNGE
        103571,              # MU
        103584,              # TSM
    ],
    "Geopolitics": [
        842, 1396, 80, 778, 303, 154, 867, 192,
    ],
    "Regulation": [
        238, 960, 458, 233, 101798,
    ],
}

# Holdings with no dedicated tag — scraped via keyword search
KEYWORD_HOLDINGS = {
    "IREN":  ["IREN", "iris energy", "iren stock"],
    "LMND":  ["lemonade", "LMND", "lemonade inc"],
    "HUT":   ["hut 8", "hut8", "HUT stock"],
    "DDOG":  ["datadog", "DDOG"],
    "TSM":   ["TSMC", "taiwan semiconductor"],
    "MU":    ["micron", "MU stock", "micron technology"],
    "HOOD":  ["robinhood", "HOOD stock"],
    "NVDA":  ["nvidia", "NVDA stock"],
    "MSFT":  ["microsoft", "MSFT stock"],
    "GOOGL": ["alphabet", "google stock", "GOOGL"],
    "RDDT":  ["reddit", "RDDT stock"],
}


# ── Filters (only these two — everything else goes to DB) ─────

def is_expired(expiry: str | None) -> bool:
    """True if market expiry is in the past."""
    if not expiry:
        return False
    try:
        exp_dt = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
        return exp_dt < datetime.now(timezone.utc)
    except Exception:
        return False


def is_resolved(yes: float, no: float) -> bool:
    """
    True if market is fully resolved (outcome certain).
    0%/100% markets carry no signal — skip them.
    """
    return (yes == 0.0 and no == 1.0) or (yes == 1.0 and no == 0.0)


# ── Parsers ───────────────────────────────────────────────────

def parse_prices(market: dict) -> tuple[float, float]:
    raw = market.get("outcomePrices", "[]")
    try:
        prices = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        prices = []
    yes = round(float(prices[0]), 4) if len(prices) > 0 else 0.0
    no  = round(float(prices[1]), 4) if len(prices) > 1 else 0.0
    return yes, no


def parse_event_row(event: dict, category: str, tag_id: int) -> dict:
    return {
        "id":       str(event["id"]),
        "title":    event.get("title", "Unknown"),
        "category": category,
        "tag_ids":  [tag_id],
        "active":   event.get("active", True),
        "closed":   event.get("closed", False),
        "end_date": event.get("endDate") or None,
    }


def parse_market_row(market: dict, event_id: str) -> dict | None:
    m_id   = market.get("id")
    volume = float(market.get("volume") or 0)
    expiry = market.get("endDate")

    if not m_id:              return None
    if volume < MIN_VOLUME:   return None
    if is_expired(expiry):    return None

    yes, no = parse_prices(market)
    if is_resolved(yes, no):  return None

    return {
        "id":            str(m_id),
        "event_id":      event_id,
        "question":      market.get("question", ""),
        "yes_price":     yes,
        "no_price":      no,
        "volume":        round(volume, 2),
        "liquidity":     round(float(market.get("liquidity") or 0), 2),
        "end_date":      expiry or None,
        "active":        market.get("active", True),
        "closed":        market.get("closed", False),
        "llm_processed": False,
    }


def make_synthetic_event(ticker: str) -> dict:
    """
    Keyword-matched markets don't have a parent event.
    We create one so they fit the events → markets schema.
    All keyword matches for one ticker group under the same event.
    """
    return {
        "id":       f"holding_{ticker.lower()}",
        "title":    f"{ticker} — Company Markets",
        "category": f"Holdings/{ticker}",
        "tag_ids":  [],
        "active":   True,
        "closed":   False,
        "end_date": None,
    }


# ── Fetch from Polymarket ─────────────────────────────────────

def fetch_events_by_tag(tag_id: int) -> list[dict]:
    """Fetch events with pagination — handles large tags like Crypto."""
    all_events: list[dict] = []
    offset = 0

    while True:
        try:
            resp = requests.get(GAMMA_EVENTS, params={
                "tag_id":    tag_id,
                "active":    "true",
                "closed":    "false",
                "order":     "volume",
                "ascending": "false",
                "limit":     100,
                "offset":    offset,
            }, timeout=30)
            resp.raise_for_status()
            page = resp.json()

            if not page:
                break

            all_events.extend(page)

            if len(page) < 100:
                break

            offset += 100
            time.sleep(0.1)

        except requests.exceptions.Timeout:
            logger.warning("Timeout at tag_id=%d offset=%d — skipping rest", tag_id, offset)
            break
        except Exception as e:
            logger.warning("Error tag_id=%d offset=%d: %s", tag_id, offset, e)
            break

    return all_events


def fetch_markets_by_keyword(keyword: str) -> list[dict]:
    """
    Use Gamma's native keyword param to search the full market DB.
    Much more effective than fetching all markets and filtering in Python.
    """
    try:
        resp = requests.get(GAMMA_MKTS, params={
            "keyword":   keyword,
            "active":    "true",
            "closed":    "false",
            "order":     "volume",
            "ascending": "false",
            "limit":     100,
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            data = data.get("data", [])
        return [
            m for m in data
            if float(m.get("volume", 0) or 0) >= MIN_VOLUME
        ]
    except requests.exceptions.Timeout:
        logger.warning("Timeout fetching keyword='%s'", keyword)
        return []
    except Exception as e:
        logger.warning("Error keyword='%s': %s", keyword, e)
        return []


# ── Supabase writes ───────────────────────────────────────────

def upsert_event(row: dict):
    try:
        supabase.table("events").upsert(row, on_conflict="id").execute()
    except Exception as e:
        logger.error("upsert event %s: %s", row.get("id"), e)


def upsert_market(row: dict):
    """
    Upsert market — updates yes_price, no_price, volume on re-scrape.
    Does NOT overwrite llm_processed if already set to True.
    """
    try:
        # Only reset llm_processed if this is a brand new market
        existing = (
            supabase.table("markets")
            .select("id, llm_processed")
            .eq("id", row["id"])
            .execute()
        )
        if existing.data:
            # Market exists — update prices/volume but preserve llm_processed
            row.pop("llm_processed", None)

        supabase.table("markets").upsert(row, on_conflict="id").execute()
    except Exception as e:
        logger.error("upsert market %s: %s", row.get("id"), e)


def batch_upsert_markets(rows: list[dict]):
    """Upsert markets in batches of 50 to stay within Supabase limits."""
    BATCH = 50
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        try:
            supabase.table("markets").upsert(batch, on_conflict="id").execute()
        except Exception as e:
            logger.error("Batch upsert failed at index %d: %s", i, e)
            # Fall back to individual upserts
            for row in batch:
                upsert_market(row)


# ── Main pipeline ─────────────────────────────────────────────

def run_ingestion() -> dict:
    """
    Full ingestion run.
    Returns summary dict with counts.
    """
    started_at      = datetime.now(timezone.utc)
    seen_event_ids  : set[str] = set()
    seen_market_ids : set[str] = set()
    events_saved    = 0
    markets_saved   = 0
    market_buffer   : list[dict] = []   # batched for efficiency

    logger.info("=" * 60)
    logger.info("INGESTION START — %s", started_at.strftime("%Y-%m-%d %H:%M:%S UTC"))
    logger.info("=" * 60)

    def flush_buffer():
        """Write buffered market rows to Supabase."""
        nonlocal markets_saved
        if market_buffer:
            batch_upsert_markets(list(market_buffer))
            markets_saved += len(market_buffer)
            market_buffer.clear()

    def handle_market(market: dict, event_id: str):
        """Parse and buffer one market row."""
        row = parse_market_row(market, event_id)
        if row and row["id"] not in seen_market_ids:
            market_buffer.append(row)
            seen_market_ids.add(row["id"])
            if len(market_buffer) >= 50:
                flush_buffer()

    # ── Phase 1: tag-based scrape ─────────────────────────────
    logger.info("\n[PHASE 1] Tag-based scrape")

    for category, tag_ids in TAG_CONFIG.items():
        cat_markets = 0

        for tag_id in tag_ids:
            events = fetch_events_by_tag(tag_id)

            for event in events:
                event_id = str(event.get("id", ""))
                if not event_id:
                    continue

                # Save event
                if event_id not in seen_event_ids:
                    upsert_event(parse_event_row(event, category, tag_id))
                    seen_event_ids.add(event_id)
                    events_saved += 1

                # Buffer markets
                for market in event.get("markets", []):
                    before = len(seen_market_ids)
                    handle_market(market, event_id)
                    if len(seen_market_ids) > before:
                        cat_markets += 1

            time.sleep(0.05)

        flush_buffer()
        logger.info("  %-20s %4d markets", category, cat_markets)

    # ── Phase 2: keyword search for holdings ─────────────────
    logger.info("\n[PHASE 2] Holdings keyword search")

    for ticker, keywords in KEYWORD_HOLDINGS.items():
        ticker_count = 0
        synthetic    = make_synthetic_event(ticker)
        synthetic_id = synthetic["id"]

        for keyword in keywords:
            markets = fetch_markets_by_keyword(keyword)

            for market in markets:
                # Ensure synthetic event exists
                if synthetic_id not in seen_event_ids:
                    upsert_event(synthetic)
                    seen_event_ids.add(synthetic_id)
                    events_saved += 1

                before = len(seen_market_ids)
                handle_market(market, synthetic_id)
                if len(seen_market_ids) > before:
                    ticker_count += 1

            time.sleep(0.2)

        flush_buffer()
        flag = "" if ticker_count > 0 else "  (no active markets)"
        logger.info("  %-8s %3d markets%s", ticker, ticker_count, flag)

    # ── Final flush ───────────────────────────────────────────
    flush_buffer()

    # ── Summary ───────────────────────────────────────────────
    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()

    summary = {
        "status":         "success",
        "run_at":         started_at.isoformat(),
        "duration_s":     round(elapsed, 1),
        "events_saved":   events_saved,
        "markets_saved":  markets_saved,
    }

    logger.info("\n%s", "=" * 60)
    logger.info("INGESTION COMPLETE — %.1fs", elapsed)
    logger.info("  Events  : %d", events_saved)
    logger.info("  Markets : %d", markets_saved)
    logger.info("%s\n", "=" * 60)

    return summary


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    result = run_ingestion()
    print(result)