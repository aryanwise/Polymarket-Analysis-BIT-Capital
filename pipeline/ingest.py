import sys
import os

# Add project root to path so utils/ can be found
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import requests
import json
import time
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

from utils.supabase_client import get_service_client
supabase = get_service_client()


# ── Config ────────────────────────────────────────────────────
TAG_CONFIG = {
    "Macro/Fed":      [129, 100478, 100488, 100196, 103360, 132, 702, 370, 102000, 101248, 101250, 102973],
    "Tariffs/Trade":  [101758, 311, 102012, 101759, 101760, 776],
    "Tech/AI":        [439, 835, 537, 22, 102038, 238, 483, 101999],
    "Crypto":         [21, 235, 833, 101798, 744, 1312],
    "Stocks":         [602, 604, 737, 100266, 102679, 102678, 102681, 102680, 103211, 103450, 103244],
    "Semiconductors": [100616, 102472, 103452]
}

GAMMA_API_URL = "https://gamma-api.polymarket.com/events"
MIN_VOLUME    = 1000


# ── Fetch from Polymarket ─────────────────────────────────────

def fetch_events_by_tag(tag_id: int) -> list[dict]:
    """Fetch active events for a single tag id."""
    try:
        resp = requests.get(GAMMA_API_URL, params={
            "tag_id":    tag_id,
            "active":    "true",
            "closed":    "false",
            "order":     "volume",
            "ascending": "false",
            "limit":     100,
        }, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  [ERROR] tag_id={tag_id}: {e}")
        return []


# ── Parse helpers ─────────────────────────────────────────────

def parse_event_row(event: dict, category: str, tag_id: int) -> dict:
    """Map a Polymarket event dict to our events table columns."""
    return {
        "id":       str(event["id"]),
        "title":    event.get("title", "Unknown"),
        "category": category,
        "tag_ids":  [tag_id],           # stored as array — grows on re-scrape
        "active":   event.get("active", True),
        "closed":   event.get("closed", False),
        "end_date": event.get("endDate") or None,
    }


def parse_market_row(market: dict, event_id: str) -> dict | None:
    """Map a Polymarket market dict to our markets table columns."""
    m_id   = market.get("id")
    volume = float(market.get("volume") or 0)

    if not m_id or volume < MIN_VOLUME:
        return None

    # outcomePrices comes as a JSON string: '["0.72","0.28"]'
    raw_prices = market.get("outcomePrices", "[]")
    try:
        prices = json.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
    except Exception:
        prices = []

    yes_price = float(prices[0]) if len(prices) > 0 else 0.0
    no_price  = float(prices[1]) if len(prices) > 1 else 0.0

    return {
        "id":        str(m_id),
        "event_id":  event_id,
        "question":  market.get("question", ""),
        "yes_price": round(yes_price, 4),
        "no_price":  round(no_price,  4),
        "volume":    volume,
        "liquidity": float(market.get("liquidity") or 0),
        "end_date":  market.get("endDate") or None,
        "active":    market.get("active", True),
        "closed":    market.get("closed", False),
    }


# ── Supabase upserts ──────────────────────────────────────────

def upsert_event(event_row: dict):
    """
    Upsert event into DB.
    If it already exists, update title/category/active/closed.
    tag_ids array is merged so we don't lose previously seen tag IDs.
    """
    try:
        supabase.table("events").upsert(
            event_row,
            on_conflict="id"
        ).execute()
    except Exception as e:
        print(f"  [ERROR] upsert event {event_row['id']}: {e}")


def upsert_market(market_row: dict):
    """
    Upsert market into DB.
    If it already exists, update price/volume/status.
    """
    try:
        supabase.table("markets").upsert(
            market_row,
            on_conflict="id"
        ).execute()
    except Exception as e:
        print(f"  [ERROR] upsert market {market_row['id']}: {e}")


# ── Main scraper ──────────────────────────────────────────────

def run_scraper():
    started_at = datetime.now(timezone.utc)
    print(f"\n{'='*55}")
    print(f"  SCRAPE START — {started_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*55}")

    seen_event_ids  : set[str] = set()
    seen_market_ids : set[str] = set()

    events_saved  = 0
    markets_saved = 0

    for category, tag_ids in TAG_CONFIG.items():
        print(f"\n[{category}]")

        for tag_id in tag_ids:
            events = fetch_events_by_tag(tag_id)

            for event in events:
                event_id = str(event.get("id", ""))
                if not event_id:
                    continue

                # ── Save event ──────────────────────────────
                event_row = parse_event_row(event, category, tag_id)

                if event_id not in seen_event_ids:
                    upsert_event(event_row)
                    seen_event_ids.add(event_id)
                    events_saved += 1

                # ── Save each market inside the event ───────
                for market in event.get("markets", []):
                    market_row = parse_market_row(market, event_id)

                    if market_row is None:
                        continue  # filtered by volume or missing id

                    m_id = market_row["id"]
                    if m_id not in seen_market_ids:
                        upsert_market(market_row)
                        seen_market_ids.add(m_id)
                        markets_saved += 1

            time.sleep(0.2)   # be polite to the API

    # ── Summary ───────────────────────────────────────────────
    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    print(f"\n{'='*55}")
    print(f"  SCRAPE COMPLETE — {elapsed:.1f}s")
    print(f"  Events  saved : {events_saved}")
    print(f"  Markets saved : {markets_saved}")
    print(f"{'='*55}\n")

    return events_saved, markets_saved


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    run_scraper()