"""
pipeline/ingest.py

Extracts Polymarket events + markets from the Gamma API.
Returns a DataFrame — does NOT write to Supabase (ETL: filter first, then load).

Called by:
  - scheduler.py  (automated pipeline)
  - run_pipeline.py (manual / debug run)
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

BASE_URL   = "https://gamma-api.polymarket.com/events"
PAGE_SIZE  = 500
MAX_EVENTS = 3000


def run_ingest(max_events: int = MAX_EVENTS) -> pd.DataFrame:
    """
    Extracts all active Polymarket markets.
    Returns raw DataFrame — no filtering, no DB writes.
    """
    rows   = []
    offset = 0

    logger.info("INGEST START — max_events=%d", max_events)

    while offset < max_events:
        try:
            resp = requests.get(BASE_URL, params={
                "active":    "true",
                "closed":    "false",
                "order":     "volume_24hr",
                "ascending": "false",
                "limit":     PAGE_SIZE,
                "offset":    offset,
            }, timeout=30)
            resp.raise_for_status()
            events = resp.json()

            if not isinstance(events, list) or not events:
                break

            for event in events:
                if not isinstance(event, dict):
                    continue

                event_id    = str(event.get("id", ""))
                event_title = event.get("title", "")
                tags_str    = ", ".join(
                    t.get("label", "") for t in event.get("tags", [])
                    if isinstance(t, dict) and t.get("label")
                )

                markets = event.get("markets")
                if not isinstance(markets, list):
                    continue

                for market in markets:
                    raw_prices   = market.get("outcomePrices", "[]")
                    raw_outcomes = market.get("outcomes", "[]")

                    try:
                        prices   = json.loads(raw_prices)   if isinstance(raw_prices,   str) else raw_prices
                        outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
                    except Exception:
                        prices, outcomes = [], []

                    prices   = prices   if isinstance(prices,   list) else []
                    outcomes = outcomes if isinstance(outcomes, list) else []

                    yes_price = round(float(prices[0]), 4) if len(prices) > 0 else None
                    no_price  = round(float(prices[1]), 4) if len(prices) > 1 else None

                    rows.append({
                        "market_id":     str(market.get("id", "")),
                        "event_id":      event_id,
                        "event_title":   event_title,
                        "question":      market.get("question", event_title),
                        "tags":          tags_str,
                        "outcomes":      outcomes,
                        "yes_price":     yes_price,
                        "no_price":      no_price,
                        "volume":        float(market.get("volumeNum") or market.get("volume") or 0),
                        "end_date":      market.get("endDate"),
                        "extracted_at":  datetime.now(timezone.utc).isoformat(),
                    })

            logger.info("offset=%d | markets so far: %d", offset, len(rows))
            offset += PAGE_SIZE
            time.sleep(0.5)

        except Exception as e:
            logger.error("Error at offset %d: %s", offset, e)
            break

    df = pd.DataFrame(rows)
    logger.info("INGEST COMPLETE — %d markets extracted", len(df))
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    df = run_ingest()
    print(df.shape)
    print(df.head())