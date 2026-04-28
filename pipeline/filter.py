import sys
import os

# Add project root to path so utils/ can be found
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import time
import json
from groq import Groq
from pydantic import BaseModel
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

from utils.supabase_client import get_service_client
supabase = get_service_client()

client = Groq(api_key=os.environ["GROQ_API_KEY"])

# ── BIT Capital holdings (used in LLM prompt) ─────────────────
BIT_CAPITAL_HOLDINGS = [
    {"ticker": "IREN",  "name": "IREN Limited",       "thesis": "Bitcoin mining and AI data centers infrastructure."},
    {"ticker": "MSFT",  "name": "Microsoft",           "thesis": "Core AI infrastructure and enterprise software play."},
    {"ticker": "GOOGL", "name": "Alphabet Inc.",       "thesis": "Search dominance and Gemini AI ecosystem."},
    {"ticker": "LMND",  "name": "Lemonade Inc.",       "thesis": "AI-driven insurtech disruptor."},
    {"ticker": "RDDT",  "name": "Reddit Inc.",         "thesis": "Data source for LLM training and ad growth."},
    {"ticker": "MU",    "name": "Micron Technology",   "thesis": "AI memory hardware supplier."},
    {"ticker": "TSM",   "name": "TSMC",                "thesis": "Foundry for high-end AI semiconductors."},
    {"ticker": "HUT",   "name": "Hut 8 Corp.",         "thesis": "Diversified crypto infrastructure."},
    {"ticker": "HOOD",  "name": "Robinhood Markets",   "thesis": "Gateway for retail crypto and equity trading."},
    {"ticker": "DDOG",  "name": "Datadog Inc.",        "thesis": "Cloud observability and security monitoring."},
]

VALID_TICKERS = {h["ticker"] for h in BIT_CAPITAL_HOLDINGS}


# ── Pydantic model ────────────────────────────────────────────
# Updated: impacted_tickers is now a LIST so one market can
# affect multiple stocks (e.g. tariff hits both TSM and MU)

class EquitySignal(BaseModel):
    is_relevant:      bool
    impacted_tickers: list[str]   # e.g. ["TSM", "MU"] or [] if not relevant
    sentiment:        str         # 'Bullish', 'Bearish', or 'Neutral'
    impact_score:     int         # 1-10
    reasoning:        str         # 1-2 sentences


# ── Fetch unscored markets from DB ────────────────────────────

def fetch_unscored_markets(min_volume: float = 5000, limit: int = 50) -> list[dict]:
    res = (
        supabase.table("markets")
        .select("id, question, volume, yes_price, end_date, event_id, events(title, category)")
        .eq("active", True)
        .eq("closed", False)
        .eq("llm_processed", False)      # ← only unprocessed markets
        .gte("volume", min_volume)
        .order("volume", desc=True)
        .limit(limit)
        .execute()
    )
    markets = res.data or []
    print(f"  To score now: {len(markets)}")
    return markets


# ── LLM analysis ──────────────────────────────────────────────

def build_prompt(market: dict) -> str:
    event      = market.get("events") or {}
    category   = event.get("category", "Unknown")
    event_title = event.get("title", "Unknown")

    return f"""You are a Senior Equity Analyst at BIT Capital.
Analyze if this Polymarket prediction market provides a material signal for any of our holdings.

Event:     {event_title}
Question:  {market['question']}
Category:  {category}
YES Price: {market['yes_price']} (probability the YES outcome occurs)

Our portfolio:
{json.dumps(BIT_CAPITAL_HOLDINGS, indent=2)}

Return a JSON object with exactly these fields:
- is_relevant (boolean): true only if this market materially impacts one or more of our holdings
- impacted_tickers (array of strings): list of tickers affected, e.g. ["TSM", "MU"] — empty array [] if not relevant
- sentiment (string): exactly one of 'Bullish', 'Bearish', or 'Neutral' for the affected tickers
- impact_score (integer 1-10): how material is the impact — 10 = immediate direct price impact
- reasoning (string): 1-2 sentences explaining which stocks are affected and why

Only include tickers from our portfolio. Do not invent tickers.
Return JSON only — no explanation outside the JSON."""


def analyze_market(market: dict, retries: int = 3, delay: int = 5) -> EquitySignal | None:
    """Send one market to Groq and parse the response."""
    prompt = build_prompt(market)

    for attempt in range(retries):
        try:
            response = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": "You are a financial analyst that outputs JSON only."},
                    {"role": "user",   "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.1,   # low = more consistent outputs
            )
            raw = response.choices[0].message.content
            return EquitySignal.model_validate_json(raw)

        except Exception as e:
            if "429" in str(e):
                wait = delay * (2 ** attempt)
                print(f"    Rate limit — waiting {wait}s (attempt {attempt+1}/{retries})")
                time.sleep(wait)
            else:
                print(f"    Error: {e}")
                break

    return None


# ── Save signals to DB ────────────────────────────────────────

def save_signals(market_id: str, signal: EquitySignal) -> int:
    stored = 0

    if signal.is_relevant and signal.impacted_tickers:
        for ticker in signal.impacted_tickers:
            if ticker not in VALID_TICKERS:
                print(f"    Skipping unknown ticker: {ticker}")
                continue
            try:
                supabase.table("signals").insert({
                    "market_id":    market_id,
                    "ticker":       ticker,
                    "is_relevant":  True,
                    "sentiment":    signal.sentiment,
                    "impact_score": signal.impact_score,
                    "reasoning":    signal.reasoning,
                    "model_used":   "llama-3.3-70b-versatile",
                }).execute()
                stored += 1
            except Exception as e:
                print(f"    Failed to save signal ({market_id}, {ticker}): {e}")

    # Always mark market as processed — never re-score it
    try:
        supabase.table("markets").update(
            {"llm_processed": True}
        ).eq("id", market_id).execute()
    except Exception as e:
        print(f"    Failed to mark {market_id} as processed: {e}")

    return stored


# ── Main pipeline ─────────────────────────────────────────────

def run_filter_pipeline(min_volume: float = 5000, batch_size: int = 50):
    started_at = datetime.now(timezone.utc)
    print(f"\n{'='*55}")
    print(f"  FILTER PIPELINE START — {started_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*55}\n")

    # Step 1: fetch unscored markets
    markets = fetch_unscored_markets(min_volume=min_volume, limit=batch_size)

    if not markets:
        print("No unscored markets found. Run the scraper first.")
        return

    # Step 2: score each market
    total_signals = 0
    irrelevant    = 0
    failed        = 0

    for i, market in enumerate(markets, 1):
        event    = market.get("events") or {}
        category = event.get("category", "?")
        q        = market["question"]
        vol      = market["volume"]

        print(f"[{i:>3}/{len(markets)}] ${vol:>10,.0f} | {category:<15} | {q[:60]}")

        signal = analyze_market(market)

        if signal is None:
            failed += 1
            print("    FAILED")
            continue

        saved = save_signals(market["id"], signal)

        if signal.is_relevant and saved > 0:
            total_signals += saved
            tickers_str = ", ".join(signal.impacted_tickers)
            print(f"    SIGNAL [{signal.sentiment.upper()}] score={signal.impact_score} tickers={tickers_str}")
            print(f"    {signal.reasoning[:120]}")
        else:
            irrelevant += 1
            print(f"    irrelevant (score={signal.impact_score})")

        time.sleep(1)   # prevent Groq burst limit

    # Step 3: summary
    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    print(f"\n{'='*55}")
    print(f"  PIPELINE COMPLETE — {elapsed:.1f}s")
    print(f"  Markets scored:  {len(markets)}")
    print(f"  Signals stored:  {total_signals}")
    print(f"  Irrelevant:      {irrelevant}")
    print(f"  Failed:          {failed}")
    print(f"{'='*55}\n")


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    run_filter_pipeline(min_volume=5000, batch_size=50)