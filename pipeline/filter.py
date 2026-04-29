"""
pipeline/filter.py

Two-Stage LLM-powered signal filter for BIT Capital.
Stage 1: Rule-based heuristic filtering (Free)
Stage 2: LLM-powered equity impact analysis (Groq)
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import time
import json
import logging
from groq import Groq
from pydantic import BaseModel
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

from utils.supabase_client import get_service_client
supabase = get_service_client()
client   = Groq(api_key=os.environ["GROQ_API_KEY"])

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BIT_CAPITAL_HOLDINGS = [
    {"ticker": "IREN",  "name": "IREN Limited",       "sector": "Crypto Mining",       "thesis": "Bitcoin mining and AI data centers infrastructure."},
    {"ticker": "MSFT",  "name": "Microsoft",          "sector": "Cloud/AI",            "thesis": "Core AI infrastructure and enterprise software play."},
    {"ticker": "GOOGL", "name": "Alphabet",           "sector": "Cloud/AI",            "thesis": "AI models, search monopoly, and cloud infrastructure."},
    {"ticker": "LMND",  "name": "Lemonade",           "sector": "Insurtech",           "thesis": "AI-driven insurance disruption."},
    {"ticker": "RDDT",  "name": "Reddit",             "sector": "Social Media",        "thesis": "Data licensing for AI and niche advertising."},
    {"ticker": "MU",    "name": "Micron",             "sector": "Semiconductors",      "thesis": "Memory chips essential for AI data centers."},
    {"ticker": "TSM",   "name": "TSMC",               "sector": "Semiconductors",      "thesis": "Leading foundry for global AI chip production."},
    {"ticker": "HUT",   "name": "Hut 8",              "sector": "Crypto Mining",       "thesis": "Diversified crypto infrastructure and AI compute."},
    {"ticker": "HOOD",  "name": "Robinhood",          "sector": "Fintech",             "thesis": "Retail trading and crypto expansion."},
    {"ticker": "DDOG",  "name": "Datadog",            "sector": "Software",            "thesis": "Cloud monitoring and security."},
    {"ticker": "AMZN",  "name": "Amazon",             "sector": "Cloud/E-commerce",    "thesis": "AWS dominance and AI integration."},
    {"ticker": "COIN",  "name": "Coinbase",           "sector": "Crypto Exchange",     "thesis": "Institutional and retail crypto adoption."},
    {"ticker": "META",  "name": "Meta Platforms",     "sector": "Social Media/AI",     "thesis": "Open-source AI leadership and digital advertising."},
    {"ticker": "NVDA",  "name": "NVIDIA",             "sector": "Semiconductors",      "thesis": "Undisputed leader in AI accelerators."}
]

# --- STAGE 1: CONSTANTS ---

RELEVANT_CATEGORIES = {
    "Macro/Fed", "Tariffs/Trade", "Tech/AI", "Crypto", "Stocks", 
    "Semiconductors", "Regulation", "Geopolitics", "Holdings"
}

SIGNAL_KEYWORDS = [
    "federal reserve", "fed rate", "interest rate", "rate cut", "rate hike",
    "fomc", "inflation", "cpi", "gdp", "recession", "monetary policy",
    "bitcoin", "btc", "ethereum", "eth", "crypto", "coinbase", "etf",
    "nvidia", "nvda", "tsmc", "taiwan semiconductor", "micron", "amd",
    "chips", "semiconductor", "export control", "ai chip", "gpu",
    "openai", "anthropic", "gemini", "gpt", "artificial intelligence",
    "antitrust", "big tech", "google", "microsoft", "meta", "amazon",
    "iren", "hut 8", "hut8", "iris energy", "robinhood", "hood",
    "tariff", "trade war", "sanctions", "acquisition", "merger", "ipo"
]

NOISE_KEYWORDS = [
    "nba", "nfl", "mlb", "nhl", "fifa", "premier league", "wimbledon", "tennis",
    "golf", "ufc", "boxing", "oscars", "grammys", "emmys", "box office", "movie",
    "taylor swift", "celebrity", "reality tv", "election winner", "poll",
    "puffpaw", "megaeth", "meme coin", "airdrop", "weather", "rainfall"
]

# --- DATA MODELS ---

class SignalResponse(BaseModel):
    is_relevant: bool
    impacted_tickers: list[str]
    sentiment: str  # Bullish, Bearish, Neutral
    impact_score: int # 1-10
    reasoning: str
    impact_type: str # Regulation, Macro, Competition, etc.
    time_horizon: str # Short-term, Medium-term, Long-term

# --- FILTERING LOGIC ---

def pre_filter(market: dict) -> tuple[bool, str]:
    """Stage 1 rule-based filter. Fast, no LLM cost."""
    question = market.get("question", "").lower()
    event_data = market.get("events") or {}
    category = event_data.get("category", "")
    volume = float(market.get("volume") or 0)
    yes_price = float(market.get("yes_price") or 0)

    # 1. Minimum volume floor
    if volume < 5000:
        return False, f"low volume ${volume:,.0f}"

    # 2. Resolution near-certainty (no alpha)
    if yes_price < 0.04 or yes_price > 0.96:
        return False, f"near-certain outcome YES={yes_price:.0%}"

    # 3. Hard noise rejection
    for kw in NOISE_KEYWORDS:
        if kw in question:
            return False, f"noise keyword: '{kw}'"

    # 4. Relevant category bypass
    if category in RELEVANT_CATEGORIES:
        return True, f"relevant category: {category}"

    # 5. Signal keyword match
    for kw in SIGNAL_KEYWORDS:
        if kw in question:
            return True, f"signal keyword: '{kw}'"

    return False, f"no signal keywords matched (category={category})"

def signal_quality(market: dict) -> float:
    """Calculates a priority score based on volume and uncertainty."""
    volume = float(market.get("volume") or 0)
    yes_price = float(market.get("yes_price") or 0.5)
    # Uncertainty is highest at 0.5 (1.0) and lowest at 0 or 1 (0.0)
    uncertainty = 1.0 - abs(0.5 - yes_price) * 2
    return volume * uncertainty

def fetch_unscored_markets(limit: int = 100) -> list[dict]:
    """Fetch unscored markets and apply Stage 1 pre-filter."""
    res = (
        supabase.table("markets")
        .select("id, question, volume, yes_price, end_date, event_id, events(title, category)")
        .eq("active", True)
        .eq("closed", False)
        .eq("llm_processed", False)
        .gte("volume", 1000)
        .order("volume", desc=True)
        .limit(500) 
        .execute()
    )

    all_markets = res.data or []
    passed = []
    rejected = 0

    for market in all_markets:
        should_process, reason = pre_filter(market)
        if should_process:
            passed.append(market)
        else:
            rejected += 1
            # Mark as processed to save LLM costs in future runs
            try:
                supabase.table("markets").update({"llm_processed": True}).eq("id", market["id"]).execute()
            except Exception:
                pass

    # Sort the passed batch by signal quality
    passed.sort(key=signal_quality, reverse=True)
    
    logger.info(
        "Stage 1: %d fetched -> %d passed -> %d rejected (Noise Filter)",
        len(all_markets), len(passed), rejected
    )
    return passed[:limit]

# --- STAGE 2: LLM SCORING ---

def score_market(market: dict) -> SignalResponse:
    """Stage 2: LLM analysis for surviving markets."""
    holdings_context = json.dumps(BIT_CAPITAL_HOLDINGS, indent=2)
    
    prompt = f"""
    Analyze the following prediction market for its impact on technology and public equities:
    Market: {market['question']}
    Category: {market.get('events', {}).get('category', 'Unknown')}
    Current Implied Probability: {float(market.get('yes_price', 0))*100:.1f}%

    Determine if this is relevant to the following BIT Capital holdings:
    {holdings_context}
    
    Evaluate the transmission mechanism of this event on these specific companies.
    Return a structured analysis matching the JSON format.
    """

    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.1
        )
        data = json.loads(completion.choices[0].message.content)
        return SignalResponse(**data)
    except Exception as e:
        logger.error(f"LLM Error for market {market['id']}: {e}")
        return None

def save_signals(market_id: str, signal: SignalResponse) -> int:
    """Saves relevant signals to the Supabase database."""
    saved_count = 0
    for ticker in signal.impacted_tickers:
        try:
            data = {
                "market_id": market_id,
                "impacted_ticker": ticker,
                "sentiment": signal.sentiment,
                "impact_score": signal.impact_score,
                "reasoning": signal.reasoning,
                "impact_type": signal.impact_type,
                "time_horizon": signal.time_horizon,
                "created_at": datetime.now(timezone.utc).isoformat()
            }
            supabase.table("signals").insert(data).execute()
            saved_count += 1
        except Exception as e:
            logger.error(f"Failed to save signal for {ticker} on market {market_id}: {e}")
    return saved_count

def run_filter_pipeline():
    """Executes the two-stage pipeline."""
    started_at = datetime.now(timezone.utc)
    logger.info("Starting Two-Stage Signal Pipeline...")

    # Stage 1: Get filtered markets
    markets = fetch_unscored_markets(limit=50)
    
    total_signals = 0
    irrelevant = 0
    failed = 0
    
    for market in markets:
        logger.info(f"Stage 2 Analyzing: {market['question'][:70]}...")
        
        # Mark as processed immediately so it's not retried if it fails
        try:
            supabase.table("markets").update({"llm_processed": True}).eq("id", market["id"]).execute()
        except Exception:
            pass

        # Stage 2: LLM Reasoning
        signal = score_market(market)
        
        if not signal:
            failed += 1
            time.sleep(1)
            continue
        
        saved = save_signals(market["id"], signal)

        if signal.is_relevant and saved > 0:
            total_signals += saved
            tickers_str = ", ".join(signal.impacted_tickers)
            logger.info(
                "    SIGNAL [%s] score=%d type=%-12s horizon=%-12s tickers=%s",
                signal.sentiment.upper(), signal.impact_score,
                signal.impact_type, signal.time_horizon, tickers_str,
            )
            logger.info("    %s", signal.reasoning[:140])
        else:
            irrelevant += 1
            logger.info(
                "    irrelevant (score=%d, type=%s)",
                signal.impact_score, signal.impact_type,
            )
        
        time.sleep(1)

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE - %.1fs", elapsed)
    logger.info("  Markets scored : %d", len(markets))
    logger.info("  Signals stored : %d", total_signals)
    logger.info("  Irrelevant     : %d", irrelevant)
    logger.info("  Failed         : %d", failed)
    logger.info("=" * 60)

    return {
        "markets_scored": len(markets),
        "signals_stored": total_signals,
        "irrelevant":     irrelevant,
        "failed":         failed
    }

if __name__ == "__main__":
    run_filter_pipeline()