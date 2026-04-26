"""
pipeline/filter.py

LLM-powered market filter — Production-grade multi-layer architecture.

Layer 1:   Coarse filter (active=true, closed=false) — applied at ingestion
Layer 2a:  Soft category scoring (prefer high-signal categories)
Layer 2b:  Word-boundary keyword scoring (theme-capped, weighted)
Layer 2c:  Pre-LLM ranking (kw_score + volume) — only top-N go to LLM
Layer 3:   LLM fine-filter — maps remaining markets to specific tickers
Final:     Hybrid relevance decision + multi-factor signal ranking
           (LLM score, volume, kw_score, probability strength, time-decay, category)

Stores signals with full metadata for auditability and analysis.
"""
import os
import re
import json
import time
import math
import logging
from google import genai
from supabase import create_client, Client
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# Logging
# ============================================================
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

# ============================================================
# Configuration
# ============================================================
SUPABASE_URL         = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client     = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

genai_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
MODEL_NAME = "gemini-2.5-flash"

# ============================================================
# Layer 2a: Soft Category Scoring
# ============================================================
CATEGORY_SCORES: dict[str, int] = {
    "politics":     3,
    "economics":    3,
    "technology":   3,
    "crypto":       3,
    "science":      2,
    "business":     2,
    "world":        2,
    "governance":   2,
    "health":       2,
    "energy":       1,
    "regulation":   1,
}


# ============================================================
# Layer 2b: Keyword Scoring (weighted, theme-capped)
# ============================================================

# Keyword → (weight, theme)
EQUITY_KEYWORDS: dict[str, tuple[int, str]] = {
    # Macro / Rates — theme: "rates"
    "federal reserve":    (3, "rates"),
    "fed rate":           (3, "rates"),
    "fomc":               (3, "rates"),
    "rate cut":           (3, "rates"),
    "rate hike":          (3, "rates"),
    "interest rate":      (3, "rates"),
    "basis point":        (2, "rates"),
    "jerome powell":      (2, "rates"),
    "monetary policy":    (3, "rates"),
    "inflation":          (3, "rates"),
    "cpi":                (3, "rates"),
    "recession":          (3, "rates"),
    "gdp":                (2, "rates"),
    "yield curve":        (2, "rates"),
    "debt ceiling":       (2, "rates"),
    "government shutdown":(2, "rates"),

    # Trade / Tariffs — theme: "trade"
    "tariff":             (4, "trade"),
    "trade war":          (4, "trade"),
    "trade deal":         (3, "trade"),
    "export control":     (4, "trade"),
    "section 301":        (3, "trade"),
    "section 232":        (3, "trade"),
    "sanctions":          (3, "trade"),
    "trade restriction":  (4, "trade"),
    "import ban":         (4, "trade"),
    "export ban":         (4, "trade"),

    # Tech / AI Regulation — theme: "tech_reg"
    "antitrust":          (3, "tech_reg"),
    "section 230":        (2, "tech_reg"),
    "digital markets act":(3, "tech_reg"),
    "dma":                (3, "tech_reg"),
    "big tech":           (3, "tech_reg"),
    "app store":          (2, "tech_reg"),
    "data privacy":       (2, "tech_reg"),
    "gdpr":               (2, "tech_reg"),
    "ai regulation":      (3, "tech_reg"),
    "ai act":             (3, "tech_reg"),
    "ai safety":          (2, "tech_reg"),
    "ai executive order": (2, "tech_reg"),
    "ai ban":             (3, "tech_reg"),
    "ai chip":            (3, "tech_reg"),

    # Semiconductors — theme: "semis"
    "semiconductor":      (4, "semis"),
    "chip":               (3, "semis"),
    "nvidia":             (4, "semis"),
    "tsmc":               (4, "semis"),
    "intel":              (2, "semis"),
    "amd":                (2, "semis"),
    "chips act":          (4, "semis"),
    "huawei":             (3, "semis"),
    "micron":             (3, "semis"),
    "hbm":                (4, "semis"),
    "gpu":                (3, "semis"),
    "foundry":            (3, "semis"),
    "wafer":              (3, "semis"),
    "advanced packaging": (4, "semis"),
    "photonics":          (4, "semis"),
    "coherent":           (4, "semis"),
    "laser":              (3, "semis"),

    # Crypto — theme: "crypto"
    "bitcoin":            (2, "crypto"),
    "btc":                (2, "crypto"),
    "ethereum":           (2, "crypto"),
    "crypto":             (2, "crypto"),
    "stablecoin":         (2, "crypto"),
    "usdc":               (2, "crypto"),
    "cbdc":               (2, "crypto"),
    "genius act":         (3, "crypto"),
    "fit21":              (3, "crypto"),
    "digital asset":      (3, "crypto"),
    "sec crypto":         (3, "crypto"),
    "bitcoin etf":        (3, "crypto"),
    "crypto etf":         (3, "crypto"),
    "coinbase":           (3, "crypto"),
    "bitcoin miner":      (3, "crypto"),
    "hut 8":              (4, "crypto"),
    "iren":               (4, "crypto"),
    "iris energy":        (4, "crypto"),

    # BIT Capital Holdings — theme: "holdings"
    "google":             (4, "holdings"),
    "alphabet":           (4, "holdings"),
    "microsoft":          (4, "holdings"),
    "meta":               (4, "holdings"),
    "reddit":             (3, "holdings"),
    "lemonade":           (4, "holdings"),
    "hinge health":       (4, "holdings"),
    "datadog":            (4, "holdings"),
    "robinhood":          (4, "holdings"),
    "taiwan semiconductor":(4, "holdings"),
    "amazon":             (4, "holdings"),
    "aws":                (3, "holdings"),

    # Corporate Events — theme: "corporate"
    "ipo":                (2, "corporate"),
    "merger":             (3, "corporate"),
    "acquisition":        (3, "corporate"),
    "fda approv":         (3, "corporate"),
    "fda reject":         (3, "corporate"),
    "sec investigation":  (3, "corporate"),
    "class action":       (2, "corporate"),
    "ceo resign":         (2, "corporate"),
    "data breach":        (2, "corporate"),
    "bankruptcy":         (3, "corporate"),
    "earnings":           (2, "corporate"),
    "revenue":            (2, "corporate"),
    "guidance":           (2, "corporate"),
}

# Compile regex patterns once at module load (word-boundary)
_KEYWORD_PATTERNS: dict[str, re.Pattern] = {
    kw: re.compile(rf"\b{re.escape(kw)}s?\b", re.IGNORECASE)
    for kw in EQUITY_KEYWORDS
}


# ============================================================
# Layer 2b: Keyword Scoring Functions
# ============================================================

def _flatten_tags(tags) -> str:
    """Robustly flatten tags (strings, dicts, lists) into a single string."""
    if isinstance(tags, str):
        try:
            tags = json.loads(tags)
        except Exception:
            return tags
    if not isinstance(tags, list):
        return str(tags)

    parts = []
    for t in tags:
        if isinstance(t, str):
            parts.append(t)
        elif isinstance(t, dict):
            parts.extend(str(v) for v in t.values())
        else:
            parts.append(str(t))
    return " ".join(parts)


def keyword_score(market: dict) -> tuple[int, list[str], dict[str, int]]:
    """
    Score a market by matching keywords with word-boundary regex.

    Uses theme-capping: within each theme, only the highest-weighted
    keyword counts, preventing "chip" + "semiconductor" double-counting.

    Returns:
        (total_score, matched_keywords, theme_breakdown)
    """
    question = (market.get("question") or "").lower()
    tag_text = _flatten_tags(market.get("tags", [])).lower()
    text = f"{question} {tag_text}"

    total = 0
    matched = []
    theme_caps: dict[str, int] = {}

    for kw, pattern in _KEYWORD_PATTERNS.items():
        if pattern.search(text):
            weight, theme = EQUITY_KEYWORDS[kw]
            matched.append(kw)
            # Theme cap: keep the highest weight per theme
            theme_caps[theme] = max(theme_caps.get(theme, 0), weight)

    total = sum(theme_caps.values())
    return total, matched, theme_caps


# ============================================================
# Signal Strength Helpers
# ============================================================

def probability_strength(outcomes) -> float:
    """
    How "decisive" is this market? 50/50 = 0, 90/10 = 0.8.
    Strong convictions matter more for trading signals.
    """
    if not outcomes:
        return 0.0
    try:
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        probs = [float(o.get("price", 0)) for o in outcomes]
        if not probs:
            return 0.0
        return max(abs(p - 0.5) for p in probs) * 2  # maps 0.5→0.0, 1.0→1.0
    except Exception:
        return 0.0

def probability_change(market: dict) -> float:
    """
    Measures change in probability from previous fetch.
    Requires prev_outcome_price column in markets table.
    """
    prev = market.get("prev_outcome_price")
    if prev is None:
        return 0.0
    try:
        outcomes = market.get("outcomes", [])
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        curr = float(outcomes[0].get("price", 0)) if outcomes else 0.0
        return min(abs(curr - prev), 0.5) * 2  # 50pp change → 1.0 score
    except Exception:
        return 0.0

def time_decay(end_date: str | None) -> float:
    """
    Exponential decay: 1.0 for today, ~0.37 after 30 days, ~0.05 after 90 days.
    Near-term catalysts matter more than distant events.
    """
    if not end_date:
        return 0.5
    try:
        expiry = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        days = max((expiry - now).days, 0)
        return math.exp(-days / 30)
    except Exception:
        return 0.5


def category_score(category: str | None) -> int:
    """Return priority score for a category. Unknown = 0 (not excluded)."""
    if not category:
        return 0
    return CATEGORY_SCORES.get(category.lower(), 0)


def compute_final_score(
    market: dict,
    llm_relevance: float,
    kw_score: int,
) -> float:
    """
    Multi-factor signal ranking.

    Weights:
      - LLM relevance:     38%
      - Volume:            25%  (capped at $500K)
      - Keyword score:     15%  (capped at 15)
      - Probability str:   10%  (decisive markets > coin-flips)
      - Time decay:         7%  (near-term > distant)
      - Category score:     5%  (politics/econ > entertainment)
    """
    volume = float(market.get("volume_total", 0) or 0)

    volume_norm = min(volume / 500_000, 1.0)
    kw_norm     = min(kw_score / 15.0, 1.0)
    prob_norm   = probability_strength(market.get("outcomes", []))
    time_score  = time_decay(market.get("end_date"))
    cat_score   = category_score(market.get("category")) / 3.0
    change_score = probability_change(market)

    return (
    llm_relevance * 0.35 +     
    volume_norm    * 0.23 +     
    kw_norm        * 0.14 +     
    prob_norm      * 0.09 +     
    time_score     * 0.07 +
    cat_score      * 0.05 +     
    change_score   * 0.07       
)
# Sum: 0.35 + 0.23 + 0.14 + 0.09 + 0.07 + 0.05 + 0.07 = 1.00


# ============================================================
# Stock Context
# ============================================================

def load_stock_context() -> tuple[str, set[str]]:
    """Build LLM context string + valid ticker set from DB."""
    stocks = supabase.table("stocks").select("*").eq("active", True).execute()
    lines = []
    valid_tickers = set()
    for s in stocks.data:
        lines.append(f"- {s['ticker']} ({s['company_name']}): {s['sector']}")
        valid_tickers.add(s["ticker"])
    return "\n".join(lines), valid_tickers


# ============================================================
# LLM Prompt
# ============================================================

def build_filter_prompt(market: dict, stock_context: str) -> str:
    """Construct classification prompt with few-shot example."""
    outcomes = market.get("outcomes", [])
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except Exception:
            outcomes = []
    outcomes_str = ", ".join(
        f"{o['name']}: {float(o['price']):.0%}" for o in outcomes
    )

    return f"""You are an equity research analyst at BIT Capital, a tech-focused investment fund.

Our portfolio consists of these stocks and sectors:
{stock_context}

We monitor themes including: Fed monetary policy, tariffs on semiconductors/tech, AI regulation, antitrust actions against Big Tech, crypto regulation, cloud spending trends, company-specific product launches, and macro events that move growth equities.

Below is a prediction market from Polymarket. Determine if this market could materially impact any of our stocks in the short to medium term.

Market question: {market['question']}
Category: {market.get('category', 'Unknown')}
Tags: {market.get('tags', '[]')}
Outcomes & current probabilities: {outcomes_str}
Total volume: ${market.get('volume_total', 0):,.0f}
Expiry: {market.get('end_date', 'N/A')}

--- EXAMPLE OF A STRONG SIGNAL ---

Market: "Will the US impose tariffs on TSMC chips above 25% in 2026?"
Probability: Yes 68% / No 32%
Relevant: true
Tickers: ["NVDA", "COHR"]
Direction: bearish
Relevance: 0.92
Reasoning: "Higher tariffs on TSMC would raise costs for NVIDIA's H200/B200 GPUs (manufactured by TSMC), potentially compressing gross margins by 200-400bps. Coherent (optical transceivers) also relies heavily on TSMC's advanced packaging. This market currently implies a 68% probability of a significant margin headwind."

--- END OF EXAMPLE ---

If relevant, return EXACTLY this JSON (no other text):
{{"relevant": true, "tickers": ["TICKER1", "TICKER2"], "relevance_score": 0.0, "direction": "bullish", "reasoning": "Specific explanation of which stock(s) are affected, the transmission mechanism, and likely price direction."}}

If NOT relevant to any of our holdings, return:
{{"relevant": false, "tickers": [], "relevance_score": 0.0, "direction": "neutral", "reasoning": "No material equity impact for our portfolio."}}

Important rules:
- "direction" must be exactly one of: bullish, bearish, neutral, mixed
- "relevance_score": 0.0-1.0, where 1.0 = direct, immediate, and material price impact expected
- Only include tickers from our portfolio list above — do not invent tickers
- Be specific about WHY the stock moves and through what mechanism (costs, revenue, regulation, sentiment)
- Only return JSON, no markdown fences, no other text"""


# ============================================================
# LLM Classifier
# ============================================================

def classify_market(market: dict, stock_context: str, retries: int = 3) -> dict:
    """Send market to Gemini, retry on parse failures with backoff."""
    prompt = build_filter_prompt(market, stock_context)

    for attempt in range(retries):
        try:
            response = genai_client.models.generate_content(
                model=MODEL_NAME,
                contents=prompt,
            )
            text = response.text.strip()

            # Strip markdown fences: ```json ... ```
            if "```" in text:
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            text = text.strip()

            result = json.loads(text)
            required = ["relevant", "tickers", "relevance_score", "direction", "reasoning"]

            if all(k in result for k in required):
                if result["direction"] not in ("bullish", "bearish", "neutral", "mixed"):
                    result["direction"] = "neutral"
                return result

            logger.warning(f"Missing fields: {result}")

        except json.JSONDecodeError as e:
            logger.warning(f"JSON parse error (attempt {attempt + 1}): {e}")
        except Exception as e:
            logger.warning(f"API error (attempt {attempt + 1}): {type(e).__name__}: {e}")

        if attempt < retries - 1:
            time.sleep(2)

    return {
        "relevant":        False,
        "tickers":         [],
        "relevance_score": 0.0,
        "direction":       "neutral",
        "reasoning":       "Classification failed after retries.",
    }


# ============================================================
# Signal Storage
# ============================================================

def store_signal(
    market_id: str,
    ticker: str,
    result: dict,
    final_score: float,
    kw_score: int,
    matched_keywords: list[str],
    themes: dict[str, int],
) -> None:
    """Insert one signal row with full metadata for auditability."""
    signal = {
        "market_id": market_id,
        "stock_ticker": ticker,

        # scoring
        "relevance_score": final_score,
        "llm_score": result.get("relevance_score", 0.0),
        "keyword_score": kw_score,

        # signal
        "signal_direction": result.get("direction", "neutral"),
        "reasoning": result.get("reasoning", ""),

        # explainability
        "matched_keywords": matched_keywords,
        "themes": list(themes.keys()),

        "model_used": MODEL_NAME,
    }
    try:
        supabase.table("signals").insert(signal).execute()
    except Exception as e:
        logger.error(f"Error storing signal for {market_id} -> {ticker}: {e}")


# ============================================================
# Market Fetching with Pre-LLM Ranking (Layer 2c)
# ============================================================

def get_unprocessed_markets(max_llm_candidates: int = 50) -> list[dict]:
    """
    Fetch and rank unprocessed markets.

    Pipeline:
      1. Pull top 1000 active markets by volume (no strict category filter)
      2. Score each with category_score() + keyword_score()
      3. Apply dynamic soft gate (cat, volume, kw)
      4. Rank by pre-LLM score = kw_score * 2 + volume_norm
      5. Return top-N for LLM classification
    """
    processed_res = supabase.table("signals").select("market_id").execute()
    processed_ids = {s["market_id"] for s in (processed_res.data or [])}

    # Fetch broad set
    res = (
        supabase.table("markets")
        .select("*, llm_processed, llm_result")
        .eq("active", True)
        .eq("closed", False)
        .order("volume_total", desc=True)
        .limit(1000)
        .execute()
    )

    all_markets = res.data or []
    unprocessed = [m for m in all_markets if m["id"] not in processed_ids or m.get("llm_processed") is not True]

    # --- Score and apply dynamic soft gate ---
    scored_candidates: list[tuple[float, dict]] = []

    for m in unprocessed:
        cat = category_score(m.get("category"))
        kw, matched, themes = keyword_score(m)

        # Store on the market dict to avoid recompute in main loop
        m["_kw_score"] = kw
        m["_matched_keywords"] = matched
        m["_themes"] = themes

        vol = float(m.get("volume_total", 0) or 0)

        # Dynamic soft gate
        if (cat >= 2 and vol >= 10_000) or (kw >= 6) or (kw >= 4 and vol >= 50_000):
            # Pre-LLM ranking score
            pre_score = (
                kw * 1.5 +
                min(vol / 100_000, 1.0) * 1.5 +
                category_score(m.get("category")) * 1.0 +
                probability_strength(m.get("outcomes", [])) * 1.0 +
                (1 if kw >= 6 and len(m.get("_themes", {})) >= 2 else 0) * 1.5
            )
            scored_candidates.append((pre_score, m))

    # Sort by pre-score descending
    scored_candidates.sort(reverse=True, key=lambda x: x[0])

    # Log funnel
    logger.info("")
    logger.info("Market funnel (soft + ranked):")
    logger.info(f"  Total active markets:     {len(all_markets)}")
    logger.info(f"  Already processed:        {len(processed_ids)}")
    logger.info(f"  Unprocessed:              {len(unprocessed)}")
    logger.info(f"  Passed soft gate:         {len(scored_candidates)}")
    logger.info(f"  Top-N for LLM:            {min(max_llm_candidates, len(scored_candidates))}")

    return [m for _, m in scored_candidates[:max_llm_candidates]]


# ============================================================
# Main Pipeline
# ============================================================

def run_filter_pipeline(batch_size: int = 50):
    """Main entry point — runs full filter pipeline."""
    start_time = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info(f"FILTER PIPELINE START — {start_time.isoformat()}")
    logger.info(f"Model: {MODEL_NAME}")
    logger.info("=" * 60)

    # Load context
    logger.info("[1/3] Loading stock context...")
    stock_context, valid_tickers = load_stock_context()
    logger.info(f"Tracking {len(valid_tickers)} active stocks: {sorted(valid_tickers)}")

    # Fetch ranked candidates
    logger.info("[2/3] Fetching and ranking unprocessed markets...")
    markets = get_unprocessed_markets(batch_size)

    if not markets:
        logger.info("No new markets to classify. Pipeline complete.")
        return

    # Classify
    logger.info(f"[3/3] Classifying {len(markets)} markets with Gemini...")
    total_signals    = 0
    irrelevant_count = 0
    failed_count     = 0
    llm_cache: dict[str, dict] = {}

    for i, market in enumerate(markets):
        question_preview = market["question"][:80]
        cat  = market.get("category", "?")
        vol  = market.get("volume_total", 0)
        kw      = market.get("_kw_score", 0)
        matched = market.get("_matched_keywords", [])
        themes  = market.get("_themes", {})

        logger.info(f"[{i+1}/{len(markets)}] [{cat}] ${vol:,.0f} | kw={kw} | {question_preview}...")

        market_id = market["id"]

        if market.get("llm_processed"):
            raw = market.get("llm_result")
            if isinstance(raw, str):
                try:
                    result = json.loads(raw)
                except Exception:
                    result = classify_market(market, stock_context)
            else:
                result = raw or {}
        else:
            result = classify_market(market, stock_context)
            # persist result as JSON string
            supabase.table("markets").update({
                "llm_processed": True,
                "llm_result": json.dumps(result)
            }).eq("id", market_id).execute()

        # --- Hybrid relevance decision ---
        llm_relevant = result.get("relevant", False)
        llm_score = result.get("relevance_score", 0.0)

        # LLM says relevant with confidence OR strong multi-theme keyword signal
        final_relevant = (
            (llm_relevant and llm_score >= 0.5) or
            (kw >= 8 and len(themes) >= 2)
        )

        if final_relevant and result["tickers"]:
            signals_stored = 0
            final_score = compute_final_score(market, llm_score, kw)

            for ticker in result["tickers"]:
                if ticker in valid_tickers:
                    store_signal(
                        market["id"],
                        ticker,
                        result,
                        final_score,
                        kw,
                        matched,
                        themes,
                    )
                    total_signals += 1
                    signals_stored += 1
                else:
                    logger.warning(f"Skipping unknown ticker: {ticker}")

            if signals_stored:
                source = "LLM+kw" if llm_relevant else "kw-only"
                logger.info(f"  ✅ RELEVANT ({source}) → {result['tickers']} | {result['direction']} | final={final_score:.2f}")
                if matched:
                    logger.info(f"  🔑 Keywords: {matched[:5]} | Themes: {list(themes.keys())}")
                logger.info(f"  📝 {result['reasoning'][:120]}...")
        else:
            irrelevant_count += 1
            if result.get("reasoning", "").startswith("Classification failed"):
                failed_count += 1
                logger.warning(f"  ❌ FAILED")
            else:
                logger.info(f"  ➖ Irrelevant (LLM: {llm_relevant}, score: {llm_score:.2f}, kw: {kw})")

        time.sleep(5)  # ~12 req/min for free tier

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE — {elapsed:.0f}s")
    logger.info(f"  Markets classified: {len(markets)}")
    logger.info(f"  Signals stored:     {total_signals}")
    logger.info(f"  Irrelevant:         {irrelevant_count}")
    logger.info(f"  Failed:             {failed_count}")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_filter_pipeline()