"""
pipeline/report_generator.py

LLM-powered daily signal report generator.
Pulls recent signals with full metadata, ranks them by multiple factors,
and generates a structured analyst report via Gemini. Stores reports in DB
with traceability to source signals.

Run: python pipeline/report_generator.py
"""
import os
import json
import logging
from google import genai
from supabase import create_client, Client
from datetime import datetime, timezone, timedelta
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
REPORT_MODEL = "gemini-2.5-flash"
model = genai.GenerativeModel(REPORT_MODEL)

# ============================================================
# Config
# ============================================================
MIN_SIGNALS_FOR_REPORT  = 3        # Don't generate if fewer than this
MAX_SIGNALS_IN_REPORT   = 20       # Cap signals sent to LLM (token limit)
MAX_SIGNALS_PER_TICKER  = 5        # Don't let one ticker dominate
DEFAULT_LOOKBACK_HOURS  = 24
MIN_RELEVANCE_SCORE     = 0.3      # Only include signals above this


# ============================================================
# Signal Fetching
# ============================================================

def fetch_recent_signals(hours: int = DEFAULT_LOOKBACK_HOURS) -> list[dict]:
    """
    Fetch signals from the last N hours, joined with market data.
    Applies diversity cap: max 5 signals per ticker to prevent
    one stock from dominating the report.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    signals = (
        supabase.table("signals")
        .select(
            "id, "
            "stock_ticker, "
            "signal_direction, "
            "relevance_score, "
            "llm_score, "
            "keyword_score, "
            "reasoning, "
            "matched_keywords, "
            "themes, "
            "created_at, "
            "markets!inner(question, outcomes, volume_total, end_date, category)"
        )
        .gte("created_at", cutoff)
        .gte("relevance_score", MIN_RELEVANCE_SCORE)
        .order("relevance_score", desc=True)
        .limit(100)                              # Fetch more, then cap by ticker
        .execute()
    )

    all_signals = signals.data or []

    # Diversity cap: max MAX_SIGNALS_PER_TICKER per ticker
    ticker_counts: dict[str, int] = {}
    capped_signals = []
    for s in all_signals:
        ticker = s.get("stock_ticker", "UNKNOWN")
        ticker_counts[ticker] = ticker_counts.get(ticker, 0) + 1
        if ticker_counts[ticker] <= MAX_SIGNALS_PER_TICKER:
            capped_signals.append(s)

    # Final cap
    capped_signals = capped_signals[:MAX_SIGNALS_IN_REPORT]

    logger.info(f"Fetched {len(all_signals)} raw signals")
    logger.info(f"After diversity cap: {len(capped_signals)} signals")
    logger.info(f"Ticker distribution: {ticker_counts}")

    return capped_signals


# ============================================================
# Prompt Construction
# ============================================================

def _parse_outcomes(outcomes_raw) -> list[dict]:
    """Safely parse outcomes JSON."""
    if isinstance(outcomes_raw, str):
        try:
            return json.loads(outcomes_raw)
        except Exception:
            return []
    return outcomes_raw or []


def _format_signal(s: dict) -> str:
    """Format a single signal for the LLM prompt with full metadata."""
    market       = s.get("markets", {})
    outcomes     = _parse_outcomes(market.get("outcomes", "[]"))
    top_outcome  = outcomes[0] if outcomes else {"name": "?", "price": 0}

    direction_emoji = {
        "bullish": "🟢 BULLISH",
        "bearish": "🔴 BEARISH",
        "neutral": "⚪ NEUTRAL",
        "mixed":   "🟡 MIXED",
    }.get(s.get("signal_direction", "neutral"), s.get("signal_direction", "").upper())

    themes_str   = ", ".join(s.get("themes", [])) if s.get("themes") else "N/A"
    keywords_str = ", ".join(s.get("matched_keywords", [])[:5]) if s.get("matched_keywords") else "N/A"
    expiry       = market.get("end_date", "N/A")

    # Calculate days to expiry
    if expiry and expiry != "N/A":
        try:
            exp_date = datetime.fromisoformat(str(expiry).replace("Z", "+00:00"))
            days_left = max((exp_date - datetime.now(timezone.utc)).days, 0)
            expiry_str = f"{expiry} ({days_left} days)"
        except Exception:
            expiry_str = str(expiry)
    else:
        expiry_str = "N/A"

    return (
        f"### {s['stock_ticker']} | {direction_emoji} | Score: {s['relevance_score']:.2f}\n"
        f"- **LLM Score**: {s.get('llm_score', 'N/A')} | **Keyword Score**: {s.get('keyword_score', 'N/A')}\n"
        f"- **Market**: {market.get('question', 'N/A')}\n"
        f"- **Top outcome**: {top_outcome['name']} at {float(top_outcome['price']):.0%}\n"
        f"- **Volume**: ${market.get('volume_total', 0):,.0f}\n"
        f"- **Expiry**: {expiry_str}\n"
        f"- **Themes**: {themes_str}\n"
        f"- **Keywords**: {keywords_str}\n"
        f"- **Reasoning**: {s.get('reasoning', 'N/A')}\n"
    )


def build_report_prompt(signals: list[dict], tickers: list[str]) -> str:
    """Build the analyst report prompt with structured signal data."""
    signal_text = "\n".join(_format_signal(s) for s in signals)
    ticker_str  = ", ".join(tickers)

    # Summary statistics
    bullish    = sum(1 for s in signals if s.get("signal_direction") == "bullish")
    bearish    = sum(1 for s in signals if s.get("signal_direction") == "bearish")
    neutral    = sum(1 for s in signals if s.get("signal_direction") == "neutral")
    mixed      = sum(1 for s in signals if s.get("signal_direction") == "mixed")
    avg_score  = sum(s.get("relevance_score", 0) for s in signals) / max(len(signals), 1)
    total_vol  = sum(
        float(s.get("markets", {}).get("volume_total", 0) or 0)
        for s in signals
    )

    # Collect all themes
    all_themes: set[str] = set()
    for s in signals:
        for t in (s.get("themes") or []):
            all_themes.add(t)

    # Ticker-level summary
    ticker_summary: dict[str, dict] = {}
    for s in signals:
        t = s.get("stock_ticker", "UNKNOWN")
        if t not in ticker_summary:
            ticker_summary[t] = {"count": 0, "bullish": 0, "bearish": 0, "avg_score": 0}
        ticker_summary[t]["count"] += 1
        if s.get("signal_direction") == "bullish":
            ticker_summary[t]["bullish"] += 1
        elif s.get("signal_direction") == "bearish":
            ticker_summary[t]["bearish"] += 1
        ticker_summary[t]["avg_score"] += s.get("relevance_score", 0)

    for t in ticker_summary:
        ticker_summary[t]["avg_score"] /= max(ticker_summary[t]["count"], 1)

    ticker_lines = []
    for t, data in sorted(ticker_summary.items()):
        bias = "🟢" if data["bullish"] > data["bearish"] else "🔴" if data["bearish"] > data["bullish"] else "⚪"
        ticker_lines.append(
            f"- {bias} **{t}**: {data['count']} signals, "
            f"avg score {data['avg_score']:.2f}"
        )

    ticker_overview = "\n".join(ticker_lines)

    return f"""You are the lead equity strategist at BIT Capital, a tech-focused investment fund.
Our portfolio managers need a daily signal report based on prediction market activity.

## Context
- **Date**: {datetime.now().strftime('%B %d, %Y')}
- **Stocks covered**: {ticker_str}
- **Total signals**: {len(signals)}
- **Directional breakdown**: {bullish} bullish, {bearish} bearish, {neutral} neutral, {mixed} mixed
- **Average relevance**: {avg_score:.2f}
- **Total market volume**: ${total_vol:,.0f}
- **Dominant themes**: {", ".join(sorted(all_themes)) if all_themes else "None"}

## Ticker Overview
{ticker_overview}

## Signals (ranked by relevance)
{signal_text}

## Instructions
Write a professional, insight-driven report. Structure it as follows (use markdown):

# Polymarket Signal Report — {datetime.now().strftime('%B %d, %Y')}

## Executive Summary
2-4 sentences covering:
- Overall signal environment (risk-on / risk-off / mixed)
- Dominant theme driving today's signals
- Most actionable insight for PMs RIGHT NOW
- Any notable change from prior signals (if detectable)

## Top 3-5 Most Actionable Signals
For each signal include:
- **Market event & current odds**: What is being predicted, what the market implies
- **Stocks affected**: Which tickers, why they're exposed, through what mechanism
- **Direction & magnitude**: How big could the move be? What's the transmission channel?
- **PM action**: Specific recommendation — monitor, reduce, add, hedge, size up, size down
- **Confidence**: Is this a high-conviction signal or speculative?

## Thematic Observations
Group signals by theme. For each theme:
- What do the collective signals suggest?
- Is there a consensus forming or are markets divided?
- How does this map to BIT Capital's current positioning?
- Are there correlated signals across tickers?

## Risk Calendar
- Events expiring in the next 7 days (near-term catalysts)
- Events expiring in 8-30 days (medium-term)
- Concentration risk: are multiple signals pointing at the same underlying event?
- Calendar gaps: any important dates with no coverage?

## Signal Heatmap
A markdown table with ALL signals:
| Ticker | Direction | Score | Signal | Top Outcome | Prob | Volume | Expiry | Themes |
|--------|-----------|-------|--------|-------------|------|--------|--------|--------|

## Key Takeaways for PMs
3-5 bullet points:
- What to act on today
- What to watch this week  
- What's noise / can be ignored
- Portfolio-level implications (not just single-stock)

Be specific, data-driven, and actionable. Reference probabilities and volumes explicitly.
Do not summarize — interpret. Every signal should answer: "so what?"
"""


# ============================================================
# Report Generation
# ============================================================

def _clean_llm_output(text: str) -> str:
    """Remove markdown fences and whitespace from LLM output."""
    text = text.strip()
    if text.startswith("```markdown"):
        text = text[11:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    return text.strip()


def generate_report(hours: int = DEFAULT_LOOKBACK_HOURS) -> dict:
    """
    Generate a report from recent signals and store it in the database.

    Returns:
        dict with keys: id, title, content, tickers, signal_count
    """
    start_time = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info(f"REPORT GENERATOR START — {start_time.isoformat()}")
    logger.info("=" * 60)

    # Fetch signals
    logger.info(f"Fetching signals from last {hours} hours...")
    signals = fetch_recent_signals(hours)

    if len(signals) < MIN_SIGNALS_FOR_REPORT:
        logger.warning(
            f"Only {len(signals)} signals found (minimum {MIN_SIGNALS_FOR_REPORT}). "
            "Skipping report generation."
        )
        return {
            "id": None,
            "title": "Insufficient signals",
            "content": (
                f"Only {len(signals)} signals above relevance threshold "
                f"({MIN_RELEVANCE_SCORE}) in the last {hours} hours. "
                f"Minimum {MIN_SIGNALS_FOR_REPORT} required for a report."
            ),
            "tickers": [],
            "signal_count": len(signals),
        }

    # Extract unique tickers
    tickers = sorted(list({s["stock_ticker"] for s in signals}))
    logger.info(f"Found {len(signals)} signals across {len(tickers)} tickers: {tickers}")

    # Build prompt and generate
    logger.info("Building report prompt...")
    prompt = build_report_prompt(signals, tickers)

    logger.info(f"Generating report with {REPORT_MODEL}...")
    try:
        response = genai_client.models.generate_content(
            model=REPORT_MODEL,
            contents=prompt,
        )
        report_content = _clean_llm_output(response.text)
    except Exception as e:
        logger.error(f"LLM generation failed: {e}")
        return {
            "id": None,
            "title": "Report generation failed",
            "content": f"LLM generation error: {e}",
            "tickers": tickers,
            "signal_count": len(signals),
        }

    # Store report
    title = f"Polymarket Signal Report — {datetime.now().strftime('%Y-%m-%d')}"
    report_data = {
        "title": title,
        "content": report_content,
        "tickers": tickers,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    logger.info("Storing report in database...")
    result = supabase.table("reports").insert(report_data).execute()
    report_id = result.data[0]["id"] if result.data else None

    # Link signals to report for traceability
    if report_id:
        linked = 0
        for s in signals:
            try:
                supabase.table("report_signals").insert({
                    "report_id": report_id,
                    "signal_id": s["id"],
                }).execute()
                linked += 1
            except Exception as e:
                logger.warning(f"Failed to link signal {s['id']}: {e}")
        logger.info(f"Linked {linked}/{len(signals)} signals to report {report_id}")

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info(f"REPORT GENERATOR COMPLETE — {elapsed:.0f}s")
    logger.info(f"  Report ID:     {report_id}")
    logger.info(f"  Tickers:       {tickers}")
    logger.info(f"  Signal count:  {len(signals)}")
    logger.info("=" * 60)

    return {
        "id": report_id,
        "title": title,
        "content": report_content,
        "tickers": tickers,
        "signal_count": len(signals),
    }


# ============================================================
# Entry Point
# ============================================================
if __name__ == "__main__":
    report = generate_report()
    if report["content"]:
        print("\n" + "=" * 80)
        print(report["content"])
        print("=" * 80)
    else:
        print("No report generated.")