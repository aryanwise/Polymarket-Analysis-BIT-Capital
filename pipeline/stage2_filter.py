"""
pipeline/stage2_filter.py

Stage 2: LLM classification + ticker mapping only.

Intentionally minimal — does two things and nothing else:
  1. Is this market relevant to BIT Capital?  (SIGNAL or NOISE)
  2. If SIGNAL, which holdings are affected?  (ticker list)

Sentiment, reasoning, impact scoring, and time horizon are NOT done here.
That analysis belongs in report_generator.py where the LLM has full context
across all signals — not just one market at a time.

This separation improves:
  - Accuracy: LLM focused on classification, not multi-task
  - Token efficiency: ~60% fewer tokens per call = fewer rate limit issues
  - Data quality: reasoning done with portfolio-wide context in report

Gemini primary → Groq fallback.
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import re
import time
import logging
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── Model config ──────────────────────────────────────────────
GEMINI_MODEL  = "gemini-2.0-flash-lite"
GROQ_MODEL    = "llama-3.3-70b-versatile"
BATCH_SIZE    = 10     # markets per LLM call
BATCH_DELAY   = 2.0    # seconds between batches

# ── BIT Capital holdings ──────────────────────────────────────
# Describes what each holding is sensitive to.
# Kept concise — the LLM only needs to classify, not reason.
HOLDINGS = {
    "IREN":  "Bitcoin miner. Sensitive to: BTC price, energy costs, Fed rates.",
    "HUT":   "Crypto miner + AI compute. Sensitive to: BTC price, energy costs.",
    "COIN":  "Crypto exchange. Sensitive to: BTC/ETH price, crypto regulation.",
    "HOOD":  "Retail trading + crypto. Sensitive to: Fed rates, crypto regulation.",
    "NVDA":  "AI chips. Sensitive to: chip export controls, Taiwan risk, AI capex.",
    "TSM":   "Semiconductor foundry. Sensitive to: Taiwan/China military, chip export controls.",
    "MU":    "AI memory (HBM). Sensitive to: chip export controls, Taiwan risk, AI capex.",
    "MSFT":  "Azure + OpenAI. Sensitive to: AI regulation, antitrust, cloud growth.",
    "GOOGL": "Search + AI. Sensitive to: AI regulation, antitrust, AI competition.",
    "AMZN":  "AWS + e-commerce. Sensitive to: AI capex, import tariffs, antitrust.",
    "META":  "Social media + AI. Sensitive to: AI regulation, ad market.",
    "DDOG":  "Cloud monitoring. Sensitive to: enterprise AI spending, cloud growth.",
    "LMND":  "AI insurer. Sensitive to: Fed rates, interest rates.",
    "RDDT":  "AI data licensing. Sensitive to: AI regulation, data rights.",
}

VALID_TICKERS = set(HOLDINGS.keys())

# ── Classification prompt ─────────────────────────────────────
# Short and focused. One decision: SIGNAL + tickers, or NOISE.
# No reasoning, no sentiment, no scoring — that's report_generator's job.

SYSTEM_PROMPT = """You are a portfolio analyst at BIT Capital, a tech-focused fund.

Your only job: decide if a Polymarket market is relevant to our holdings.

OUR HOLDINGS:
""" + "\n".join(f"  {k}: {v}" for k, v in HOLDINGS.items()) + """

CLASSIFY AS SIGNAL if the market outcome would directly affect any holding's revenue, costs, or regulatory environment. List only the genuinely affected tickers.
Do not require perfect certainty. If a reasonable, direct mechanism exists, mark SIGNAL.

CLASSIFY AS NOISE if there is no clear direct impact on any holding.
Do not force a connection. If unsure, it is NOISE.

SIGNAL categories (classify these):
- BTC/ETH/crypto prices            → IREN, HUT, COIN, HOOD
- Fed rate decisions               → IREN, HUT, LMND, HOOD
- US chip export controls to China → NVDA, TSM, MU
- Taiwan/China military conflict   → TSM, NVDA, MU
- AI regulation / antitrust        → MSFT, GOOGL, META, AMZN, NVDA, DDOG
- Tech/semiconductor tariffs       → TSM, NVDA, AMZN
- Iran / Strait of Hormuz / energy → IREN, HUT
- Crypto regulation                → COIN, HOOD
- Major AI company IPO or M&A      → MSFT, GOOGL (competitive impact)
- Stablecoin regulation or adoption → COIN, HOOD
- Cloud spending indicators        → DDOG, MSFT, AMZN, GOOGL

NOISE (do not classify these as SIGNAL):
- Presidential travel, cabinet changes
- Foreign elections with no trade outcome
- EM country economic data
- Random token launches
- Sports, entertainment, celebrity
- Diplomatic meetings without policy result

OUTPUT — strictly one line per market, using this format:
[index] THOUGHT: <one-sentence reasoning> | RESULT: <SIGNAL: TICKER1,TICKER2 or NOISE>

Examples:
[0] THOUGHT: Fed rate cuts decrease borrowing costs for high-debt miners and boost lending demand. | RESULT: SIGNAL: IREN,HUT,COIN,HOOD
[1] THOUGHT: Diplomatic meeting in South America has no trade impact on our tech stack. | RESULT: NOISE
[2] THOUGHT: Chip export controls directly impact supply chain for leading edge nodes. | RESULT: SIGNAL: NVDA,TSM,MU
[3] THOUGHT: Microsoft Azure growth directly correlates with enterprise cloud spending. | RESULT: SIGNAL: MSFT,GOOGL
[4] THOUGHT: Random celebrity token launch has no bearing on our equity holdings. | RESULT: NOISE"""


def _build_prompt(markets: list[dict]) -> str:
    """
    Minimal prompt — question + tags + probability only.
    No event title fluff, no volume — LLM only needs to classify.
    """
    lines = []
    for i, m in enumerate(markets):
        yes  = float(m.get("yes_price") or 0)
        tags = str(m.get("tags", ""))[:80]
        lines.append(
            f"[{i}] {m.get('question', '')}\n"
            f"    Tags: {tags} | YES: {yes:.0%}"
        )
    return "Classify each market:\n\n" + "\n\n".join(lines)


def _parse_response(text: str, n: int) -> list[list[str] | None]:
    """
    Parse LLM output into list of ticker lists or None.
    Returns: list of length n where each element is:
      - list of valid ticker strings  → SIGNAL
      - None                          → NOISE
    """
    results = [None] * n

    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue

        m = re.search(r"\[(\d+)\].*?RESULT:\s*(SIGNAL|NOISE)[:\s]*(.*)", line, re.I)

        if not m:
            continue

        idx   = int(m.group(1))
        label = m.group(2).upper()
        rest  = m.group(3).strip()

        if idx >= n:
            continue

        if label == "NOISE" or not rest:
            results[idx] = None
            continue

        # Extract tickers — comma separated, filter to valid ones only
        raw_tickers = [t.strip().upper() for t in rest.split(",") if t.strip()]
        valid       = [t for t in raw_tickers if t in VALID_TICKERS]

        # Only mark as SIGNAL if at least one valid ticker found
        results[idx] = valid if valid else None

    found = sum(1 for r in results if r is not None)
    logger.info("  Parsed: %d signals, %d noise from %d markets", found, n - found, n)
    return results


def _call_gemini(prompt: str) -> str | None:
    """Call Gemini. Returns raw text or None on failure."""
    try:
        from google import genai
        from google.genai import types

        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            return None

        client   = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                temperature=0.0,
                max_output_tokens=500,   # minimal output = faster + cheaper
            ),
        )
        return response.text

    except Exception as e:
        if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
            logger.warning("Gemini rate limited: %s", e)
        else:
            logger.warning("Gemini error: %s", e)
        return None


def _call_groq(prompt: str) -> str | None:
    """Call Groq. Returns raw text or None on failure."""
    try:
        from groq import Groq

        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            return None

        client   = Groq(api_key=api_key)
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.0,
            max_tokens=500,
        )
        return response.choices[0].message.content

    except Exception as e:
        if "429" in str(e) or "rate" in str(e).lower():
            logger.warning("Groq rate limited: %s", e)
        else:
            logger.warning("Groq error: %s", e)
        return None
    

def _call_llm(prompt: str, batch_num: int) -> tuple[str | None, str]:
    """Returns (raw_text, model_name_used)"""
    for attempt in range(3):
        raw = _call_gemini(prompt)
        if raw:
            logger.info("Batch %d — Gemini OK", batch_num)
            return raw, GEMINI_MODEL

        raw = _call_groq(prompt)
        if raw:
            logger.info("Batch %d — Groq fallback OK", batch_num)
            return raw, GROQ_MODEL

        # Wait 30s, then 60s, then 120s
        wait = 30 * (2 ** attempt) 
        logger.warning("Both APIs failed (attempt %d/3) — waiting %ds...", attempt + 1, wait)
        time.sleep(wait)

    return None, "none"


def run_stage2(
    df:       pd.DataFrame,
    supabase  = None,
    dry_run:  bool = False,
) -> tuple[pd.DataFrame, dict]:
    """
    Classifies Stage 1 output.
    Writes one row per (market × ticker) to Supabase signals table.

    DB fields populated here:   market_id, event_id, event_title, question,
                                 tags, yes_price, volume, end_date, ticker
    DB fields left NULL (for report_generator):
                                 sentiment, impact_score, impact_type,
                                 time_horizon, reasoning
    """
    logger.info("=" * 55)
    logger.info("STAGE 2 START — %d markets | Gemini→Groq", len(df))
    logger.info("=" * 55)

    signal_rows = []
    stats = {"total": 0, "signals": 0, "noise": 0, "errors": 0, "db_writes": 0}
    markets = df.to_dict("records")

    for batch_start in range(0, len(markets), BATCH_SIZE):
        batch     = markets[batch_start:batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (len(markets) + BATCH_SIZE - 1) // BATCH_SIZE

        stats["total"] += len(batch)
        logger.info("Processing batch %d/%d (%d markets)...",
                    batch_num, total_batches, len(batch))

        prompt = _build_prompt(batch)
        raw, model_used = _call_llm(prompt, batch_num)

        if raw is None:
            logger.error("Batch %d skipped — no LLM response", batch_num)
            stats["errors"] += len(batch)
            time.sleep(BATCH_DELAY)
            continue

        logger.debug("Raw response:\n%s", raw)
        results = _parse_response(raw, len(batch))

        # Save results
        for market, tickers in zip(batch, results):
            if not tickers:
                stats["noise"] += 1
                continue

            stats["signals"] += 1
            for ticker in tickers:
                row = {
                    # Market data from ingest
                    "market_id":   str(market.get("market_id", "")),
                    "event_id":    str(market.get("event_id", "")),
                    "event_title": market.get("event_title", ""),
                    "question":    market.get("question", ""),
                    "tags":        market.get("tags", ""),
                    "yes_price":   float(market.get("yes_price") or 0),
                    "volume":      float(market.get("volume") or 0),
                    "end_date":    market.get("end_date") or None,
                    # Classification output
                    "ticker":      ticker,
                    "model_used":  model_used,
                    # These are intentionally NULL — report_generator fills them
                    # sentiment, impact_score, impact_type, time_horizon, reasoning
                }
                signal_rows.append(row)
                logger.info("  ✓ SIGNAL  %s → %s", market.get("question","")[:60], ticker)

                if supabase and not dry_run:
                    try:
                        supabase.table("signals").upsert(
                            row, on_conflict="market_id,ticker"
                        ).execute()
                        stats["db_writes"] += 1
                    except Exception as e:
                        logger.error("DB write failed (%s, %s): %s",
                                     market.get("market_id"), ticker, e)

        logger.info("Batch %d done — signals=%d noise=%d",
                    batch_num, stats["signals"], stats["noise"])
        time.sleep(BATCH_DELAY)

    signals_df = pd.DataFrame(signal_rows) if signal_rows else pd.DataFrame()

    logger.info("=" * 55)
    logger.info("STAGE 2 COMPLETE")
    logger.info("  Total processed : %d", stats["total"])
    logger.info("  Signals found   : %d", stats["signals"])
    logger.info("  Noise rejected  : %d", stats["noise"])
    logger.info("  Errors          : %d", stats["errors"])
    logger.info("  DB writes       : %d", stats["db_writes"])
    logger.info("=" * 55)

    return signals_df, stats