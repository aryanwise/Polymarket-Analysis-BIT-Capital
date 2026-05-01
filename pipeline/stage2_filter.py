"""
pipeline/stage2_filter.py

Stage 2: Gemini LLM classification.
Reads Stage 1 DataFrame → classifies each market as SIGNAL or NOISE
→ writes SIGNAL rows to Supabase signals table.

Uses batched prompts (10 markets per API call) to stay within
Gemini free tier limits (60 req/min, 1M tokens/day).

Called by scheduler.py and run_pipeline.py
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import re
import time
import logging
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

GEMINI_MODEL  = "gemini-flash-lite-latest"
BATCH_SIZE    = 10     # markets per Gemini call
BATCH_DELAY   = 1.2    # seconds between batches (free tier safe)

# ── BIT Capital context ───────────────────────────────────────

HOLDINGS = {
    "IREN":  "Bitcoin mining + AI data centers. Driven by: BTC price, Fed rates, energy costs (oil/Hormuz).",
    "HUT":   "Crypto mining + AI compute. Driven by: BTC price, energy costs, crypto regulation.",
    "COIN":  "Crypto exchange. Driven by: crypto regulation, BTC/ETH price, SEC actions, trading volumes.",
    "HOOD":  "Retail trading + crypto gateway. Driven by: Fed rates, crypto regulation, retail sentiment.",
    "NVDA":  "AI accelerators. Driven by: AI capex, chip export controls to China, Taiwan risk.",
    "TSM":   "Semiconductor foundry. Driven by: Taiwan/China military, chip export controls, AI capex.",
    "MU":    "AI memory chips (HBM). Driven by: AI capex, chip export controls, Taiwan risk.",
    "MSFT":  "Azure + OpenAI. Driven by: AI regulation, antitrust, enterprise AI spending, cloud growth.",
    "GOOGL": "Search + Gemini AI. Driven by: AI regulation, antitrust, ad market, AI model competition.",
    "AMZN":  "AWS + Bedrock AI. Driven by: AI capex, import tariffs, antitrust.",
    "META":  "Social media + Llama AI. Driven by: AI regulation, ad market, antitrust.",
    "DDOG":  "Cloud monitoring. Driven by: enterprise AI spending, cloud growth, economic slowdown.",
    "LMND":  "AI insurtech. Driven by: Fed rates, interest rates, insurance claims.",
    "RDDT":  "AI data licensing + ads. Driven by: AI regulation, data licensing deals, ad market.",
}

VALID_TICKERS = set(HOLDINGS.keys())

SYSTEM_PROMPT = """You are a senior equity analyst at BIT Capital, a Berlin-based technology fund.

━━ HOLDINGS ━━
""" + "\n".join(f"  {k}: {v}" for k, v in HOLDINGS.items()) + """

━━ SIGNAL CRITERIA — classify as SIGNAL if the market affects any holding through: ━━
1. Fed rates / monetary policy → LMND, HOOD, IREN, HUT cost of capital
2. Bitcoin / crypto prices → IREN, HUT mining revenue, COIN volumes, HOOD crypto revenue
3. Taiwan / chip export controls → TSM production, NVDA/MU supply chain
4. AI regulation / major AI M&A → MSFT, GOOGL, META, NVDA, AMZN, DDOG, RDDT
5. Tariffs / trade policy → TSM costs, NVDA China revenue, AMZN imports
6. Geopolitics / energy prices → IREN, HUT electricity costs via oil/gas/Hormuz

━━ NOISE — classify as NOISE if: ━━
- Presidential travel to US states (no policy outcome)
- Cabinet personnel with no direct equity impact
- Foreign diplomatic meetings with no trade/tariff outcome
- Middle East politics NOT involving Iran/Hormuz/energy prices
- Foreign inflation data (no EM exposure in portfolio)
- Sports, entertainment, elections, celebrity news
- Random token launches with no holding relevance
- Novelty/parlay markets ("Nothing Ever Happens")

━━ LABELED EXAMPLES ━━
SIGNAL: "Will the Fed cut rates by 25bps?" → rate cuts lower LMND/HOOD cost of capital, boost IREN/HUT
SIGNAL: "Will Bitcoin reach $100k?" → BTC directly drives IREN/HUT mining revenue and COIN volumes
SIGNAL: "Will China invade Taiwan?" → TSM production disruption → NVDA/MU supply shock
SIGNAL: "Will US impose chip export controls?" → hits NVDA China revenue, TSM customer base
SIGNAL: "Will OpenAI IPO above $1T?" → validates AI, affects MSFT (partner), GOOGL (competition)
SIGNAL: "AI bubble burst in 2026?" → simultaneous hit to MSFT/GOOGL/AMZN/NVDA/DDOG/META
SIGNAL: "Will Strait of Hormuz normalize?" → energy prices affect IREN/HUT electricity costs
SIGNAL: "Will stablecoin regulation pass?" → legitimizes crypto → bullish COIN/HOOD
NOISE: "Will Trump visit Wyoming?" → no policy outcome
NOISE: "Will Mahmoud Abbas resign?" → no transmission to holdings
NOISE: "Will Venezuela oil reach 1.7m barrels?" → EM commodity, no direct holding impact

━━ OUTPUT FORMAT — one line per market ━━
[index] SIGNAL: ticker1,ticker2 | sentiment | impact_type | time_horizon | reasoning
[index] NOISE: - | - | - | - | reason why irrelevant

Rules:
- sentiment: exactly Bullish, Bearish, or Neutral
- impact_type: exactly one of margin, revenue, sentiment, regulatory, operational
- time_horizon: exactly one of short-term, medium-term, long-term
- reasoning: one sentence using format "[event] → [what changes] → [financial impact] → [why stock moves]"
- Only use tickers: IREN HUT COIN HOOD NVDA TSM MU MSFT GOOGL AMZN META DDOG LMND RDDT
- One line per market. No extra text."""


def _build_batch_prompt(markets: list[dict]) -> str:
    lines = []
    for i, m in enumerate(markets):
        yes = float(m.get("yes_price") or 0)
        lines.append(
            f"[{i}] Event: {str(m.get('event_title',''))[:80]}\n"
            f"    Question: {m.get('question','')}\n"
            f"    Tags: {str(m.get('tags',''))[:100]}\n"
            f"    YES probability: {yes:.0%}\n"
            f"    Volume: ${float(m.get('volume') or 0):,.0f}\n"
        )
    return "Classify each market:\n\n" + "\n".join(lines)


def _parse_response(text: str, n: int) -> list[dict | None]:
    """
    Parses Gemini batch response.
    Returns list of dicts (SIGNAL) or None (NOISE) per market.
    """
    results = [None] * n

    for line in text.strip().splitlines():
        m = re.match(
            r"\[(\d+)\]\s*(SIGNAL|NOISE):\s*([^\|]+)\|\s*([^\|]*)\|\s*([^\|]*)\|\s*([^\|]*)\|\s*(.*)",
            line.strip(), re.I,
        )
        if not m:
            continue

        idx        = int(m.group(1))
        label      = m.group(2).upper()
        tickers    = [t.strip() for t in m.group(3).split(",") if t.strip() and t.strip() != "-"]
        sentiment  = m.group(4).strip()
        imp_type   = m.group(5).strip()
        horizon    = m.group(6).strip()
        reasoning  = m.group(7).strip()

        if idx >= n:
            continue

        if label == "SIGNAL" and tickers:
            valid_tickers = [t for t in tickers if t in VALID_TICKERS]
            if not valid_tickers:
                continue

            # Coerce to valid enum values
            if sentiment  not in ("Bullish", "Bearish", "Neutral"):
                sentiment = "Neutral"
            if imp_type   not in ("margin", "revenue", "sentiment", "regulatory", "operational"):
                imp_type  = "sentiment"
            if horizon    not in ("short-term", "medium-term", "long-term"):
                horizon   = "medium-term"

            results[idx] = {
                "tickers":     valid_tickers,
                "sentiment":   sentiment,
                "impact_type": imp_type,
                "time_horizon": horizon,
                "reasoning":   reasoning,
            }

    return results


def run_stage2(
    df: pd.DataFrame,
    supabase=None,
    dry_run: bool = False,
) -> tuple[pd.DataFrame, dict]:
    """
    Classifies Stage 1 output with Gemini.
    Writes SIGNAL rows to Supabase signals table (unless dry_run=True).
    Returns (signals_df, stats_dict).
    """
    from google import genai
    from google.genai import types

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY not set in .env")

    client = genai.Client(api_key=api_key)

    logger.info("STAGE 2 START — input: %d rows | model: %s", len(df), GEMINI_MODEL)

    signal_rows = []
    stats = {
        "total":    0,
        "signals":  0,
        "noise":    0,
        "errors":   0,
        "db_writes": 0,
    }

    markets = df.to_dict("records")

    for batch_start in range(0, len(markets), BATCH_SIZE):
        batch = markets[batch_start:batch_start + BATCH_SIZE]
        stats["total"] += len(batch)

        prompt = _build_batch_prompt(batch)

        # Call Gemini
        retry = 0
        results = None
        while retry < 3:
            try:
                response = client.models.generate_content(
                    model=GEMINI_MODEL,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        system_instruction=SYSTEM_PROMPT,
                        temperature=0.0,
                        max_output_tokens=1200,
                    ),
                )
                results = _parse_response(response.text, len(batch))
                break
            except Exception as e:
                if "429" in str(e):
                    wait = 30 * (retry + 1)
                    logger.warning("Rate limited — waiting %ds...", wait)
                    time.sleep(wait)
                    retry += 1
                else:
                    logger.error("Gemini error: %s", e)
                    stats["errors"] += len(batch)
                    results = [None] * len(batch)
                    break

        if results is None:
            results = [None] * len(batch)

        # Process results
        for market, result in zip(batch, results):
            if result is None:
                stats["noise"] += 1
                continue

            stats["signals"] += 1
            for ticker in result["tickers"]:
                row = {
                    # Market data
                    "market_id":    str(market.get("market_id", "")),
                    "event_id":     str(market.get("event_id", "")),
                    "event_title":  market.get("event_title", ""),
                    "question":     market.get("question", ""),
                    "tags":         market.get("tags", ""),
                    "yes_price":    float(market.get("yes_price") or 0),
                    "volume":       float(market.get("volume") or 0),
                    "end_date":     market.get("end_date") or None,
                    # LLM enrichment
                    "ticker":       ticker,
                    "sentiment":    result["sentiment"],
                    "impact_score": 5,  # default — report_generator will refine
                    "impact_type":  result["impact_type"],
                    "time_horizon": result["time_horizon"],
                    "reasoning":    result["reasoning"],
                    "model_used":   GEMINI_MODEL,
                }
                signal_rows.append(row)

                # Write to Supabase
                if supabase and not dry_run:
                    try:
                        supabase.table("signals").upsert(
                            row, on_conflict="market_id,ticker"
                        ).execute()
                        stats["db_writes"] += 1
                    except Exception as e:
                        logger.error("DB write failed (%s, %s): %s", market.get("market_id"), ticker, e)

                logger.info(
                    "  SIGNAL [%s] %s | %s | %s | %s",
                    result["sentiment"].upper(),
                    ticker,
                    result["impact_type"],
                    result["time_horizon"],
                    result["reasoning"][:80],
                )

        logger.info(
            "batch %d/%d — signals=%d noise=%d errors=%d",
            batch_start // BATCH_SIZE + 1,
            (len(markets) + BATCH_SIZE - 1) // BATCH_SIZE,
            stats["signals"], stats["noise"], stats["errors"],
        )
        time.sleep(BATCH_DELAY)

    signals_df = pd.DataFrame(signal_rows) if signal_rows else pd.DataFrame()

    logger.info(
        "STAGE 2 COMPLETE — signals=%d noise=%d errors=%d db_writes=%d",
        stats["signals"], stats["noise"], stats["errors"], stats["db_writes"],
    )
    return signals_df, stats