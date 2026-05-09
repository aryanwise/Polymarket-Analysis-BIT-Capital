"""
pipeline/stage2_filter.py

Stage 2: Two-stage LLM filtering.

Architecture:
  PASS 1 — Mistral (fast gate)
    Sees all markets from Stage 1.
    Binary decision: SIGNAL or NOISE.
    Prompt is short and strict — optimised for speed and low false positives.
    ~60-70% of markets rejected here at minimal cost.

  PASS 2 — Gemini (deep classifier, primary) → Groq (fallback)
    Sees only markets that passed Mistral.
    Maps exact tickers with transmission mechanism context.
    Operates on a much smaller set (~30-40 markets vs 100+).

Why this works:
  - Mistral is faster and cheaper per token than Gemini
  - Gemini's rate limits matter less when the input set is already filtered
  - Each model does one focused job, not multitasking
  - Recall is preserved (Mistral is permissive on edge cases)
  - Precision is enforced (Gemini is strict on ticker mapping)
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import re
import time
import logging
import pandas as pd
from dotenv import load_dotenv
import math

load_dotenv()

logger = logging.getLogger(__name__)

# ── Model config ──────────────────────────────────────────────
MISTRAL_MODEL = "mistral-small-latest"
GEMINI_MODEL  = "gemini-2.0-flash"
# GROQ_MODEL    = "llama-3.3-70b-versatile"
GROQ_MODEL = "meta-llama/llama-4-scout-17b-16e-instruct" # new model

GATE_BATCH_SIZE       = 20    # Mistral handles larger batches (simpler task)
CLASSIFIER_BATCH_SIZE = 10    # Gemini needs smaller batches (richer output)
BATCH_DELAY           = 2.0   # seconds between batches

# ── Holdings ──────────────────────────────────────────────────
HOLDINGS = {
    "IREN":  "Bitcoin miner. Revenue = BTC price × hashrate. Break-even ~$45-50k BTC.",
    "HUT":   "Crypto miner + AI compute. Same BTC/energy drivers as IREN.",
    "COIN":  "Crypto exchange. Revenue = trading volume × fee. Regulatory ban = existential.",
    "HOOD":  "Retail brokerage + crypto. Rate-sensitive interest income + crypto revenue.",
    "NVDA":  "AI chips. ~20% China revenue (H20). Export controls and Taiwan risk.",
    "TSM":   "Foundry for all AI chips. Taiwan conflict = operational shutdown.",
    "MU":    "HBM memory for AI. Export controls + Taiwan risk.",
    "MSFT":  "Azure + OpenAI partner. Antitrust (DOJ). Enterprise AI spend.",
    "GOOGL": "Search + Gemini AI. Antitrust (ad market). AI competition.",
    "AMZN":  "AWS + Bedrock AI. Import tariffs hit hardware costs.",
    "META":  "Digital ads + Llama AI. Ad revenue = macro consumer proxy.",
    "DDOG":  "Cloud monitoring. Leading indicator of enterprise AI/cloud spend.",
    "LMND":  "AI insurer. Float in bonds → rate cuts compress investment income.",
    "RDDT":  "AI data licensing + niche ads. AI regulation affects licensing rights.",
}

VALID_TICKERS = set(HOLDINGS.keys())


# ─────────────────────────────────────────────────────────────
# PASS 1 PROMPT — Mistral gate
# Goal: fast, high-recall binary filter
# Design: permissive on edge cases (false negative = losing a real signal)
#         strict on obvious noise (presidential travel, sports, EM macro)
# ─────────────────────────────────────────────────────────────

GATE_PROMPT = """You are a signal pre-screener for BIT Capital, a technology fund.

Your only job: decide if each Polymarket market is POTENTIALLY relevant to a tech fund
that holds Bitcoin miners, AI chip companies, crypto exchanges, cloud platforms,
and fintech/insurtech companies.

Be PERMISSIVE — if there is any reasonable chance it affects tech stocks, crypto,
semiconductors, AI regulation, or interest rates → mark SIGNAL.
Only mark NOISE if the market is clearly irrelevant: sports, entertainment,
celebrity, state-level elections, random token launches, or EM country data.

If unsure → SIGNAL. It is better to pass a weak market than to miss a real one.

CLEAR SIGNAL THEMES (mark SIGNAL):
- Bitcoin, Ethereum, crypto prices or regulation
- Federal Reserve, interest rates, monetary policy
- Taiwan, China military, semiconductor supply
- US chip export controls
- AI regulation, antitrust, major AI company events
- Tariffs on technology or semiconductors
- Iran, Strait of Hormuz, energy prices
- Cloud spending, enterprise AI budgets
- Stablecoin legislation

CLEAR NOISE (mark NOISE without hesitation):
- Sports results, entertainment, awards, celebrity
- State-level US elections or primaries
- EM country inflation/GDP (Argentina, Turkey, Korea etc)
- Presidential travel to US states
- Random DeFi token launches
- North/South Korea diplomacy
- "Nothing Ever Happens" novelty markets
- Venezuela, Middle East oil production volumes

OUTPUT — one line per market, nothing else:
[index] SIGNAL
[index] NOISE"""


# Sanitize float values before updatign to DB
def _safe_float(v, default: float = 0.0) -> float:
    try:
        if str(v).lower() in ("nan", "inf", "-inf", "none", "null", ""):
            return default
        f = float(v)
        return default if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return default



def _build_gate_prompt(markets: list[dict]) -> str:
    """Short prompt for Mistral — question + tags only, no volume."""
    lines = []
    for i, m in enumerate(markets):
        yes  = float(m.get("yes_price") or 0)
        tags = str(m.get("tags", ""))[:60]
        lines.append(
            f"[{i}] {m.get('question', '')}\n"
            f"    Tags: {tags} | YES: {yes:.0%}"
        )
    return "Classify each market:\n\n" + "\n\n".join(lines)


def _parse_gate_response(text: str, n: int) -> list[bool]:
    """
    Parses Mistral gate response.
    Returns list of bools: True = passes gate (SIGNAL), False = rejected (NOISE).
    Defaults to True on parse failure — permissive by design.
    """
    results = [True] * n   # default = pass (high recall)

    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r"\[(\d+)\]\s*(SIGNAL|NOISE)", line, re.I)
        if not m:
            continue
        idx   = int(m.group(1))
        label = m.group(2).upper()
        if idx < n:
            results[idx] = (label == "SIGNAL")

    passed = sum(results)
    logger.info("  Gate parsed: %d pass / %d reject from %d markets", passed, n - passed, n)
    return results


# ─────────────────────────────────────────────────────────────
# PASS 2 PROMPT — Gemini/Groq deep classifier
# Goal: precise ticker mapping on a pre-filtered set
# Design: strict on ticker selection, no over-mapping
# ─────────────────────────────────────────────────────────────

CLASSIFIER_PROMPT = """You are a strict portfolio signal analyst at BIT Capital, a Berlin-based technology fund.

Your only task: read each Polymarket prediction market and decide if its outcome
would DIRECTLY move the revenue, costs, or regulatory standing of a specific holding.

━━ THE ONLY QUESTION THAT MATTERS ━━
"If this market resolves YES, which specific holding gets a measurable financial impact?"

Before classifying SIGNAL, ask:
"Would an equity analyst include this in a 1-page earnings impact note?"
If NO → it is NOISE.

If you cannot name the exact P&L line that changes → it is NOISE.
When in doubt → NOISE.

━━ TRANSMISSION FILTER (STRICT) ━━
The impact must be ALL THREE:
  DIRECT   — 1 step, not 2-3 steps removed
  MATERIAL — affects earnings, margins, or valuation drivers measurably
  SPECIFIC — tied to a named holding's business model, not broad macro sentiment

━━ OUR 14 HOLDINGS ━━
""" + "\n".join(f"  {k}: {v}" for k, v in HOLDINGS.items()) + """

━━ DIRECT SIGNAL MAPPINGS ━━
  Bitcoin / crypto prices     → IREN, HUT, COIN, HOOD
  Fed interest rate decision  → IREN, HUT, LMND, HOOD
  US chip export controls     → NVDA, TSM, MU
  Taiwan / China military     → TSM, NVDA, MU
  AI antitrust ruling         → MSFT, GOOGL
  Tech / chip tariffs         → TSM, NVDA, AMZN
  Iran / Hormuz / energy      → IREN, HUT
  Crypto regulation / SEC     → COIN, HOOD
  Stablecoin legislation      → COIN, HOOD
  OpenAI IPO                  → MSFT, GOOGL
  Enterprise AI spend signals → DDOG, MSFT, AMZN, GOOGL

━━ MACRO-ONLY → ALWAYS NOISE ━━
GDP, unemployment, general recession probability, CPI/inflation without a
specific company cost mechanism → NOISE. These affect everything and therefore nothing.

━━ TICKER SELECTION RULES ━━
NEVER map more than 4 tickers unless the mechanism clearly affects all of them.
If you are listing 5+ tickers, you are over-generalising → reduce.

  Chip export controls: NVDA, TSM, MU — NOT MSFT, GOOGL, AMZN
  Fed rate cut: IREN, HUT, LMND, HOOD — NOT NVDA, MSFT, GOOGL

━━ HARD EDGE CASES ━━
  "Will Trump visit China?"         → NOISE (visit ≠ policy)
  "Will OpenAI release GPT-5?"      → NOISE (product launch ≠ revenue change)
  "Will AI bubble burst?"           → NOISE (macro narrative, no direct trigger)
  "Will US unemployment reach 5%?"  → NOISE (macro only)
  "Will BTC hit $150k?"             → SIGNAL: IREN,HUT,COIN
  "Will Fed cut 25bps?"             → SIGNAL: IREN,HUT,LMND,HOOD
  "Will US impose chip tariffs?"    → SIGNAL: NVDA,TSM

━━ OUTPUT FORMAT (STRICT) ━━
One line per market. No explanations. No commentary.

[index] SIGNAL: TICKER1,TICKER2
[index] NOISE"""


# ─────────────────────────────────────────────────────────────
# GROQ FALLBACK CLASSIFIER PROMPT
# Simpler, more direct version for Groq.
# The full CLASSIFIER_PROMPT is too strict for Groq — it defaults
# to NOISE on everything. This version is shorter and more permissive.
# ─────────────────────────────────────────────────────────────

GROQ_CLASSIFIER_PROMPT = """You are a portfolio analyst at BIT Capital, a tech fund.
Map each Polymarket market to the holdings it directly affects.

HOLDINGS (ticker: what moves it):
IREN/HUT  — Bitcoin price, energy costs, Fed rates
COIN      — Crypto prices, regulation, trading volume
HOOD      — Fed rates, crypto regulation, consumer trading
NVDA      — Chip export controls, Taiwan, AI capex
TSM       — Taiwan/China risk, chip demand
MU        — Chip export controls, Taiwan, AI memory
MSFT      — OpenAI partnership, cloud growth, antitrust
GOOGL     — Search, AI competition, antitrust
AMZN      — AWS, tariffs, AI capex
META      — Ad market, AI regulation
DDOG      — Enterprise cloud/AI spending
LMND      — Fed rates, bond yields
RDDT      — AI regulation, data licensing

Key signal patterns — mark these SIGNAL:
Bitcoin/crypto price → IREN,HUT,COIN,HOOD
Fed rates / Treasury yields → IREN,HUT,LMND,HOOD
Chip export controls / Taiwan → NVDA,TSM,MU
AI regulation / antitrust → MSFT,GOOGL,META,AMZN
Stablecoins → COIN,HOOD
Enterprise AI / cloud spend → DDOG,MSFT,AMZN,GOOGL
Tariffs on tech hardware → NVDA,TSM,AMZN
Inflation / bond yields → LMND,HOOD
Crypto regulation / SEC → COIN,HOOD
Major AI company IPO → MSFT,GOOGL

Mark NOISE only if clearly irrelevant: sports, entertainment,
state elections, EM politics, medical/pharma, general travel.

OUTPUT — one line per market, nothing else:
[index] SIGNAL: TICKER1,TICKER2
[index] NOISE"""


def _build_classifier_prompt(markets: list[dict]) -> str:
    """Richer prompt for Gemini — includes volume as signal quality indicator."""
    lines = []
    for i, m in enumerate(markets):
        yes  = float(m.get("yes_price") or 0)
        vol  = float(m.get("volume") or 0)
        tags = str(m.get("tags", ""))[:80]
        lines.append(
            f"[{i}] {m.get('question', '')}\n"
            f"    Tags: {tags} | YES: {yes:.0%} | Vol: ${vol:,.0f}"
        )
    return "Classify each market:\n\n" + "\n\n".join(lines)


def _parse_classifier_response(text: str, n: int) -> list[list[str] | None]:
    """
    Parses Gemini/Groq response into ticker lists or None.
    Strict parsing — invalid tickers are dropped.
    """
    results = [None] * n

    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue

        m = re.match(r"\[(\d+)\]\s*(SIGNAL|NOISE)[:\s]*(.*)", line, re.I)
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

        raw_tickers = [t.strip().upper() for t in rest.split(",") if t.strip()]
        valid       = [t for t in raw_tickers if t in VALID_TICKERS]
        results[idx] = valid if valid else None

    found = sum(1 for r in results if r is not None)
    logger.info("  Classified: %d signals, %d noise from %d markets", found, n - found, n)
    return results


# ─────────────────────────────────────────────────────────────
# LLM CALLERS
# ─────────────────────────────────────────────────────────────

def _call_mistral(prompt: str, system: str) -> str | None:
    try:
        from mistralai.client import Mistral
        api_key = os.environ.get("MISTRAL_API_KEY")
        if not api_key:
            return None
        client   = Mistral(api_key=api_key)
        response = client.chat.complete(
            model=MISTRAL_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.0,
            max_tokens=300,   # gate output is very short
        )
        return response.choices[0].message.content
    except Exception as e:
        logger.warning("Mistral error: %s", e)
        return None


def _call_gemini(prompt: str, system: str) -> str | None:
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
                system_instruction=system,
                temperature=0.0,
                max_output_tokens=500,
            ),
        )
        return response.text
    except Exception as e:
        if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
            logger.warning("Gemini rate limited — waiting 20s")
            time.sleep(20)
        else:
            logger.warning("Gemini error: %s", e)
        return None


def _call_groq(prompt: str, system: str) -> str | None:
    try:
        from groq import Groq
        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            return None
        client   = Groq(api_key=api_key)
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {"role": "system", "content": system},
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

def _call_openrouter(prompt: str, system: str) -> str | None:
    try:
        import requests
        headers = {
            "Authorization": f"Bearer {os.environ.get('OPENROUTER_API_KEY','')}",
            "Content-Type": "application/json"
        }
        body = {
            "model": "qwen/qwen3-235b-a22b:free",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user",   "content": prompt}
            ],
            "temperature": 0.0,
            "max_tokens": 500,
        }
        resp = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers=headers, json=body, timeout=30
        )
        return resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        logger.warning("OpenRouter failed: %s", e)
        return None

# ─────────────────────────────────────────────────────────────
# PASS 1 — Mistral gate
# ─────────────────────────────────────────────────────────────

def run_gate(markets: list[dict]) -> list[dict]:
    """
    Mistral fast gate: filters markets to only those potentially relevant.
    Permissive by design — prefers false positives over false negatives.
    Returns list of markets that passed.
    """
    logger.info("─" * 55)
    logger.info("PASS 1 — Mistral gate | %d markets | batch=%d",
                len(markets), GATE_BATCH_SIZE)

    passed = []
    total_rejected = 0

    for batch_start in range(0, len(markets), GATE_BATCH_SIZE):
        batch     = markets[batch_start:batch_start + GATE_BATCH_SIZE]
        batch_num = batch_start // GATE_BATCH_SIZE + 1

        prompt = _build_gate_prompt(batch)
        raw    = _call_mistral(prompt, GATE_PROMPT)

        if raw is None:
            # Mistral unavailable — pass all (fail open, not closed)
            logger.warning("Mistral unavailable — passing all %d markets in batch", len(batch))
            passed.extend(batch)
            time.sleep(BATCH_DELAY)
            continue

        gates = _parse_gate_response(raw, len(batch))

        for market, passes in zip(batch, gates):
            if passes:
                passed.append(market)
            else:
                total_rejected += 1

        logger.info("Gate batch %d — passed: %d / %d",
                    batch_num, sum(gates), len(batch))
        time.sleep(BATCH_DELAY)

    logger.info("PASS 1 COMPLETE — passed: %d / %d (rejected: %d)",
                len(passed), len(markets), total_rejected)
    return passed


# ─────────────────────────────────────────────────────────────
# PASS 2 — Gemini/Groq deep classifier
# ─────────────────────────────────────────────────────────────

def run_classifier(
    markets:  list[dict],
    supabase  = None,
    dry_run:  bool = False,
) -> tuple[list[dict], dict]:
    """
    Gemini deep classifier: maps tickers precisely on pre-filtered markets.
    Falls back to Groq if Gemini fails.
    """
    logger.info("─" * 55)
    logger.info("PASS 2 — Gemini classifier | %d markets | batch=%d",
                len(markets), CLASSIFIER_BATCH_SIZE)

    signal_rows = []
    stats = {"total": 0, "signals": 0, "noise": 0, "errors": 0, "db_writes": 0}

    for batch_start in range(0, len(markets), CLASSIFIER_BATCH_SIZE):
        batch     = markets[batch_start:batch_start + CLASSIFIER_BATCH_SIZE]
        batch_num = batch_start // CLASSIFIER_BATCH_SIZE + 1
        total_batches = (len(markets) + CLASSIFIER_BATCH_SIZE - 1) // CLASSIFIER_BATCH_SIZE

        stats["total"] += len(batch)
        logger.info("Classifier batch %d/%d (%d markets)...",
                    batch_num, total_batches, len(batch))

        prompt = _build_classifier_prompt(batch)

        # Gemini primary
        """
        raw = _call_gemini(prompt, CLASSIFIER_PROMPT)
        if raw:
            model_used = GEMINI_MODEL
        else:
            # Groq fallback — uses simpler prompt, Groq struggles with the full one
            groq_prompt = _build_classifier_prompt(batch)
            raw = _call_groq(groq_prompt, GROQ_CLASSIFIER_PROMPT)
            model_used = GROQ_MODEL if raw else "none"
        """
        groq_prompt = _build_classifier_prompt(batch)
        raw = _call_groq(groq_prompt, GROQ_CLASSIFIER_PROMPT)
        model_used = GROQ_MODEL if raw else "none"


        if raw is None:
            logger.error("Batch %d — both Gemini and Groq failed", batch_num)
            stats["errors"] += len(batch)
            time.sleep(BATCH_DELAY)
            continue

        logger.info("  Model: %s", model_used)
        results = _parse_classifier_response(raw, len(batch))

        for market, tickers in zip(batch, results):
            if not tickers:
                stats["noise"] += 1
                continue

            stats["signals"] += 1
            for ticker in tickers:
                row = {
                    "market_id":   str(market.get("market_id", "")),
                    "event_id":    str(market.get("event_id", "")),
                    "event_title": market.get("event_title", ""),
                    "question":    market.get("question", ""),
                    "tags":        market.get("tags", ""),
                    "yes_price": _safe_float(market.get("yes_price")),
                    "volume":    _safe_float(market.get("volume")),
                    "end_date":    market.get("end_date") or None,
                    "ticker":      ticker,
                    "model_used":  model_used,
                }
                signal_rows.append(row)
                logger.info("  ✓ SIGNAL  [%s] %s",
                            ticker, market.get("question","")[:55])

                if supabase and not dry_run:
                    try:
                        supabase.table("signals").upsert(
                            row, on_conflict="market_id,ticker"
                        ).execute()
                        stats["db_writes"] += 1
                    except Exception as e:
                        logger.error("DB write failed (%s, %s): %s",
                                     market.get("market_id"), ticker, e)

        time.sleep(BATCH_DELAY)

    return signal_rows, stats


# ─────────────────────────────────────────────────────────────
# PASS B — Groq enrichment
# Input:  signal rows from Pass A (question + ticker + yes_price)
# Output: sentiment, impact_score (1-10), reasoning (one line)
# Model:  Groq only — cheap, fast, structured output
# ─────────────────────────────────────────────────────────────

ENRICHMENT_PROMPT = """You are a portfolio analyst at BIT Capital.

For each signal, output exactly 3 fields on one line, tab-separated:
  [index]  SENTIMENT  SCORE  REASONING

Rules:
  SENTIMENT : Bullish, Bearish, or Neutral — for the named ticker only
              Bullish = YES outcome increases revenue/valuation for that ticker
              Bearish = YES outcome decreases revenue/valuation for that ticker
              Neutral = contested or unclear direction

  SCORE     : Integer 1-10 reflecting how directly and materially this market
              affects the ticker's near-term earnings
              9-10 = existential or >10% revenue impact (e.g. Coinbase license revoked)
              7-8  = direct material impact, changes a key earnings line
              5-6  = relevant but modest (indirect cost or secondary effect)
              3-4  = weak connection, monitoring only
              1-2  = very indirect, tail risk only

  REASONING : One sentence. Name the exact P&L mechanism.
              "Fed cut at 72% YES removes ~$2M quarterly from LMND bond income."
              No hedging. No "could" or "may". State the impact directly.

Output format — one line per signal, nothing else:
[index]	Bullish	7	One sentence reasoning here.

Example output:
[0]	Bearish	8	Export controls on H20 chips remove ~$15B of NVDA China revenue directly.
[1]	Bullish	6	Rate cut at 72% YES compresses LMND bond float income by ~150bps.
[2]	Neutral	4	BTC tail risk at 12% YES — IREN mining revenue upside if resolved, monitoring only."""


def _build_enrichment_prompt(signal_rows: list[dict]) -> str:
    lines = []
    for i, s in enumerate(signal_rows):
        yes    = _safe_float(s.get("yes_price"))
        vol    = _safe_float(s.get("volume"))
        ticker = s.get("ticker", "")
        lines.append(
            f"[{i}] Ticker: {ticker} | YES: {yes:.0%} | Vol: ${vol:,.0f}\n"
            f"    Question: {s.get('question','')}"
        )
    return "Enrich each signal:\n\n" + "\n\n".join(lines)


def _parse_enrichment_response(
    text: str, n: int
) -> list[dict]:
    """
    Parse Groq enrichment output into list of dicts.
    Each dict has: sentiment, impact_score, reasoning
    Defaults to safe values on parse failure.
    """
    results = [
        {"sentiment": "Neutral", "impact_score": 5, "reasoning": ""}
        for _ in range(n)
    ]

    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue

        # Match: [index]\tSentiment\tScore\tReasoning
        m = re.match(r"\[(\d+)\]\t([A-Za-z]+)\t(\d+)\t(.*)", line)
        if not m:
            # Try looser match with spaces
            m = re.match(r"\[(\d+)\]\s+(Bullish|Bearish|Neutral)\s+(\d+)\s+(.*)", line, re.I)
        if not m:
            continue

        idx       = int(m.group(1))
        sentiment = m.group(2).capitalize()
        score     = max(1, min(10, int(m.group(3))))
        reasoning = m.group(4).strip()

        if idx >= n:
            continue
        if sentiment not in ("Bullish", "Bearish", "Neutral"):
            sentiment = "Neutral"

        results[idx] = {
            "sentiment":    sentiment,
            "impact_score": score,
            "reasoning":    reasoning,
        }

    parsed = sum(1 for r in results if r["reasoning"])
    logger.info("  Enrichment parsed: %d / %d signals", parsed, n)
    return results


ENRICH_BATCH_SIZE = 15   # Groq handles more per call for this simpler task


def run_enrichment(
    signal_rows: list[dict],
    supabase     = None,
    dry_run:     bool = False,
) -> list[dict]:
    """
    Pass B: enriches classified signals with sentiment, impact_score, reasoning.
    Runs after Pass A (classification) on SIGNAL rows only.
    Uses Groq — cheap and fast for structured output.
    Updates signal_rows in-place and writes to DB if supabase provided.
    """
    if not signal_rows:
        return signal_rows

    logger.info("─" * 55)
    logger.info("PASS B — Enrichment | %d signals | batch=%d",
                len(signal_rows), ENRICH_BATCH_SIZE)

    all_enriched = []

    for batch_start in range(0, len(signal_rows), ENRICH_BATCH_SIZE):
        batch     = signal_rows[batch_start:batch_start + ENRICH_BATCH_SIZE]
        batch_num = batch_start // ENRICH_BATCH_SIZE + 1
        total_b   = (len(signal_rows) + ENRICH_BATCH_SIZE - 1) // ENRICH_BATCH_SIZE

        logger.info("Enrichment batch %d/%d (%d signals)...",
                    batch_num, total_b, len(batch))

        prompt  = _build_enrichment_prompt(batch)
        raw     = _call_groq(prompt, ENRICHMENT_PROMPT)

        if raw is None:
            logger.warning("Groq enrichment failed for batch %d — using defaults", batch_num)
            enriched = [
                {"sentiment": "Neutral", "impact_score": 5, "reasoning": ""}
                for _ in batch
            ]
        else:
            enriched = _parse_enrichment_response(raw, len(batch))

        # Merge enrichment into signal rows + write to DB
        for row, enr in zip(batch, enriched):
            row["sentiment"]    = enr["sentiment"]
            row["impact_score"] = enr["impact_score"]
            row["reasoning"]    = enr["reasoning"]
            all_enriched.append(row)

            logger.info(
                "  [%s] %s score=%d — %s",
                row["ticker"], enr["sentiment"], enr["impact_score"],
                enr["reasoning"][:60]
            )

            # Update the DB row that Pass A already wrote
            if supabase and not dry_run and enr["reasoning"]:
                try:
                    supabase.table("signals").update({
                        "sentiment":    enr["sentiment"],
                        "impact_score": enr["impact_score"],
                        "reasoning":    enr["reasoning"],
                    }).eq("market_id", row["market_id"]
                    ).eq("ticker",     row["ticker"]
                    ).execute()
                except Exception as e:
                    logger.error("Enrichment DB update failed (%s, %s): %s",
                                 row.get("market_id"), row.get("ticker"), e)

        time.sleep(BATCH_DELAY)

    logger.info("PASS B COMPLETE — enriched %d signals", len(all_enriched))
    return all_enriched

def run_stage2(
    df:       pd.DataFrame,
    supabase  = None,
    dry_run:  bool = False,
) -> tuple[pd.DataFrame, dict]:
    """
    Three-pass LLM pipeline within Stage 2:

      Pass 1 (Mistral gate):    ~105 markets → ~35-40 pass
        Fast binary filter. Permissive. Rejects obvious noise cheaply.

      Pass 2 (Gemini/Groq):     ~35-40 markets → signal rows with tickers
        Strict classification + ticker mapping.
        Writes rows to signals table (enrichment fields still NULL).

      Pass B (Groq enrichment): signal rows → sentiment + score + reasoning
        Runs on SIGNAL rows only (~57 rows vs 105 markets).
        Fills sentiment, impact_score, reasoning in same pipeline run.
        No NULL columns in DB after this point.
    """
    logger.info("=" * 55)
    logger.info("STAGE 2 START — %d markets", len(df))
    logger.info("  Pass 1: Mistral gate    (fast, binary, permissive)")
    logger.info("  Pass 2: Gemini/Groq     (strict, ticker mapping)")
    logger.info("  Pass B: Groq enrichment (sentiment, score, reasoning)")
    logger.info("=" * 55)

    markets = df.to_dict("records")

    # Pass 1 — Mistral gate
    passed_markets = run_gate(markets)

    if not passed_markets:
        logger.info("Gate rejected all markets — no signals.")
        return pd.DataFrame(), {
            "total": len(markets), "gate_passed": 0,
            "signals": 0, "noise": 0, "errors": 0, "db_writes": 0
        }

    # Pass 2 — Gemini/Groq classifier (writes to DB, enrichment fields NULL)
    signal_rows, stats = run_classifier(passed_markets, supabase, dry_run)
    stats["gate_passed"]   = len(passed_markets)
    stats["gate_rejected"] = len(markets) - len(passed_markets)

    # Pass B — Groq enrichment (fills sentiment, impact_score, reasoning)
    if signal_rows:
        signal_rows = run_enrichment(signal_rows, supabase, dry_run)

    signals_df = pd.DataFrame(signal_rows) if signal_rows else pd.DataFrame()

    logger.info("=" * 55)
    logger.info("STAGE 2 COMPLETE")
    logger.info("  Input markets   : %d", len(markets))
    logger.info("  Gate passed     : %d  (Mistral)", len(passed_markets))
    logger.info("  Gate rejected   : %d  (Mistral)", len(markets) - len(passed_markets))
    logger.info("  Signals found   : %d  (Gemini/Groq)", stats["signals"])
    logger.info("  Noise rejected  : %d  (Gemini/Groq)", stats["noise"])
    logger.info("  Enriched        : %d  (Groq Pass B)", len(signal_rows))
    logger.info("  DB writes       : %d", stats["db_writes"])
    logger.info("=" * 55)

    return signals_df, stats