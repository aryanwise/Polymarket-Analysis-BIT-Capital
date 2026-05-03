"""
pipeline/report_generator.py

BIT Capital Daily Alpha Report Generator.

Aligned with new Stage 2 schema — signals table now contains:
  market_id, event_id, event_title, question, tags,
  yes_price, volume, end_date, ticker

Fields that Stage 2 no longer populates (now NULL):
  sentiment, impact_score, impact_type, time_horizon, reasoning

Design: the report LLM receives clean signal data (market + tickers)
and derives ALL reasoning, sentiment, and implications itself.
This produces higher quality output because the LLM reasons with
full portfolio context across all signals simultaneously — not one
market at a time.
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import time
import logging
from dotenv import load_dotenv
from datetime import datetime, timezone
from collections import defaultdict

load_dotenv()
from utils.supabase_client import get_service_client

supabase = get_service_client()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

GEMINI_MODEL = "gemini-2.0-flash"
GROQ_MODEL   = "llama-3.3-70b-versatile"


# ─────────────────────────────────────────────────────────────
# BIT CAPITAL FUND BRIEF
# ─────────────────────────────────────────────────────────────

FUND_BRIEF = """
BIT Capital — Berlin-based technology fund, ~€500M AUM.
Concentrated, high-conviction tech portfolio. Every holding has a thesis.
Mandate: 12-to-18 month holding period. We actively hedge tail risks.

CLUSTER 1 — CRYPTO INFRASTRUCTURE (25% of Fund)
  IREN  (8% weight):  Bitcoin miner pivoting to AI data centers.
                      Revenue = hashrate × BTC price. Break-even BTC ~$45-50k.
  COIN  (12% weight): Crypto exchange. Revenue = volumes × fee rate.
                      Regulatory risk is existential.
  HUT   (5% weight):  Diversified miner + AI compute. Less BTC-pure than IREN.

CLUSTER 2 — SEMICONDUCTORS (35% of Fund)
  NVDA  (15% weight): AI chips. ~20% revenue from China (H20). Export control risk.
  TSM   (12% weight): Foundry for all AI chips. Taiwan = existential operational risk.
  MU    (8% weight):  HBM memory for AI. Export control + Taiwan risk.

CLUSTER 3 — CLOUD / AI PLATFORMS (30% of Fund)
  MSFT  (12% weight): Azure + OpenAI commercial deal. Antitrust risk (DOJ).
  GOOGL (8% weight):  Search + Gemini AI. Antitrust ongoing.
  META  (5% weight):  Llama AI. Ad revenue = macro consumer proxy.
  AMZN  (3% weight):  AWS + Bedrock. Tariff risk on hardware.
  DDOG  (2% weight):  Cloud monitoring. Leading indicator of enterprise AI spend.

CLUSTER 4 — FINTECH / INSURTECH (10% of Fund)
  HOOD  (5% weight):  Interest income + crypto revenue. Rate-sensitive.
  RDDT  (3% weight):  AI data licensing + ads. AI regulation sensitivity.
  LMND  (2% weight):  Insurance float in bonds. Rate cuts compress income.

PORTFOLIO RISKS (priority order):
1. Taiwan/China military — existential for TSM (12%), supply shock for NVDA/MU
2. Bitcoin price — IREN near break-even at $45k BTC
3. US chip export controls — ~20% NVDA China revenue at risk
4. Fed rate path — affects our Fintech cluster directly
5. AI capex slowdown — DDOG is the canary in the coalmine
6. Crypto regulation — COIN business model risk
"""

# Cluster map for grouping signals
TICKER_CLUSTERS = {
    "IREN": "Crypto Infrastructure", "HUT":  "Crypto Infrastructure",
    "COIN": "Crypto Infrastructure",
    "NVDA": "Semiconductors",        "TSM":  "Semiconductors", "MU": "Semiconductors",
    "MSFT": "Cloud/AI Platforms",    "GOOGL":"Cloud/AI Platforms",
    "AMZN": "Cloud/AI Platforms",    "META": "Cloud/AI Platforms",
    "DDOG": "Cloud/AI Platforms",
    "HOOD": "Fintech/Insurtech",     "LMND": "Fintech/Insurtech",
    "RDDT": "Fintech/Insurtech",
}


# ─────────────────────────────────────────────────────────────
# SIGNAL PROCESSING — Python does quantitative work
# ─────────────────────────────────────────────────────────────

def prob_framing(yes: float) -> str:
    """Convert probability to market-implied language."""
    if yes >= 0.85: return f"{yes:.0%} — near-certain, already priced in"
    if yes >= 0.65: return f"{yes:.0%} — high conviction, market expects this"
    if yes >= 0.45: return f"{yes:.0%} — genuinely contested, maximum uncertainty"
    if yes >= 0.25: return f"{yes:.0%} — tail risk, material if it resolves YES"
    return         f"{yes:.0%} — unlikely, monitoring only"


def rank_signals(signals: list[dict]) -> list[dict]:
    """
    Rank signals by signal quality: uncertainty × volume.
    impact_score is now NULL (populated by report LLM, not stage 2).
    """
    def score(s):
        yes   = float(s.get("yes_price") or 0.5)
        vol   = min(float(s.get("volume") or 0) / 5_000_000, 1.0)
        uncert = 1 - abs(yes - 0.5) * 2
        return uncert * 0.65 + vol * 0.35

    return sorted(signals, key=score, reverse=True)


def deduplicate_by_market(signals: list[dict]) -> list[dict]:
    """
    Multiple tickers can map to the same market (e.g. BTC → IREN, HUT, COIN).
    For the signal brief, show each market once and list all its tickers.
    """
    market_map: dict[str, dict] = {}
    for s in signals:
        mid = s.get("market_id", "")
        if mid not in market_map:
            market_map[mid] = {**s, "tickers": [s.get("ticker", "")]}
        else:
            t = s.get("ticker", "")
            if t and t not in market_map[mid]["tickers"]:
                market_map[mid]["tickers"].append(t)
    return list(market_map.values())


def build_signal_brief(signals: list[dict]) -> tuple[str, list[str]]:
    """
    Builds the structured signal brief for the report LLM.

    Key design changes vs previous version:
    - No sentiment/reasoning/impact_type (those are NULL from stage 2)
    - Shows each unique MARKET once with all its mapped tickers
    - Groups by cluster
    - Probability framing done in Python
    - LLM derives all reasoning from this raw data
    """
    # Deduplicate: one entry per market with all tickers
    unique_markets = deduplicate_by_market(signals)
    unique_markets = rank_signals(unique_markets)

    all_tickers = sorted({s.get("ticker") for s in signals if s.get("ticker")})

    # Group by primary cluster (first ticker's cluster)
    clusters: dict[str, list[dict]] = defaultdict(list)
    for m in unique_markets:
        tickers = m.get("tickers", [m.get("ticker", "")])
        primary = tickers[0] if tickers else ""
        cluster = TICKER_CLUSTERS.get(primary, "Other")
        clusters[cluster].append(m)

    lines = []
    for cluster_name in ["Crypto Infrastructure", "Semiconductors",
                         "Cloud/AI Platforms", "Fintech/Insurtech", "Other"]:
        cluster_markets = clusters.get(cluster_name, [])
        if not cluster_markets:
            continue

        lines.append(f"\n{'─'*52}")
        lines.append(f"CLUSTER: {cluster_name.upper()}")
        lines.append(f"{'─'*52}")

        for m in cluster_markets:
            yes      = float(m.get("yes_price") or 0)
            vol      = float(m.get("volume") or 0)
            question = m.get("question", "")
            tickers  = m.get("tickers", [m.get("ticker", "")])
            end_date = (m.get("end_date") or "")[:10] or "—"
            ticker_str = ", ".join(tickers)

            lines.append(f"""
  Market:  {question}
  Tickers: {ticker_str}
  Prob:    {prob_framing(yes)}
  Volume:  ${vol:>12,.0f}   Expires: {end_date}""")

    return "\n".join(lines), all_tickers


def fetch_top_signals(limit: int = 30) -> list[dict]:
    """
    Fetch signals from signal_feed view.
    Ordered by volume (impact_score is NULL after stage 2 refactor).
    """
    try:
        res = (
            supabase.table("signal_feed")
            .select("*")
            .order("volume", desc=True)
            .limit(limit)
            .execute()
        )
        signals = res.data or []
        logger.info("Fetched %d signals from signal_feed", len(signals))
        return signals
    except Exception as e:
        logger.error("Failed to fetch signals: %s", e)
        return []


# ─────────────────────────────────────────────────────────────
# REPORT PROMPT
# ─────────────────────────────────────────────────────────────

SYSTEM_INSTRUCTION = """You are the Chief Investment Strategist at BIT Capital.

You write the Daily Alpha Report for senior portfolio managers who make buy/sell decisions.
They do not want summaries. They want your judgment.

Your style:
- Direct and precise. No hedging ("could", "may", "might").
- Cite specific probabilities: "markets price 67% chance of X — that implies Y."
- State the transmission mechanism explicitly: [event] → [what changes] → [P&L impact].
- Quantify where possible: % revenue at risk, basis points of margin, break-even levels.
- Make a call. Acknowledge uncertainty but state your view.

CRITICAL SYNTHESIS REQUIREMENT:
- Look for CONTRADICTORY or COMPOUNDING signals across different markets.
- Cross-Cluster Impact: If Market A implies higher energy costs (bad for IREN) but Market B implies lower rates (good for IREN borrowing), synthesize the net effect. 
- The Canary Effect: If signals imply a slowdown in enterprise cloud spending (DDOG), explicitly connect that to the revenue expectations for the AI Platforms (MSFT, GOOGL).
- Do not analyze signals in isolation. Connect the dots between macro policy, infrastructure costs, and tech fundamentals across the entire portfolio.

This is NOT:
- A list of signals.
- A "things to watch" memo.
- Vague market commentary.

This is an actionable investment document."""


def build_report_prompt(
    signal_brief: str,
    top_market:   dict,
    tickers:      list[str],
    date_str:     str,
) -> str:
    top_q   = top_market.get("question", "")
    top_t   = ", ".join(top_market.get("tickers", [top_market.get("ticker", "")]))
    top_yes = float(top_market.get("yes_price") or 0)

    return f"""Today is {date_str}.

━━ FUND BRIEF ━━
{FUND_BRIEF}

━━ POLYMARKET SIGNAL DATA ━━
These markets were classified as relevant to our portfolio by our signal pipeline.
Each entry shows: the market question, affected tickers, current probability, and volume.

{signal_brief}

━━ HIGHEST-PRIORITY SIGNAL ━━
Market:  {top_q}
Tickers: {top_t}
Prob:    {prob_framing(top_yes)}

━━ YOUR TASK ━━

For each signal, you must:
1. Determine the direction (Bullish/Bearish/Neutral) for each affected ticker
2. Identify the transmission mechanism: [event outcome] → [what changes] → [P&L impact]
3. Assess whether the current probability makes it actionable or just monitoring

Write the Daily Alpha Report in this exact structure:

---

# BIT Capital — Daily Alpha Report
**{date_str}**

---

## 1. Portfolio Risk Posture
One paragraph. What is the dominant macro theme from these signals?
Is the portfolio risk-on or risk-off? Which cluster faces the most exposure?

---

## 2. Signal of the Week
The highest-priority signal above. Why does it matter most?
State: probability, what happens to the stock if YES vs NO, and at what threshold it becomes actionable.
Include the transmission mechanism explicitly.

---

## 3. Most Interesting Markets Right Now
 
Pick exactly 3 markets from the signal data where the probability implies something
actionable — not the highest volume, but the ones with the sharpest edge.
 
Criteria for selection:
- The probability is at a level that creates asymmetric risk/reward for a specific holding
- There is a divergence between the Polymarket crowd and what equity consensus expects
- The YES or NO resolution would directly change a valuation driver, not just "sentiment"
 
For each market use EXACTLY this format, with each field on its own line:
 
**Market:** [the exact question text]
**Probability:** [X%] — [one sentence on what this probability implies in plain English]
**Why interesting:** [one sentence — be specific, name the holding and the mechanism]
**If YES:** [specific impact on named ticker — revenue %, margin bps, or break-even cross]
**If NO:** [specific alternative scenario for the same ticker]
**Edge:** [is Polymarket above or below equity consensus? State the gap if you see one. If no gap, say "consensus aligned — monitoring only"]
 
Do not use hedging language ("could", "may", "might", "potential").
State the direction and mechanism directly.
Each field must be on its own line — do not run them together in a paragraph.

---

## 4. Cluster Analysis

### Crypto Infrastructure — IREN, HUT, COIN
What do the signals collectively imply for this cluster?
Connect: BTC direction + Fed rate path + energy signals.
IREN breaks even at ~$45-50k BTC — what does the implied BTC direction mean for their margin?

### Semiconductors — NVDA, TSM, MU
Probability-weighted revenue at risk. NVDA ~20% China revenue. What do signals imply?
Taiwan/export control interaction.

### Cloud & AI Platforms — MSFT, GOOGL, AMZN, META, DDOG
Any divergence between names?
DDOG as leading indicator — what do signals say about enterprise AI capex?

### Fintech & Insurtech — HOOD, LMND, RDDT
Fed rate path implications. A 25bps cut changes HOOD's net interest margin by approximately X bps.
What do current probability-weighted scenarios imply?

---

## 5. Actionable Recommendations
Provide 3-5 specific, high-conviction recommendations based ONLY on the signals above.
You MUST format this as a Markdown table with the following exact columns:

| Ticker | Action (BUY/SELL/HEDGE/HOLD) | Catalyst (The Event) | Mechanism (Why it hits P&L) | Action Threshold (At what probability do we execute?) |
|---|---|---|---|---|
---

## 6. Risk Calendar
3 most important market expiry dates in the next 30 days.
For each: what happens to which holding if YES vs NO?

---
*BIT Capital Signal Scanner | {date_str} | Polymarket-powered*"""


# ─────────────────────────────────────────────────────────────
# LLM CALLS — Gemini primary, Groq fallback
# ─────────────────────────────────────────────────────────────

def call_gemini(prompt: str) -> str | None:
    try:
        from google import genai
        from google.genai import types
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            return None
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_INSTRUCTION,
                temperature=0.3,
                max_output_tokens=4000,
            ),
        )
        return response.text
    except Exception as e:
        logger.warning("Gemini failed: %s", e)
        return None


def call_groq(prompt: str) -> str | None:
    try:
        from groq import Groq
        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            return None
        client = Groq(api_key=api_key)
        delays = [1, 2, 4, 8, 16]
        for i, delay in enumerate(delays):
            try:
                resp = client.chat.completions.create(
                    model=GROQ_MODEL,
                    messages=[
                        {"role": "system", "content": SYSTEM_INSTRUCTION},
                        {"role": "user",   "content": prompt},
                    ],
                    temperature=0.3,
                    max_tokens=3500,
                )
                return resp.choices[0].message.content
            except Exception as e:
                if i == len(delays) - 1:
                    logger.error("Groq failed after retries: %s", e)
                    return None
                logger.warning("Groq retry %d: %s", i + 1, e)
                time.sleep(delay)
    except Exception as e:
        logger.warning("Groq unavailable: %s", e)
        return None


def generate_report(prompt: str) -> tuple[str | None, str]:
    logger.info("Trying Gemini %s...", GEMINI_MODEL)
    result = call_gemini(prompt)
    if result:
        logger.info("Report generated with Gemini")
        return result, GEMINI_MODEL

    logger.info("Falling back to Groq %s...", GROQ_MODEL)
    result = call_groq(prompt)
    if result:
        logger.info("Report generated with Groq")
        return result, GROQ_MODEL

    logger.error("Both Gemini and Groq failed")
    return None, "none"


# ─────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────

def run_report_pipeline(top_n: int = 30) -> dict | None:
    started_at = datetime.now(timezone.utc)
    date_str   = started_at.strftime("%B %d, %Y")

    logger.info("=" * 60)
    logger.info("REPORT GENERATOR START — %s", date_str)
    logger.info("=" * 60)

    # 1. Fetch signals
    signals = fetch_top_signals(limit=top_n)
    if not signals:
        logger.warning("No signals found. Run stage2_filter.py first.")
        return None

    # 2. Rank by signal quality (volume × uncertainty)
    ranked = rank_signals(signals)

    # 3. Build signal brief — deduplicated by market
    signal_brief, tickers = build_signal_brief(ranked)

    # 4. Top market for the "Signal of the Week" section
    unique_markets = deduplicate_by_market(ranked)
    top_market     = unique_markets[0] if unique_markets else ranked[0]

    logger.info("Unique markets in report : %d", len(unique_markets))
    logger.info("Tickers covered          : %s", ", ".join(tickers))

    # 5. Build prompt
    prompt = build_report_prompt(
        signal_brief=signal_brief,
        top_market=top_market,
        tickers=tickers,
        date_str=date_str,
    )

    # 6. Generate report
    report_content, model_used = generate_report(prompt)
    if not report_content:
        return None

    # 7. Save to DB
    try:
        row = {
            "content":      report_content,
            "tickers":      tickers,
            "signal_count": len(signals),
            "model_used":   model_used,
            "generated_at": started_at.isoformat(),
        }
        result    = supabase.table("reports").insert(row).execute()
        report_id = result.data[0]["id"]
        logger.info("Report saved — ID: %d | model: %s", report_id, model_used)
    except Exception as e:
        logger.error("Failed to save report: %s", e)
        return None

    # 8. Link signals to report
    linked = 0
    for s in signals:
        sid = s.get("signal_id")
        if not sid:
            continue
        try:
            supabase.table("report_signals").insert({
                "report_id": report_id,
                "signal_id": sid,
            }).execute()
            linked += 1
        except Exception:
            pass

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    logger.info("=" * 60)
    logger.info("REPORT COMPLETE — %.1fs", elapsed)
    logger.info("  Report ID   : %d", report_id)
    logger.info("  Model       : %s", model_used)
    logger.info("  Tickers     : %s", ", ".join(tickers))
    logger.info("  Signals     : %d linked", linked)
    logger.info("=" * 60)
    logger.info("\nPREVIEW:\n%s...\n", report_content[:500].replace("\n", " "))

    return {
        "report_id":      report_id,
        "tickers":        tickers,
        "signals_linked": linked,
        "model_used":     model_used,
        "duration_s":     round(elapsed, 1),
    }


if __name__ == "__main__":
    run_report_pipeline(top_n=30)