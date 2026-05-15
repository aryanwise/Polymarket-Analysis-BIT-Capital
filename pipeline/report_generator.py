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
import pytz
berlin = pytz.timezone("Europe/Berlin")
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

GEMINI_MODEL = "gemini-2.5-flash"
# GROQ_MODEL   = "llama-3.3-70b-versatile"
GROQ_MODEL = "meta-llama/llama-4-scout-17b-16e-instruct" # new model


# ─────────────────────────────────────────────────────────────
# BIT CAPITAL FUND BRIEF
# ─────────────────────────────────────────────────────────────

FUND_BRIEF = """
BIT Capital — Berlin-based technology fund, ~€500M AUM.
Concentrated, high-conviction tech portfolio. Every holding has a thesis.

CLUSTER 1 — CRYPTO INFRASTRUCTURE
  IREN  (IREN Limited):  Bitcoin miner pivoting to AI data centers.
                         Revenue = hashrate × BTC price. Break-even BTC ~$45-50k.
  HUT   (Hut 8 Corp.):  Diversified miner + AI compute. Less BTC-pure than IREN.
  COIN  (Coinbase):      Crypto exchange. Revenue = volumes × fee rate.
                         Regulatory risk is existential.

CLUSTER 2 — SEMICONDUCTORS
  NVDA  (NVIDIA):   AI chips. ~20% revenue from China (H20). Export control risk.
  TSM   (TSMC):     Foundry for all AI chips. Taiwan = existential operational risk.
  MU    (Micron):   HBM memory for AI. Export control + Taiwan risk.

CLUSTER 3 — CLOUD / AI PLATFORMS
  MSFT  (Microsoft):  Azure + OpenAI commercial deal. Antitrust risk (DOJ).
  GOOGL (Alphabet):   Search + Gemini AI. Antitrust ongoing.
  AMZN  (Amazon):     AWS + Bedrock. Tariff risk on hardware.
  META  (Meta):       Llama AI. Ad revenue = macro consumer proxy.
  DDOG  (Datadog):    Cloud monitoring. Leading indicator of enterprise AI spend.

CLUSTER 4 — FINTECH / INSURTECH
  HOOD  (Robinhood):   Interest income + crypto revenue. Rate-sensitive.
  LMND  (Lemonade):    Insurance float in bonds. Rate cuts compress income.
  RDDT  (Reddit):      AI data licensing + ads. AI regulation sensitivity.

PORTFOLIO RISKS (priority order):
1. Bitcoin price — IREN near break-even at $45k BTC
2. Fed rate path — affects IREN, HUT, LMND, HOOD directly
3. Taiwan/China military — existential for TSM, supply shock for NVDA/MU
4. US chip export controls — ~20% NVDA China revenue at risk
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

def build_crowd_vs_reality(signals: list[dict], max_signals: int = 3) -> str:
    """
    For top N signals by impact_score, fetch news and calculate divergence.
    Returns formatted section string to inject into report prompt.
    Only runs on signals with impact_score >= 7 to keep runtime reasonable.
    """
    try:
        from dig_deeper_analysis import get_news_divergence, calculate_divergence
    except ImportError:
        logger.warning("dig_deeper not available — skipping Crowd vs Reality")
        return ""

    # Filter to high-score signals only, deduplicated by market
    unique = deduplicate_by_market(signals)
    high_score = [
        m for m in unique
        if float(m.get("impact_score") or 0) >= 7
    ][:max_signals]

    if not high_score:
        return ""

    logger.info("Running Crowd vs Reality for %d signals...", len(high_score))

    sections = []
    for market in high_score:
        result = get_news_divergence(market)
        if not result:
            continue

        # Override with rule-based check
        divergence = calculate_divergence(
            result["yes_price"],
            result["news_sentiment"],
            result["news_confidence"],
        )
        result["divergence"] = divergence

        yes_pct    = f"{result['yes_price']:.0%}"
        sentiment  = result["news_sentiment"]
        confidence = result["news_confidence"]
        div        = result["divergence"]
        explanation = result["explanation"]
        ticker     = result["ticker"]
        question   = result["question"]

        # Divergence label with emoji for dashboard readability
        div_label = {
            "HIGH":   "🔴 HIGH — information gap detected",
            "MEDIUM": "🟡 MEDIUM — uncertain signal",
            "LOW":    "🟢 LOW — crowd and news aligned",
            "NONE":   "⚪ NONE — fully aligned",
        }.get(div, div)

        # Alpha implication based on divergence direction
        poly_bullish = result["yes_price"] >= 0.55
        if div == "HIGH" and poly_bullish and sentiment == "Bearish":
            alpha = f"Polymarket is pricing {yes_pct} YES but news consensus is Bearish at {confidence}% confidence — Polymarket sees a hidden catalyst. Watch {ticker} for asymmetric upside if the crowd is right."
        elif div == "HIGH" and not poly_bullish and sentiment == "Bullish":
            alpha = f"News is Bullish at {confidence}% confidence but Polymarket prices only {yes_pct} — market underpricing positive catalyst. {ticker} is a contrarian entry if news is leading."
        else:
            alpha = f"{ticker} crowd and news are broadly aligned at {yes_pct} YES — no information gap. Monitor for divergence."

        sections.append(f"""**Signal:** {question}
**Polymarket:** {yes_pct} YES — crowd pricing
**News consensus:** {sentiment} ({confidence}% confidence across {len(result['source_urls'])} sources)
**Divergence:** {div_label}
**What it means:** {explanation}
**Alpha implication:** {alpha}""")

    if not sections:
        return ""

    header = "## 3.5 Crowd vs Reality\n\nFor high-priority signals, we compare Polymarket crowd pricing against live news consensus to identify information gaps where the crowd sees something the equity market hasn't priced yet.\n\n---\n\n"
    return header + "\n\n---\n\n".join(sections)


def fetch_top_signals(limit: int = 30) -> list[dict]:
    """
    Fetch signals from signal_feed view.
    Ordered by impact_score DESC — signals enriched by Pass B in stage2_filter.
    """
    try:
        res = (
            supabase.table("signal_feed")
            .select("*")
            .order("impact_score", desc=True)
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

SYSTEM_INSTRUCTION = """CRITICAL LANGUAGE RULE — READ FIRST:
NEVER use these words: "could", "may", "might", "potential", "significant", "likely", "possibly".
If you use any of these words, the report is a failure. Rewrite using: "will", "implies", "results in", "is", "increases", "decreases", "adds", "removes".

You are the Chief Investment Strategist at BIT Capital.

You write the Daily Alpha Report for senior portfolio managers who make buy/sell decisions.
They do not want summaries. They want your judgment.

Your style:
- Direct and precise. NEVER use: "could", "may", "might", "potential", "significant impact".
- Cite specific probabilities: "markets price 67% chance of X — that implies Y."
- State the transmission mechanism explicitly: [event] → [what changes] → [P&L impact].
- Quantify where possible: % revenue at risk, basis points of margin, break-even levels.
- Make a call. Acknowledge uncertainty but state your view.
- Get transmission direction right: a competitor winning = bearish for incumbents, not bullish.
- DATA-FIRST: When discussing any ticker, cite its current price and 5D change from the LIVE PORTFOLIO PRICES section. Never discuss a stock's direction without referencing its actual price movement.

MANDATORY FORMATTING:
- Every section starts with ## and a --- divider above it
- Section 3 fields are always on separate lines with **bold** labels
- Section 5 uses a Markdown table
- Never merge multiple fields onto one line

This is an actionable investment document."""


def build_report_prompt(
    signal_brief:  str,
    top_market:    dict,
    tickers:       list[str],
    date_str:      str,
    price_context: str = "",
) -> str:
    top_q   = top_market.get("question", "")
    top_t   = ", ".join(top_market.get("tickers", [top_market.get("ticker", "")]))
    top_yes = float(top_market.get("yes_price") or 0)

    price_section = ""
    if price_context:
        price_section = f"""
━━ LIVE PORTFOLIO PRICES ━━
Use this to assess whether signals are already priced in or still actionable.
A stock down 15% this week with a bearish signal = may already be priced in.
A stock near 52-week lows with a bullish signal = potential entry point.

{price_context}
"""

    return f"""Today is {date_str}.

━━ FUND BRIEF ━━
{FUND_BRIEF}
{price_section}
━━ POLYMARKET SIGNAL DATA ━━
These markets were classified as relevant to our portfolio by our signal pipeline.
Each entry shows: the market question, affected tickers, current probability, and volume.

{signal_brief}

━━ HIGHEST-PRIORITY SIGNAL ━━
Market:  {top_q}
Tickers: {top_t}
Prob:    {prob_framing(top_yes)}

━━ YOUR TASK ━━

Portfolio tickers in scope: {", ".join(tickers)}

For each signal, you must:
1. Determine the direction (Bullish/Bearish/Neutral) for each affected ticker
2. Identify the transmission mechanism: [event outcome] → [what changes] → [P&L impact]
3. Cross-reference current price action — is this already priced in or still actionable?

CRITICAL: Only reference markets that appear in the POLYMARKET SIGNAL DATA 
section above. Do not invent signals, probabilities, or portfolio risks that 
are not backed by an actual market listed above. If a cluster has no signals, 
say "No active signals for this cluster this run."

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

**Market:** [exact question from the highest-priority signal]
**Probability:** [X%] — [one sentence on what this implies]
**Price context:** [ticker] is at $[price] ([5D change]% over 5 days)
**If YES:** [specific impact — name the ticker, give a number]
**If NO:** [specific alternative for same ticker]
**Transmission mechanism:** [event] → [what changes] → [P&L impact]
**Actionable at:** [what probability threshold makes this a trade]

---

## 3. Most Interesting Markets Right Now

Pick exactly 3 markets from the POLYMARKET SIGNAL DATA section above.
Only use markets that appear in that section — do not invent signals.

Criteria: pick markets where probability creates asymmetric risk/reward,
or where Polymarket diverges from equity consensus.

HARD RULE: Never pick a market with probability >85% or <15%.
CHECK YOURSELF: Before writing each market, state its probability.
If it is above 85%, STOP and pick a different market.

Use EXACTLY this format. Each field on its own line. Blank line between markets.
No deviations. Copy the field labels exactly as written below.

--- FORMAT EXAMPLE (fictional — do not copy this market into your output) ---
**Market:** Will flying cars be legal in the US by 2027?
**Probability:** 12% — tail risk, market does not expect this
**Why interesting:** FICTIONAL EXAMPLE — replace with a real market from the signal data above
**If YES:** FICTIONAL
**If NO:** FICTIONAL
**Edge:** FICTIONAL
--- END EXAMPLE ---

**Market:** [first market question — from signal data above]
**Probability:** [X%] — [one sentence]
**Why interesting:** [one sentence, name ticker and mechanism]
**If YES:** [specific impact — revenue %, margin bps, or named dollar figure]
**If NO:** [specific alternative for same ticker]
**Edge:** [Polymarket vs consensus gap, or "consensus aligned — monitoring only"]

**Market:** [second market question]
**Probability:** [X%] — [one sentence]
**Why interesting:** [one sentence, name ticker and mechanism]
**If YES:** [specific impact — revenue %, margin bps, or named dollar figure]
**If NO:** [specific alternative for same ticker]
**Edge:** [Polymarket vs consensus gap, or "consensus aligned — monitoring only"]

**Market:** [third market question]
**Probability:** [X%] — [one sentence]
**Why interesting:** [one sentence, name ticker and mechanism]
**If YES:** [specific impact]
**If NO:** [specific alternative]
**Edge:** [gap or consensus aligned]

Rules:
- No hedging language (no "could", "may", "might", "potential")
- State impacts as specific numbers, not vague directions
- Each field must be on its own line, never inline in a paragraph

---

## 4. Cluster Analysis

### Crypto Infrastructure — IREN, HUT, COIN
What do the signals collectively imply for this cluster?
Connect: BTC direction + Fed rate path + energy signals.
IREN breaks even at ~$45-50k BTC — what does the implied BTC direction mean for their margin?

### Semiconductors — NVDA, TSM, MU
Check the signal data above. If no chip export / Taiwan / AI capex signals appear,
write exactly: "No active Polymarket signals this run — monitoring only."
Only write analysis if a relevant market appears in the signal data above.

### Cloud & AI Platforms — MSFT, GOOGL, AMZN, META, DDOG
Any divergence between names?
DDOG as leading indicator — what do signals say about enterprise AI capex?

### Fintech & Insurtech — HOOD, LMND, RDDT
Fed rate path implications. A 25bps cut changes HOOD's net interest margin by approximately X bps.
What do current probability-weighted scenarios imply?

---

## 5. Actionable Recommendations

Only include tickers where probability is in the 25-75% range — outside that range is either priced in or tail risk only.

| Ticker | Action | Probability | Price (5D) | Reasoning |
|:-------|:-------|:------------|:-----------|:----------|
| **[TICKER]** | **[BUY/SELL/HOLD/ADD/REDUCE/HEDGE]** | [X%] | $[price] ([5D]%) | [one sentence — transmission mechanism and number] |

Repeat for 3-5 tickers maximum.

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
                temperature=0.1,
                max_output_tokens=8192,
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
                    temperature=0.1,
                    max_tokens=4096,
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
    # Try Gemini 2.5 Flash first
    logger.info("Trying Gemini %s...", GEMINI_MODEL)
    result = call_gemini(prompt)
    if result:
        logger.info("Report generated with Gemini")
        return result, GEMINI_MODEL

    # Groq fallback
    logger.info("Falling back to Groq %s...", GROQ_MODEL)
    result = call_groq(prompt)
    if result:
        logger.info("Report generated with Groq")
        return result, GROQ_MODEL

    logger.error("Both failed")
    return None, "none"

"""
def generate_report(prompt: str) -> tuple[str | None, str]:
    '''Using Groq only'''
    logger.info("Generating report with Groq %s...", GROQ_MODEL)
    result = call_groq(prompt)
    if result:
        logger.info("Report generated with Groq")
        return result, GROQ_MODEL

    logger.error("Groq failed")
    return None, "none"
"""

# ─────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────

def run_report_pipeline(top_n: int = 30) -> dict | None:
    started_at = datetime.now(pytz.utc).astimezone(berlin)
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

    # 5. Fetch live prices for report context
    price_context = ""
    try:
        from real_time_price import fetch_prices_for_report, build_price_context_for_prompt
        prices        = fetch_prices_for_report()
        price_context = build_price_context_for_prompt(prices)
        logger.info("Live prices fetched for %d holdings", len(prices))
    except Exception as e:
        logger.warning("Price fetch failed (non-fatal): %s", e)

    # 5.5 Crowd vs Reality — news divergence for top signals
    crowd_vs_reality = ""
    try:
        crowd_vs_reality = build_crowd_vs_reality(ranked, max_signals=3)
        if crowd_vs_reality:
            logger.info("Crowd vs Reality section built")
    except Exception as e:
        logger.warning("Crowd vs Reality failed (non-fatal): %s", e)

    # 6. Build prompt
    prompt = build_report_prompt(
        signal_brief=signal_brief,
        top_market=top_market,
        tickers=tickers,
        date_str=date_str,
        price_context=price_context,
    )

    # 6. Generate report
    report_content, model_used = generate_report(prompt)
    if not report_content:
        return None
    
    if crowd_vs_reality:
        report_content = report_content.replace(
            "## 4. Cluster Analysis",
            f"{crowd_vs_reality}\n\n---\n\n## 4. Cluster Analysis",
            1
        )

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

    elapsed = (datetime.now(pytz.utc).astimezone(berlin) - started_at).total_seconds()
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