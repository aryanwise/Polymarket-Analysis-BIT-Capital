"""
pipeline/dig_deeper.py

Deep Dive analysis — triggered when analyst clicks "Analyse" on a signal.

Key improvements over previous version:
1. Multiple targeted search queries built from market question (not NULL category field)
2. Article body included in headlines for richer context
3. Prompt cleaned — no references to NULL stage2 fields
4. Smarter direction detection scoped to the direction section only
5. Articles deduplicated across queries
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import re
import time
import warnings
from groq import Groq
from ddgs import DDGS
from dotenv import load_dotenv
from datetime import datetime, timezone

warnings.filterwarnings("ignore", category=DeprecationWarning)
load_dotenv()

from utils.supabase_client import get_service_client
supabase = get_service_client()
client   = Groq(api_key=os.environ["GROQ_API_KEY"])


# ── Holdings context ──────────────────────────────────────────

HOLDINGS = {
    "IREN":  "Bitcoin miner + AI data centers. Revenue tied directly to BTC price and energy costs.",
    "HUT":   "Diversified crypto miner + AI compute. BTC price and energy cost sensitive.",
    "COIN":  "Crypto exchange. Revenue = trading volumes × fee rate. Regulatory risk is key.",
    "HOOD":  "Retail trading + crypto. Rate-sensitive (interest income) + crypto revenue.",
    "NVDA":  "AI chip leader. ~20% revenue from China (H20 chips). Export control + Taiwan risk.",
    "TSM":   "Foundry for all AI chips. Taiwan = existential operational risk.",
    "MU":    "HBM memory for AI data centers. Export control + Taiwan risk.",
    "MSFT":  "Azure + OpenAI commercial deal. Antitrust (DOJ) risk.",
    "GOOGL": "Search + Gemini AI. Antitrust ongoing. Ad market = macro proxy.",
    "AMZN":  "AWS market leader + Bedrock AI. Import tariff risk on hardware.",
    "META":  "Llama AI + digital ads. Ad revenue = macro consumer spending proxy.",
    "DDOG":  "Cloud monitoring. Leading indicator of enterprise AI spend.",
    "LMND":  "AI insurer. Insurance float in bonds — rate-sensitive.",
    "RDDT":  "AI data licensing + niche ads. AI regulation risk.",
}


# ── DB helpers ────────────────────────────────────────────────

def fetch_signal(signal_id: int) -> dict | None:
    try:
        res = (
            supabase.table("signal_feed")
            .select("*")
            .eq("signal_id", signal_id)
            .single()
            .execute()
        )
        return res.data
    except Exception as e:
        print(f"  Signal fetch failed: {e}")
        return None


def get_existing_deep_dive(signal_id: int) -> dict | None:
    """Return cached result if < 6 hours old."""
    try:
        res = (
            supabase.table("deep_dives")
            .select("*")
            .eq("signal_id", signal_id)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        if not res.data:
            return None

        import dateutil.parser
        row       = res.data[0]
        age_hours = (datetime.now(timezone.utc) - dateutil.parser.parse(row["created_at"])).total_seconds() / 3600

        if age_hours < 6:
            print(f"  Cache hit ({age_hours:.1f}h old)")
            return row

        print(f"  Cache expired ({age_hours:.1f}h) — refreshing")
        return None
    except Exception as e:
        print(f"  Cache check failed: {e}")
        return None


# ── News search ───────────────────────────────────────────────

def build_news_queries(signal: dict) -> list[str]:
    """
    Build 2-3 targeted queries from the market question.
    Does NOT use category/reasoning/sentiment — those are NULL from stage2.
    """
    ticker   = signal.get("ticker", "")
    company  = signal.get("company_name", ticker)
    question = signal.get("question", "")
    q_lower  = question.lower()

    queries = []

    # Query 1 — always: company + ticker + recent news
    queries.append(f"{company} {ticker} stock news 2026")

    # Query 2 — extract core topic from question text
    topic = re.sub(
        r"(Will |will |by December 31|by June 30|before 2027|by end of 2026|"
        r"in 2026|by May|by April|or higher|or lower|by \d+|\?)",
        "", question
    ).strip()[:70]
    if topic:
        queries.append(topic)

    # Query 3 — theme-specific macro query
    if any(w in q_lower for w in ["bitcoin", "btc", "crypto"]):
        queries.append(f"Bitcoin price outlook 2026 {ticker} miners")
    elif any(w in q_lower for w in ["fed", "rate cut", "interest rate", "fomc"]):
        queries.append(f"Federal Reserve interest rates decision 2026")
    elif any(w in q_lower for w in ["taiwan", "chip", "semiconductor", "export control"]):
        queries.append(f"semiconductor chip export controls China Taiwan 2026 {ticker}")
    elif any(w in q_lower for w in ["openai", "gpt", "claude", "gemini", "ai model"]):
        queries.append(f"AI model competition OpenAI {ticker} 2026")
    elif any(w in q_lower for w in ["tariff", "trade"]):
        queries.append(f"US tariffs technology imports 2026 {ticker}")
    elif any(w in q_lower for w in ["inflation", "cpi"]):
        queries.append(f"US inflation outlook 2026 Fed impact")
    elif any(w in q_lower for w in ["ipo", "acquisition", "merger"]):
        queries.append(f"{company} IPO acquisition 2026")

    return queries[:3]


def fetch_news(queries: list[str], max_per_query: int = 3) -> tuple[str, list[str]]:
    """
    Run multiple DuckDuckGo searches, deduplicate, include article body.
    Returns (formatted_headlines_with_body, source_urls).
    """
    all_articles = []
    seen_urls    = set()

    for query in queries:
        print(f"  Searching: '{query[:60]}'")
        try:
            time.sleep(3)
            with DDGS() as ddgs:
                results = list(ddgs.news(query=query, max_results=max_per_query))

            for article in (results or []):
                url = article.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_articles.append(article)
        except Exception as e:
            print(f"  Search failed for '{query[:40]}': {e}")
            continue

    if not all_articles:
        return "No recent news found.", []

    # Format with body excerpt for richer LLM context
    lines = []
    for i, article in enumerate(all_articles[:6], 1):
        title  = article.get("title", "No title")
        source = article.get("source", "?")
        date   = str(article.get("date", ""))[:10]
        body   = article.get("body", "")

        lines.append(f"{i}. {title}")
        lines.append(f"   {source}  |  {date}")
        if body:
            lines.append(f"   {body[:150].strip()}...")
        lines.append("")

    source_urls = [a.get("url", "") for a in all_articles[:6] if a.get("url")]
    return "\n".join(lines).strip(), source_urls


# ── Analysis prompt ───────────────────────────────────────────

def build_prompt(signal: dict, news_headlines: str) -> str:
    ticker   = signal.get("ticker", "?")
    company  = signal.get("company_name", ticker)
    thesis   = HOLDINGS.get(ticker, "Technology holding in BIT Capital portfolio.")
    yes      = float(signal.get("yes_price") or 0)
    question = signal.get("question", "?")
    event    = signal.get("event_title", "?")

    # Probability framing in plain English
    if yes >= 0.65:
        prob_label = f"{yes:.0%} — market strongly expects this"
    elif yes >= 0.40:
        prob_label = f"{yes:.0%} — genuinely contested, high uncertainty"
    elif yes >= 0.20:
        prob_label = f"{yes:.0%} — tail risk, low probability but material"
    else:
        prob_label = f"{yes:.0%} — unlikely scenario, monitoring only"

    return f"""You are a senior equity analyst at BIT Capital writing a quick briefing.

━━ SIGNAL ━━
Event:       {event}
Question:    {question}
Probability: {prob_label}
Stock:       {ticker} — {company}
Thesis:      {thesis}

━━ LATEST NEWS ━━
{news_headlines}

━━ YOUR ANALYSIS ━━
Write a concise briefing with exactly these three sections:

## Agreement or Conflict
Does the news align with the Polymarket probability or is there a divergence?
A divergence between news sentiment and market probability is where alpha lives.
Reference specific headlines by number (e.g., "Article 2 suggests...").

## Short-term Direction for {ticker}
State clearly: Bullish / Bearish / Neutral — and why.
Give one specific price catalyst or risk event to watch.
Be direct — make a call.

## Reasoning
Connect the {yes:.0%} YES probability with what the news says.
What would need to happen for the thesis to change?
Reference at least one specific headline.

Keep each section to 3-4 bullet points. Morning briefing style — no fluff."""


# ── Direction extraction ──────────────────────────────────────

def extract_direction(text: str) -> str:
    """
    Extract direction from the Short-term Direction section specifically.
    Avoids false positives from words like 'not bullish' in other sections.
    """
    # Isolate the direction section
    section = re.search(
        r"Short-term Direction.*?\n(.*?)(?=##|\Z)",
        text, re.DOTALL | re.IGNORECASE
    )
    target = section.group(1).lower() if section else text.lower()

    bull = sum(1 for w in ["bullish", "upside", "positive outlook", "buy"] if w in target)
    bear = sum(1 for w in ["bearish", "downside", "negative outlook", "sell"] if w in target)

    if bull > bear:   return "Bullish"
    if bear > bull:   return "Bearish"
    return "Neutral"


# ── Main entry point ──────────────────────────────────────────

def dig_deeper(signal_id: int) -> dict:
    """
    Triggered when analyst clicks 'Analyse' on a signal.
    Steps: cache check → fetch signal → news search → Groq → save → return
    """
    started_at = datetime.now(timezone.utc)
    print(f"\n{'='*55}\n  DIG DEEPER — signal_id={signal_id}\n{'='*55}")

    # 1. Cache check
    cached = get_existing_deep_dive(signal_id)
    if cached:
        return {
            "analysis_text": cached["analysis_text"],
            "direction":     cached["direction"],
            "source_urls":   cached.get("source_urls") or [],
            "from_cache":    True,
        }

    # 2. Fetch signal from signal_feed view
    signal = fetch_signal(signal_id)
    if not signal:
        return {"error": f"Signal {signal_id} not found"}

    print(f"  Ticker:   {signal.get('ticker')}")
    print(f"  Question: {signal.get('question','')[:70]}")

    # 3. Build targeted queries from question text
    queries = build_news_queries(signal)
    print(f"  Queries:  {len(queries)}")

    # 4. Search news — multiple queries, deduplicated, with article body
    news_headlines, source_urls = fetch_news(queries, max_per_query=3)
    print(f"  Articles: {len(source_urls)} found")

    if not source_urls:
        print("  Warning: no articles found — analysis will rely on probability only")

    # 5. Run Groq
    print("  Running analysis...")
    prompt        = build_prompt(signal, news_headlines)
    analysis_text = None
    delays        = [2, 4, 8, 16]

    for i, delay in enumerate(delays):
        try:
            resp = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system",
                     "content": "You are a quantitative equity analyst. "
                                "Write concise, specific, actionable briefings. "
                                "Always cite specific news headlines. Always make a directional call."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                max_tokens=800,
            )
            analysis_text = resp.choices[0].message.content
            break
        except Exception as e:
            if i == len(delays) - 1:
                return {"error": f"Groq failed: {e}"}
            print(f"  Rate limit — retrying in {delay}s...")
            time.sleep(delay)

    # 6. Extract direction
    direction = extract_direction(analysis_text)

    # 7. Save to DB
    try:
        supabase.table("deep_dives").insert({
            "signal_id":      signal_id,
            "analysis_text":  analysis_text,
            "direction":      direction,
            "news_query":     " | ".join(queries),
            "news_headlines": news_headlines,
            "source_urls":    source_urls,
            "model_used":     "llama-3.3-70b-versatile",
        }).execute()
        print("  Saved to deep_dives")
    except Exception as e:
        print(f"  DB save failed: {e}")

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    print(f"  Complete — {elapsed:.1f}s\n")

    return {
        "analysis_text": analysis_text,
        "direction":     direction,
        "source_urls":   list(dict.fromkeys(source_urls)),
        "from_cache":    False,
    }


if __name__ == "__main__":
    # Get a real ID: SELECT signal_id, ticker, question FROM signal_feed LIMIT 5;
    result = dig_deeper(1)

    print("\n" + "="*60)
    if "error" in result:
        print(f"Error: {result['error']}")
    else:
        print(f"Direction  : {result['direction']}")
        print(f"From cache : {result['from_cache']}")
        print(f"Sources    : {len(result['source_urls'])}")
        print()
        print(result["analysis_text"])
        print("\nSources:")
        for url in result["source_urls"]:
            print(f"  {url}")