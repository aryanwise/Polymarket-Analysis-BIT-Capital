import sys
import os

# Add project root to path so utils/ can be found
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import time
import warnings
from groq import Groq
from duckduckgo_search import DDGS
from dotenv import load_dotenv
from datetime import datetime, timezone

warnings.filterwarnings("ignore", category=DeprecationWarning)
load_dotenv()

from utils.supabase_client import get_service_client
supabase = get_service_client()

client = Groq(api_key=os.environ["GROQ_API_KEY"])


# ── Fetch signal from DB ──────────────────────────────────────

def fetch_signal(signal_id: int) -> dict | None:
    """
    Fetch a single signal with its market + event data.
    Uses signal_feed view so everything is already joined.
    """
    res = (
        supabase.table("signal_feed")
        .select("*")
        .eq("signal_id", signal_id)
        .single()
        .execute()
    )
    return res.data


def get_existing_deep_dive(signal_id: int) -> dict | None:
    """
    Check if this signal was already analyzed.
    Returns the existing deep dive if found and less than 6 hours old.
    This prevents re-running Groq on every click.
    """
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

    existing   = res.data[0]
    created_at = existing["created_at"]

    # Parse and check age
    from datetime import datetime, timezone
    import dateutil.parser
    created_dt = dateutil.parser.parse(created_at)
    age_hours  = (datetime.now(timezone.utc) - created_dt).total_seconds() / 3600

    if age_hours < 6:
        print(f"  Cache hit — last analyzed {age_hours:.1f}h ago, reusing result")
        return existing

    print(f"  Cache expired ({age_hours:.1f}h old) — re-analyzing")
    return None


# ── News fetch ────────────────────────────────────────────────

def fetch_live_news(query: str, max_results: int = 4) -> tuple[str, list[str]]:
    """Fetch real-time news headlines via DuckDuckGo."""
    print(f"  Fetching news: '{query}'")
    try:
        time.sleep(1)   # mitigate rate limits
        results = DDGS().news(keywords=query, max_results=max_results)

        if not results:
            return "No recent news found.", []

        headlines  = ""
        source_urls = []
        for i, article in enumerate(results, 1):
            headlines += (
                f"{i}. {article.get('title', 'No title')} "
                f"({article.get('source', '?')} — {article.get('date', '')})\n"
            )
            if article.get("url"):
                source_urls.append(article["url"])

        return headlines, source_urls

    except Exception as e:
        print(f"  News fetch failed: {e}")
        return "News search failed.", []


# ── Build prompt ──────────────────────────────────────────────

def build_prompt(signal: dict, news_headlines: str) -> str:
    return f"""You are an expert equity analyst at BIT Capital.
A portfolio manager clicked "Dig Deeper" on the following signal.

━━ SIGNAL CONTEXT ━━
Polymarket Event:    {signal.get('event_title', '?')}
Market Question:     {signal.get('question', '?')}
YES Probability:     {float(signal.get('yes_price', 0)):.0%}
Category:            {signal.get('category', '?')}
Target Ticker:       {signal.get('ticker', '?')} ({signal.get('company_name', '?')})
Sentiment:           {signal.get('sentiment', '?')}
Impact Score:        {signal.get('impact_score', '?')}/10
Initial Hypothesis:  {signal.get('reasoning', '?')}

━━ REAL-TIME NEWS ━━
{news_headlines}

━━ YOUR TASK ━━
1. Agreement or conflict? Compare the news sentiment with the Polymarket probability.
   Are they aligned or is there a divergence worth acting on?

2. Short-term direction for {signal.get('ticker', '?')}: Bullish / Bearish / Neutral?
   Give a specific price catalyst if one exists.

3. Reasoning: Connect the prediction market probability with the news.
   Be specific — reference actual headlines above, not generic statements.

Format your response with clear bullet points under each section.
Be concise — this is a morning briefing, not an essay."""


# ── Core analysis function ────────────────────────────────────

def dig_deeper(signal_id: int) -> dict:
    """
    Main entry point for Dig Deeper.

    1. Check cache — return existing analysis if < 6 hours old
    2. Fetch signal from DB
    3. Fetch live news from DuckDuckGo
    4. Run Groq analysis
    5. Save to deep_dives table
    6. Return result

    Returns dict with keys: analysis_text, direction, source_urls, from_cache
    """
    started_at = datetime.now(timezone.utc)
    print(f"\n{'='*55}")
    print(f"  DIG DEEPER — signal_id={signal_id}")
    print(f"{'='*55}")

    # Step 1: check cache
    cached = get_existing_deep_dive(signal_id)
    if cached:
        return {
            "analysis_text": cached["analysis_text"],
            "direction":     cached["direction"],
            "source_urls":   cached["source_urls"] or [],
            "from_cache":    True,
            "created_at":    cached["created_at"],
        }

    # Step 2: fetch signal
    signal = fetch_signal(signal_id)
    if not signal:
        return {"error": f"Signal {signal_id} not found"}

    print(f"  Ticker:   {signal.get('ticker')}")
    print(f"  Question: {signal.get('question','')[:70]}")

    # Step 3: fetch news
    news_query     = f"{signal.get('ticker')} stock {signal.get('event_title', '')} {signal.get('category', '')}"
    news_headlines, source_urls = fetch_live_news(news_query)

    # Step 4: run Groq
    print("  Running Groq analysis...")
    prompt        = build_prompt(signal, news_headlines)
    analysis_text = None
    delays        = [1, 2, 4, 8]

    for delay in delays:
        try:
            response = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": "You are a quantitative equity analyst writing concise briefings."},
                    {"role": "user",   "content": prompt}
                ],
                temperature=0.2,
            )
            analysis_text = response.choices[0].message.content
            break

        except Exception as e:
            if delay == delays[-1]:
                return {"error": f"Groq failed after retries: {e}"}
            print(f"  Rate limit — retrying in {delay}s...")
            time.sleep(delay)

    # Step 5: extract direction from analysis text
    direction = "Neutral"
    text_lower = analysis_text.lower()
    if "bullish" in text_lower:
        direction = "Bullish"
    elif "bearish" in text_lower:
        direction = "Bearish"

    # Step 6: save to deep_dives table
    print("  Saving to database...")
    try:
        row = {
            "signal_id":      signal_id,
            "analysis_text":  analysis_text,
            "direction":      direction,
            "news_query":     news_query,
            "news_headlines": news_headlines,
            "source_urls":    source_urls,
            "model_used":     "llama-3.3-70b-versatile",
        }
        supabase.table("deep_dives").insert(row).execute()
        print("  Saved successfully")
    except Exception as e:
        print(f"  Failed to save deep dive: {e}")

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    print(f"  Done — {elapsed:.1f}s\n")

    return {
        "analysis_text": analysis_text,
        "direction":     direction,
        "source_urls":   list(set(source_urls)),
        "from_cache":    False,
    }


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    # Pass the signal_id from your signals table
    # You can find a valid ID by running:
    # SELECT signal_id, ticker, question FROM signal_feed LIMIT 5;

    SIGNAL_ID = 1   # replace with a real signal_id from your DB

    result = dig_deeper(SIGNAL_ID)

    print("\n" + "="*60)
    print("  DIG DEEPER ANALYSIS")
    print("="*60)

    if "error" in result:
        print(f"Error: {result['error']}")
    else:
        print(f"Direction:  {result['direction']}")
        print(f"From cache: {result['from_cache']}")
        print()
        print(result["analysis_text"])

        if result["source_urls"]:
            print("\nSources:")
            for url in result["source_urls"]:
                print(f"  - {url}")