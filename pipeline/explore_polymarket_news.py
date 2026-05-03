import re
import unicodedata
import requests
import json
import os
from dotenv import load_dotenv
from tavily import TavilyClient
from google import genai
from google.genai import types

# ==============================
# ENV SETUP
# ==============================
load_dotenv()

TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")

if not TAVILY_API_KEY:
    raise ValueError("TAVILY_API_KEY not found in .env")

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found in .env")

# ── Gemini client (fallback) ──
client = genai.Client(api_key=GEMINI_API_KEY)

# ── Mistral client (primary) ──
mistral_client = None
if MISTRAL_API_KEY:
    try:
        from mistralai.client import Mistral
        mistral_client = Mistral(api_key=MISTRAL_API_KEY)
    except ImportError:
        print("[WARN] mistralai SDK not installed. Run: pip install mistralai")
else:
    print("[WARN] MISTRAL_API_KEY not found. Using Gemini only.")


# ==============================
# HOLDINGS CONTEXT
# ==============================
raw_holdings = {
    "IREN":  "Bitcoin mining and AI data centers.",
    "MSFT":  "Azure + OpenAI. Enterprise AI play.",
    "GOOGL": "Search + Gemini AI. Antitrust risk.",
    "LMND":  "AI-driven insurance. Rate-sensitive.",
    "RDDT":  "AI data licensing + niche ads.",
    "MU":    "HBM memory for AI data centers.",
    "TSM":   "AI chip foundry. Taiwan risk.",
    "HUT":   "Crypto + AI compute infrastructure.",
    "HOOD":  "Retail trading + crypto. Rate-sensitive.",
    "DDOG":  "Cloud monitoring. AI spend indicator.",
    "AMZN":  "AWS + Bedrock. Import tariff exposure.",
    "COIN":  "Crypto exchange. Regulatory risk.",
    "META":  "Llama AI + digital ads.",
    "NVDA":  "AI accelerators. Chip export risk."
}

# ==============================
# SLUG HELPERS
# ==============================
def normalise_slug(text, sep='-'):
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore').decode('ascii')
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', ' ', text)
    text = text.strip()
    text = re.sub(r'\s+', sep, text)
    return text


def unslugify(slug, capitalize=True):
    text = re.sub(r'[-_]+', ' ', slug).strip()
    text = re.sub(r'\s+', ' ', text)
    return text.title() if capitalize else text


def normalize_query(text):
    text = re.sub(r'[-_]+', ' ', text)
    return re.sub(r'\s+', ' ', text)


# ==============================
# FETCH POLYMARKET EVENT
# ==============================
def fetch_event_from_text(user_input):
    slug = normalise_slug(user_input)

    url = f"https://gamma-api.polymarket.com/events/slug/{slug}"
    response = requests.get(url)

    if response.status_code != 200:
        return {"error": f"Event not found for slug: {slug}"}

    data = response.json()

    result = {
        "title": unslugify(data.get("ticker", "")),
        "markets": []
    }

    for m in data.get("markets", []):
        outcomes = json.loads(m.get("outcomes", "[]"))
        prices = json.loads(m.get("outcomePrices", "[]"))

        if not prices or len(outcomes) != len(prices):
            continue

        outcome_map = {
            outcome: round(float(price), 2)
            for outcome, price in zip(outcomes, prices)
        }

        result["markets"].append({
            "question": m.get("question"),
            "outcomes": outcome_map
        })

    return result


# ==============================
# FETCH NEWS
# ==============================
def fetch_news(query_input, max_results=5):
    tavily_client = TavilyClient(api_key=TAVILY_API_KEY)

    response = tavily_client.search(
        query=normalize_query(query_input),
        search_depth="advanced",
        max_results=max_results
    )

    return [
        {
            "title": r.get("title"),
            "content": r.get("content")
        }
        for r in response.get("results", [])
    ]


# ==============================
# PROMPT BUILDER (shared)
# ==============================
def build_gemini_prompt(event, news):
    markets_text = "\n".join([
        f"- {m['question']} → {m['outcomes']}"
        for m in event.get("markets", [])
    ])

    news_text = "\n".join([
        f"{i+1}. {n['title']}\n{(n['content'] or '')[:200]}"
        for i, n in enumerate(news)
    ])

    holdings_text = "\n".join([
        f"- {k}: {v}"
        for k, v in raw_holdings.items()
    ])

    return f"""You are a portfolio analyst at BIT Capital.

MARKET:
{event.get("title")}
{markets_text}

NEWS:
{news_text}

PORTFOLIO:
{holdings_text}

TASK:

## Agreement or Divergence
Summarize if news supports or contradicts the market.

## Market Interpretation
What is being priced? What would need to change for this to flip?

## Portfolio Impact

Focus ONLY on real transmission mechanisms.

MANDATORY LOGIC:
[event + market probability + news] → [what changes] → [driver] → [business effect]

Rules:
- Max 4 holdings
- ONLY include if a clear mechanism exists
- If no mechanism → output NOTHING

Use drivers:
- crypto → volatility, trading activity, BTC price
- rates → margins, valuation
- AI → capex, compute demand
- ads → macro demand

Format STRICTLY (one line per holding):
TICKER → impact explanation

Rules:
- No bullets
- No numbering
- No markdown
- Each line MUST start with ticker

## Trade Insight
Give one clear takeaway (no Bullish/Bearish labels).
"""


# ==============================
# MISTRAL (PRIMARY)
# ==============================
def run_mistral_analysis(event, news):
    """
    Primary analysis via Mistral AI.
    Falls back to Gemini if Mistral is unavailable.
    """
    if not mistral_client:
        raise RuntimeError("Mistral client not initialized")

    prompt = build_gemini_prompt(event, news)

    response = mistral_client.chat.complete(
        model="mistral-large-latest",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=600
    )

    return response.choices[0].message.content


# ==============================
# GEMINI (FALLBACK)
# ==============================
def run_gemini_analysis(event, news):
    """
    Fallback analysis via Gemini.
    Kept for backward compatibility.
    """
    prompt = build_gemini_prompt(event, news)

    response = client.models.generate_content(
        model="gemini-2.5-flash-lite",
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0.2,
            max_output_tokens=600
        )
    )

    return response.text


# ==============================
# UNIFIED ENTRYPOINT (Mistral → Gemini fallback)
# ==============================
def run_analysis(event, news):
    """
    Run analysis with Mistral as primary.
    Automatically falls back to Gemini on any failure.
    """
    if mistral_client:
        try:
            return run_mistral_analysis(event, news)
        except Exception as e:
            print(f"[WARN] Mistral failed ({e}). Falling back to Gemini...")
            return run_gemini_analysis(event, news)
    else:
        return run_gemini_analysis(event, news)


# ==============================
# PARSER
# ==============================
def extract_portfolio_impacts(text):
    section = re.search(
        r"## Portfolio Impact(.*?)(##|$)",
        text,
        re.DOTALL | re.IGNORECASE
    )

    if not section:
        return []

    content = section.group(1).strip()

    # Early exit
    if "no meaningful impact" in content.lower():
        return []

    impacts = []
    lines = content.split("\n")

    for line in lines:
        line = line.strip()

        if not line:
            continue

        # Remove bullets (*, -, •)
        line = re.sub(r"^[\*\-\•\s]+", "", line)

        # Match: TICKER → text
        match = re.match(r"([A-Z]{2,5})\s*→\s*(.+)", line)

        if match:
            ticker = match.group(1)
            impact = match.group(2).strip()

            impacts.append({
                "ticker": ticker,
                "impact": impact
            })
            continue

        # Fallback: catch loose formats like "COIN - something"
        match_alt = re.match(r"([A-Z]{2,5})\s*[-:]\s*(.+)", line)
        if match_alt:
            ticker = match_alt.group(1)
            impact = match_alt.group(2).strip()

            impacts.append({
                "ticker": ticker,
                "impact": impact
            })

    return impacts


# ==============================
# FORMAT OUTPUT (UX)
# ==============================
def format_output(event, analysis_text, portfolio_impacts):
    print(f"\nMARKET: {event.get('title')}\n")

    # Market signal
    market = event.get("markets", [])
    if market:
        print("📊 Market Signal")
        print(f"→ {market[0]['outcomes']}\n")

    # Extract short lines
    def extract_section(name):
        match = re.search(rf"{name}(.*?)(##|$)", analysis_text, re.DOTALL)
        return match.group(1).strip() if match else ""

    news = extract_section("Agreement or Divergence")
    trade = extract_section("Trade Insight")

    print("📰 News Sentiment")
    print(f"→ {news.splitlines()[0] if news else 'N/A'}\n")

    print("💼 BIT Capital Impact")
    if portfolio_impacts:
        for p in portfolio_impacts:
            print(f"- {p['ticker']} → {p['impact']}")
    else:
        print("→ No meaningful impact")

    print("\n🧠 Takeaway")
    print(f"→ {trade.splitlines()[0] if trade else 'N/A'}")


# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    print("\n=== POLYMARKET EXPLORE ===\n")

    user_input = input("Enter market: ")

    event = fetch_event_from_text(user_input)
    news = fetch_news(user_input)
    analysis = run_analysis(event, news)   # ← uses Mistral → Gemini fallback
    impacts = extract_portfolio_impacts(analysis)

    format_output(event, analysis, impacts)