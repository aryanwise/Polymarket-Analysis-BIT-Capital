import sys
import os

# Add project root to path so utils/ can be found
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import time
from groq import Groq
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

from utils.supabase_client import get_service_client
supabase = get_service_client()

client = Groq(api_key=os.environ["GROQ_API_KEY"])


# ── Fetch top signals from DB ─────────────────────────────────

def fetch_top_signals(limit: int = 15) -> list[dict]:
    """
    Fetch the top relevant signals joined with market + event data.
    Uses the signal_feed view which already filters is_relevant=TRUE.
    """
    res = (
        supabase.table("signal_feed")
        .select("*")
        .order("impact_score", desc=True)
        .limit(limit)
        .execute()
    )
    return res.data or []


# ── Build prompt ──────────────────────────────────────────────

def build_report_prompt(signals: list[dict]) -> tuple[str, list[str]]:
    """
    Format signals into a prompt string.
    Returns (prompt, list_of_tickers_mentioned).
    """
    signal_lines = ""
    tickers_seen = set()

    for s in signals:
        ticker  = s.get("ticker", "?")
        company = s.get("company_name", "")
        tickers_seen.add(ticker)

        signal_lines += (
            f"- [{s.get('sentiment','?').upper()}] "
            f"{s.get('question','?')} \n"
            f"  Target: {ticker} ({company}) | "
            f"Score: {s.get('impact_score','?')}/10 | "
            f"YES Probability: {float(s.get('yes_price', 0)):.0%} | "
            f"Volume: ${float(s.get('volume', 0)):,.0f}\n"
            f"  Reasoning: {s.get('reasoning','')}\n\n"
        )

    prompt = f"""You are the Head of Research at BIT Capital, a Berlin-based tech-focused fund.
You are writing the Daily Alpha Report for Portfolio Managers based on today's Polymarket signals.

Today's top prediction market signals:
{signal_lines}

Write a professional investment report in Markdown with these sections:

# BIT Capital — Daily Alpha Report ({datetime.now().strftime('%B %d, %Y')})

## Executive Summary
2-3 sentences. What is the dominant macro theme today? What is the overall risk posture?

## Top 3 High-Conviction Signals
For each: explain the market, current probability, what it implies for the specific stock, and why it matters NOW.

## Sector Breakdown
Group signals into: Crypto Infrastructure | Semiconductors | Enterprise Software/AI
For each sector: what do the collective signals say? Is the sector in risk-on or risk-off mode?

## Actionable Insights
3-5 specific recommendations. Examples: 'Trim TSM exposure ahead of tariff resolution', 'Add to IREN on rate stability'.

## Risk Flags
Any signals that contradict each other or suggest elevated uncertainty.

Rules:
- Connect the dots across signals. If the Fed is holding rates AND Bitcoin ETF flows are strong, say what that means for IREN and HUT together.
- Reference specific probabilities and volumes.
- Do not just summarise — interpret.
- Write for a sophisticated audience that already knows the holdings."""

    return prompt, sorted(tickers_seen)


# ── Generate report ───────────────────────────────────────────

def generate_report(top_n: int = 15) -> dict | None:
    """
    Full pipeline:
    1. Fetch top signals from DB
    2. Build prompt
    3. Call Groq
    4. Save to reports + report_signals tables
    Returns the saved report record or None on failure.
    """
    started_at = datetime.now(timezone.utc)
    print(f"\n{'='*55}")
    print(f"  REPORT GENERATOR START — {started_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*55}\n")

    # Step 1: fetch signals
    signals = fetch_top_signals(limit=top_n)

    if not signals:
        print("No relevant signals found. Run the filter pipeline first.")
        return None

    print(f"  Fetched {len(signals)} signals for report")
    for s in signals:
        print(f"  [{s.get('sentiment','?'):>7}] score={s.get('impact_score','?')} | "
              f"{s.get('ticker','?')} | {s.get('question','')[:60]}")

    # Step 2: build prompt
    prompt, tickers = build_report_prompt(signals)
    print(f"\n  Tickers covered: {', '.join(tickers)}")

    # Step 3: call Groq
    print("\n  Generating report with Groq (llama-3.3-70b)...")
    report_content = None
    delays = [1, 2, 4, 8]

    for delay in delays:
        try:
            response = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": "You are a professional financial research analyst writing for sophisticated investors."},
                    {"role": "user",   "content": prompt}
                ],
                temperature=0.4,
            )
            report_content = response.choices[0].message.content
            break

        except Exception as e:
            if delay == delays[-1]:
                print(f"  Report generation failed: {e}")
                return None
            print(f"  Rate limit / error — retrying in {delay}s...")
            time.sleep(delay)

    # Step 4: save report to DB
    print("\n  Saving report to database...")
    try:
        report_row = {
            "content":      report_content,
            "tickers":      tickers,
            "signal_count": len(signals),
            "model_used":   "llama-3.3-70b-versatile",
            "generated_at": started_at.isoformat(),
        }
        result     = supabase.table("reports").insert(report_row).execute()
        report_id  = result.data[0]["id"]
        print(f"  Report saved — ID: {report_id}")
    except Exception as e:
        print(f"  Failed to save report: {e}")
        return None

    # Step 5: link signals to report in report_signals
    print("  Linking signals to report...")
    linked = 0
    for s in signals:
        try:
            supabase.table("report_signals").insert({
                "report_id": report_id,
                "signal_id": s["signal_id"],
            }).execute()
            linked += 1
        except Exception as e:
            print(f"  Failed to link signal {s.get('signal_id')}: {e}")

    # Step 6: summary
    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    print(f"\n{'='*55}")
    print(f"  REPORT COMPLETE — {elapsed:.1f}s")
    print(f"  Report ID:      {report_id}")
    print(f"  Tickers:        {', '.join(tickers)}")
    print(f"  Signals linked: {linked}/{len(signals)}")
    print(f"{'='*55}\n")

    # Preview
    print(report_content[:600] + "\n...")

    return result.data[0]


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    generate_report(top_n=15)