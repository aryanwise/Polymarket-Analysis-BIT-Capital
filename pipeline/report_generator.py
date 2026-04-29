"""
pipeline/report_generator.py

Generates the daily BIT Capital Portfolio Impact Report.
Fetches high-impact signals from Supabase, synthesizes them into actionable insights 
using Groq (Llama-3), and saves the final report back to the database with relational links.
"""
import sys
import os
import time
import logging
from groq import Groq
from dotenv import load_dotenv
from datetime import datetime, timezone

# Add project root to path so utils/ can be found
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.supabase_client import get_service_client

# --- Configuration & Setup ---
load_dotenv()
supabase = get_service_client()
client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── 1. Fetch top signals from DB ──────────────────────────────

def fetch_top_signals(limit: int = 15) -> list[dict]:
    """
    Fetch the top relevant signals joined with market + event data.
    Uses the signal_feed view which already filters is_relevant=TRUE.
    """
    logger.info(f"Fetching top {limit} signals from Supabase 'signal_feed'...")
    try:
        res = (
            supabase.table("signal_feed")
            .select("*")
            .order("impact_score", desc=True)
            .limit(limit)
            .execute()
        )
        signals = res.data or []
        logger.info(f"Retrieved {len(signals)} high-impact signals.")
        return signals
    except Exception as e:
        logger.error(f"Failed to fetch signals from Supabase: {e}")
        return []

# ── 2. Build prompt ───────────────────────────────────────────

def build_report_prompt(signals: list[dict]) -> tuple[str, list[str]]:
    """
    Format signals into a structured prompt string.
    Returns (prompt_context, list_of_tickers_mentioned).
    """
    signal_lines = ""
    tickers_seen = set()

    for s in signals:
        ticker = s.get("ticker", "Unknown")
        # Handle cases where company_name might be missing from the view
        company = s.get("company_name", ticker) 
        tickers_seen.add(ticker)
        
        question = s.get("question", "Unknown Market Event")
        sentiment = s.get("sentiment", "Neutral")
        score = s.get("impact_score", 0)
        reasoning = s.get("reasoning", "No DB reasoning provided.")
        
        # Safely parse probability
        try:
            prob = float(s.get("yes_price", 0)) * 100
        except (ValueError, TypeError):
            prob = 0.0

        signal_lines += f"- [TICKER: {ticker}] {company}\n"
        signal_lines += f"  Event: {question} (Implied Probability: {prob:.1f}%)\n"
        signal_lines += f"  Signal: {sentiment.upper()} (Impact Score: {score}/10)\n"
        signal_lines += f"  DB Analysis: {reasoning}\n\n"

    return signal_lines.strip(), list(tickers_seen)

# ── 3. Generate Report via LLM ────────────────────────────────

def generate_report_content(signal_context: str) -> str:
    """Uses Groq's Llama 3 to synthesize the signals into a professional report."""
    current_date = datetime.now(timezone.utc).strftime("%B %d, %Y")
    
    system_prompt = """
    You are the Lead Quantitative Strategist at BIT Capital. 
    Your job is to read raw prediction market signals and write the 'Daily Alpha Report' for the portfolio managers.
    
    CRITICAL INSTRUCTIONS:
    - Do not just list the signals. Synthesize them into a cohesive narrative.
    - Write in a sharp, professional, institutional finance tone.
    - Use Markdown formatting extensively (headers, bullet points, bold text).
    
    STRUCTURE YOUR REPORT EXACTLY LIKE THIS:
    # 📈 BIT Capital Daily Alpha Report
    **Date:** [Insert Today's Date]
    
    ## 1. Executive Summary
    (1 concise paragraph summarizing the overarching macro/tech themes from the data)
    
    ## 2. Sector Impacts & Transmission Mechanisms
    (Group by sector e.g., AI Infrastructure, Crypto Mining, FinTech. Detail how the specific prediction market odds impact the target tickers.)
    
    ## 3. Actionable Portfolio Adjustments
    (Provide concrete recommendations: Overweight, Underweight, Monitor based on the sentiment and impact scores).
    """

    user_prompt = f"Today is {current_date}. Here are the top prediction market signals detected overnight:\n\n{signal_context}\n\nPlease generate the Daily Alpha Report."

    logger.info("Generating report with Llama-3.3-70b...")
    
    # Exponential backoff for API robustness
    delays = [1, 2, 4, 8, 16]
    for delay in delays:
        try:
            completion = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.2 # Low temp for analytical consistency
            )
            return completion.choices[0].message.content
        except Exception as e:
            if delay == delays[-1]:
                logger.error(f"LLM generation failed after retries: {e}")
                return None
            logger.warning(f"Groq API error. Retrying in {delay}s... ({e})")
            time.sleep(delay)

# ── 4. Main Execution ─────────────────────────────────────────

def run_report_pipeline():
    started_at = datetime.now(timezone.utc)
    logger.info("Starting Morning Report Generation Pipeline...")

    # Step 1: Fetch data
    signals = fetch_top_signals(limit=15)
    if not signals:
        logger.warning("No signals found in 'signal_feed'. Aborting report generation.")
        return

    # Step 2: Build context
    signal_context, tickers = build_report_prompt(signals)
    
    # Step 3: Generate report
    report_content = generate_report_content(signal_context)
    if not report_content:
        return

    # Step 4: Save report to DB
    logger.info("Saving report to Supabase 'reports' table...")
    try:
        report_row = {
            "content": report_content,
            "tickers": tickers,
            "signal_count": len(signals),
            "model_used": "llama-3.3-70b-versatile",
            "generated_at": started_at.isoformat(),
        }
        result = supabase.table("reports").insert(report_row).execute()
        report_id = result.data[0]["id"]
        logger.info(f"Report saved — ID: {report_id}")
    except Exception as e:
        logger.error(f"Failed to save report: {e}")
        return

    # Step 5: Link signals to report relationally
    logger.info("Linking signals to report in 'report_signals' table...")
    linked = 0
    for s in signals:
        signal_id = s.get("signal_id")
        if not signal_id:
            continue # Skip if view doesn't provide signal_id
            
        try:
            supabase.table("report_signals").insert({
                "report_id": report_id,
                "signal_id": signal_id,
            }).execute()
            linked += 1
        except Exception as e:
            logger.warning(f"Failed to link signal {signal_id}: {e}")

    # Step 6: Summary
    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    logger.info("=" * 60)
    logger.info("REPORT GENERATION COMPLETE - %.1fs", elapsed)
    logger.info("  Report ID      : %s", report_id)
    logger.info("  Tickers Covered: %s", ", ".join(tickers))
    logger.info("  Signals Linked : %d/%d", linked, len(signals))
    logger.info("=" * 60)

if __name__ == "__main__":
    run_report_pipeline()