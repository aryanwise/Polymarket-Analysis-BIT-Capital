"""
pipeline/backfill_enrichment.py

One-time script to backfill sentiment, impact_score, reasoning
for all signals that were written before Pass B was working.

Run once:
    uv run pipeline/backfill_enrichment.py
    uv run pipeline/backfill_enrichment.py --dry-run   # preview only
"""

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import re
import time
import logging
import argparse
from dotenv import load_dotenv

load_dotenv()
from utils.supabase_client import get_service_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

GROQ_MODEL    = "meta-llama/llama-4-scout-17b-16e-instruct"
BATCH_SIZE    = 15
BATCH_DELAY   = 2.0

ENRICHMENT_PROMPT = """You are a portfolio analyst at BIT Capital.

For each signal, output exactly 3 fields on one line, tab-separated:
  [index]  SENTIMENT  SCORE  REASONING

Rules:
  SENTIMENT : Bullish, Bearish, or Neutral — for the named ticker only
  SCORE     : Integer 1-10 (10 = existential/>10% revenue, 1 = very indirect)
  REASONING : One sentence. Name the exact P&L mechanism and dollar figure.
              No hedging. State direction and number directly.

Output format — one line per signal:
[index]\tBullish\t7\tOne sentence reasoning here.

Example:
[0]\tBearish\t8\tExport controls on H20 chips remove ~$15B of NVDA China revenue directly.
[1]\tBullish\t6\tRate cut at 72% YES compresses LMND bond float income by ~150bps."""


def _call_groq(prompt: str) -> str | None:
    try:
        from groq import Groq
        client = Groq(api_key=os.environ["GROQ_API_KEY"])
        resp = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {"role": "system", "content": ENRICHMENT_PROMPT},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.1,
            max_tokens=800,
        )
        return resp.choices[0].message.content
    except Exception as e:
        logger.warning("Groq failed: %s", e)
        return None


def _build_prompt(signals: list[dict]) -> str:
    lines = []
    for i, s in enumerate(signals):
        yes    = float(s.get("yes_price") or 0)
        ticker = s.get("ticker", "")
        lines.append(
            f"[{i}] Ticker: {ticker} | YES: {yes:.0%}\n"
            f"    Question: {s.get('question','')}"
        )
    return "Enrich each signal:\n\n" + "\n\n".join(lines)


def _parse_response(text: str, n: int) -> list[dict]:
    results = [{"sentiment": "Neutral", "impact_score": 5, "reasoning": ""} for _ in range(n)]
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r"\[(\d+)\]\t([A-Za-z]+)\t(\d+)\t(.*)", line)
        if not m:
            m = re.match(r"\[(\d+)\]\s+(Bullish|Bearish|Neutral)\s+(\d+)\s+(.*)", line, re.I)
        if not m:
            continue
        idx       = int(m.group(1))
        sentiment = m.group(2).capitalize()
        score     = max(1, min(10, int(m.group(3))))
        reasoning = m.group(4).strip()
        if idx < n and sentiment in ("Bullish", "Bearish", "Neutral"):
            results[idx] = {"sentiment": sentiment, "impact_score": score, "reasoning": reasoning}
    return results


def run_backfill(dry_run: bool = False):
    supabase = get_service_client()

    # Fetch all NULL signals
    res = supabase.table("signals").select("*").is_("sentiment", "null").execute()
    signals = res.data or []

    if not signals:
        logger.info("No NULL signals found — nothing to backfill.")
        return

    logger.info("Backfilling %d signals (dry_run=%s)...", len(signals), dry_run)

    updated = 0
    errors  = 0

    for batch_start in range(0, len(signals), BATCH_SIZE):
        batch     = signals[batch_start:batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_b   = (len(signals) + BATCH_SIZE - 1) // BATCH_SIZE

        logger.info("Batch %d/%d (%d signals)...", batch_num, total_b, len(batch))

        prompt   = _build_prompt(batch)
        raw      = _call_groq(prompt)

        if not raw:
            logger.warning("Batch %d — Groq failed, skipping", batch_num)
            errors += len(batch)
            time.sleep(BATCH_DELAY)
            continue

        enriched = _parse_response(raw, len(batch))

        for s, enr in zip(batch, enriched):
            logger.info(
                "  [%s] %s score=%d — %s",
                s["ticker"], enr["sentiment"], enr["impact_score"],
                enr["reasoning"][:60]
            )

            if not dry_run and enr["reasoning"]:
                try:
                    supabase.table("signals").update({
                        "sentiment":    enr["sentiment"],
                        "impact_score": enr["impact_score"],
                        "reasoning":    enr["reasoning"],
                    }).eq("id", s["id"]).execute()
                    updated += 1
                except Exception as e:
                    logger.error("Update failed for id=%d: %s", s["id"], e)
                    errors += 1
            else:
                updated += 1

        time.sleep(BATCH_DELAY)

    logger.info("=" * 55)
    logger.info("BACKFILL COMPLETE")
    logger.info("  Updated : %d", updated)
    logger.info("  Errors  : %d", errors)
    logger.info("  Dry run : %s", dry_run)
    logger.info("=" * 55)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing to DB")
    args = parser.parse_args()
    run_backfill(dry_run=args.dry_run)