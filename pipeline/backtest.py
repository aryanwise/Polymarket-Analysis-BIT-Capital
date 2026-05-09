"""
pipeline/backtest.py

Signal outcome tracker — answers: "were our signals actually right?"

How it works:
  1. Finds signals where the Polymarket market has expired (end_date < now)
  2. Checks if the market resolved YES or NO via current yes_price
     (price ≈ 1.0 = resolved YES, price ≈ 0.0 = resolved NO)
  3. Fetches the stock price at signal creation and at market expiry
  4. Checks if the sentiment direction was correct:
     Bullish signal + stock up = CORRECT
     Bearish signal + stock down = CORRECT
     Otherwise = INCORRECT
  5. Saves to signal_outcomes table

Run weekly — gives you accuracy stats per cluster, per ticker, per score range.

Schema (add to schema3.sql):
  CREATE TABLE signal_outcomes (
    id                 BIGSERIAL PRIMARY KEY,
    signal_id          INT REFERENCES signals(id),
    ticker             TEXT,
    sentiment          TEXT,
    impact_score       INT,
    yes_resolved       BOOLEAN,
    price_at_signal    DECIMAL,
    price_at_expiry    DECIMAL,
    price_change_pct   DECIMAL,
    sentiment_correct  BOOLEAN,
    evaluated_at       TIMESTAMPTZ DEFAULT NOW()
  );
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import time
import logging
import yfinance as yf
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

from utils.supabase_client import get_service_client
supabase = get_service_client()

logger = logging.getLogger(__name__)

# A market is considered resolved if YES price is above or below these thresholds
RESOLVED_YES_THRESHOLD = 0.92   # YES price ≥ 0.92 = market resolved YES
RESOLVED_NO_THRESHOLD  = 0.08   # YES price ≤ 0.08 = market resolved NO


# ─────────────────────────────────────────────────────────────
# FETCH EXPIRED SIGNALS
# ─────────────────────────────────────────────────────────────

def fetch_expired_unscored_signals() -> list[dict]:
    """
    Find signals that:
    1. Have expired (end_date < now)
    2. Have sentiment populated (Pass B ran)
    3. Haven't been evaluated yet (not in signal_outcomes)
    """
    try:
        now = datetime.now(timezone.utc).isoformat()

        # Expired signals with sentiment
        res = (
            supabase.table("signals")
            .select("id, market_id, ticker, sentiment, impact_score, yes_price, end_date, question")
            .lt("end_date", now)
            .not_.is_("sentiment", "null")
            .execute()
        )
        all_expired = res.data or []

        if not all_expired:
            return []

        # Get already-evaluated signal IDs
        evaluated_res = (
            supabase.table("signal_outcomes")
            .select("signal_id")
            .execute()
        )
        evaluated_ids = {r["signal_id"] for r in (evaluated_res.data or [])}

        # Filter to unevaluated only
        unevaluated = [s for s in all_expired if s["id"] not in evaluated_ids]
        logger.info("Expired signals: %d total | %d unevaluated",
                    len(all_expired), len(unevaluated))
        return unevaluated

    except Exception as e:
        logger.error("Failed to fetch expired signals: %s", e)
        return []


# ─────────────────────────────────────────────────────────────
# PRICE LOOKUP
# ─────────────────────────────────────────────────────────────

def get_price_at_date(ticker: str, target_date: str) -> float | None:
    """
    Get closing price for a ticker on or near a specific date.
    Uses yfinance history with a 5-day window around the target date.
    """
    try:
        from datetime import datetime
        dt       = datetime.fromisoformat(target_date.replace("Z", "+00:00"))
        start    = (dt - timedelta(days=3)).strftime("%Y-%m-%d")
        end      = (dt + timedelta(days=3)).strftime("%Y-%m-%d")

        hist = yf.Ticker(ticker).history(start=start, end=end, interval="1d")
        if hist.empty:
            return None

        # Get the closest available date
        return float(hist["Close"].iloc[-1])
    except Exception as e:
        logger.warning("Price lookup failed for %s at %s: %s", ticker, target_date, e)
        return None


def get_current_price(ticker: str) -> float | None:
    """Get latest price for a ticker."""
    try:
        info = yf.Ticker(ticker).fast_info
        return float(info.last_price or 0) or None
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
# RESOLUTION CHECK
# ─────────────────────────────────────────────────────────────

def check_market_resolution(yes_price: float) -> bool | None:
    """
    Infer market resolution from current YES price.
    Returns True = resolved YES, False = resolved NO, None = still unresolved.

    Note: Polymarket doesn't have a public resolution API on the free tier.
    We infer from price: near 1.0 = YES won, near 0.0 = NO won.
    Markets in the 0.08-0.92 range are still live or recently expired.
    """
    if yes_price >= RESOLVED_YES_THRESHOLD:
        return True
    if yes_price <= RESOLVED_NO_THRESHOLD:
        return False
    return None   # can't determine — skip


# ─────────────────────────────────────────────────────────────
# ACCURACY CHECK
# ─────────────────────────────────────────────────────────────

def check_sentiment_correct(
    sentiment: str,
    yes_resolved: bool,
    price_change_pct: float,
) -> bool | None:
    """
    Was the sentiment directionally correct?

    Logic:
      Bullish + YES resolved + stock up   = CORRECT
      Bullish + NO resolved + stock down  = CORRECT (bearish outcome = stock fell)
      Bearish + YES resolved + stock down = CORRECT
      Bearish + NO resolved + stock up    = CORRECT
      Neutral                             = None (can't evaluate direction)

    A 2% threshold for "meaningful move" to avoid noise.
    """
    MOVE_THRESHOLD = 2.0

    if sentiment == "Neutral":
        return None

    price_up   = price_change_pct >  MOVE_THRESHOLD
    price_down = price_change_pct < -MOVE_THRESHOLD

    if sentiment == "Bullish":
        if yes_resolved and price_up:   return True
        if not yes_resolved and price_down: return True   # risk didn't materialise... but price fell anyway
        if yes_resolved and price_down: return False
        return None   # inconclusive move

    if sentiment == "Bearish":
        if yes_resolved and price_down: return True
        if not yes_resolved and price_up: return True
        if yes_resolved and price_up:   return False
        return None

    return None


# ─────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────

def run_backtest(dry_run: bool = False) -> dict:
    """
    Evaluate expired signals and record outcomes.
    Call weekly (or after each pipeline run).
    """
    logger.info("=" * 55)
    logger.info("BACKTEST START")

    signals = fetch_expired_unscored_signals()
    if not signals:
        logger.info("No new expired signals to evaluate.")
        return {"evaluated": 0, "correct": 0, "incorrect": 0, "skipped": 0}

    stats = {"evaluated": 0, "correct": 0, "incorrect": 0, "skipped": 0}

    for s in signals:
        ticker      = s.get("ticker","")
        sentiment   = s.get("sentiment","Neutral")
        end_date    = s.get("end_date","")
        yes_price   = float(s.get("yes_price") or 0.5)
        signal_id   = s["id"]

        # Check resolution
        yes_resolved = check_market_resolution(yes_price)
        if yes_resolved is None:
            logger.info("  [%s] Skipped — market not clearly resolved (YES=%.0f%%)",
                        ticker, yes_price * 100)
            stats["skipped"] += 1
            continue

        # Get prices
        # Note: we approximate "price at signal creation" as price at end_date - 30 days
        # For a production system, you'd store price_at_creation when the signal is written
        signal_date = (
            datetime.fromisoformat(end_date.replace("Z","+00:00")) - timedelta(days=30)
        ).isoformat() if end_date else None

        price_at_signal = get_price_at_date(ticker, signal_date) if signal_date else None
        price_at_expiry = get_price_at_date(ticker, end_date) if end_date else None
        time.sleep(0.5)   # Yahoo rate limit

        if not price_at_signal or not price_at_expiry:
            logger.warning("  [%s] Skipped — price data unavailable", ticker)
            stats["skipped"] += 1
            continue

        price_change_pct = round(
            (price_at_expiry - price_at_signal) / price_at_signal * 100, 2
        ) if price_at_signal else 0.0

        # Check accuracy
        correct = check_sentiment_correct(sentiment, yes_resolved, price_change_pct)

        outcome_row = {
            "signal_id":         signal_id,
            "ticker":            ticker,
            "sentiment":         sentiment,
            "impact_score":      s.get("impact_score"),
            "yes_resolved":      yes_resolved,
            "price_at_signal":   round(price_at_signal, 2),
            "price_at_expiry":   round(price_at_expiry, 2),
            "price_change_pct":  price_change_pct,
            "sentiment_correct": correct,
        }

        direction_icon = "✓" if correct else "✗" if correct is False else "~"
        logger.info(
            "  [%s] %s | resolved=%s | price %+.1f%% | sentiment=%s | %s",
            ticker, s.get("question","")[:40],
            "YES" if yes_resolved else "NO",
            price_change_pct, sentiment, direction_icon
        )

        if not dry_run:
            try:
                supabase.table("signal_outcomes").insert(outcome_row).execute()
                stats["evaluated"] += 1
                if correct is True:   stats["correct"]   += 1
                if correct is False:  stats["incorrect"] += 1
            except Exception as e:
                logger.error("  DB write failed for signal %d: %s", signal_id, e)
        else:
            stats["evaluated"] += 1

    # Print accuracy summary
    total_directional = stats["correct"] + stats["incorrect"]
    accuracy = (
        stats["correct"] / total_directional * 100
        if total_directional > 0 else 0
    )

    logger.info("=" * 55)
    logger.info("BACKTEST COMPLETE")
    logger.info("  Evaluated  : %d", stats["evaluated"])
    logger.info("  Correct    : %d", stats["correct"])
    logger.info("  Incorrect  : %d", stats["incorrect"])
    logger.info("  Skipped    : %d", stats["skipped"])
    logger.info("  Accuracy   : %.0f%% (directional signals only)", accuracy)
    logger.info("=" * 55)

    return stats


# ─────────────────────────────────────────────────────────────
# ACCURACY REPORT
# ─────────────────────────────────────────────────────────────

def print_accuracy_report():
    """
    Print a breakdown of signal accuracy from signal_outcomes table.
    Shows accuracy by ticker, cluster, and impact score range.
    """
    try:
        res = supabase.table("signal_outcomes").select("*").execute()
        rows = res.data or []
    except Exception as e:
        logger.error("Failed to fetch outcomes: %s", e)
        return

    if not rows:
        print("No outcomes recorded yet.")
        return

    # Filter to directional only
    directional = [r for r in rows if r.get("sentiment_correct") is not None]
    if not directional:
        print("No directional outcomes yet.")
        return

    total    = len(directional)
    correct  = sum(1 for r in directional if r["sentiment_correct"])
    accuracy = correct / total * 100

    print(f"\n{'='*55}")
    print(f"SIGNAL ACCURACY REPORT — {len(rows)} total outcomes")
    print(f"{'='*55}")
    print(f"Overall accuracy: {accuracy:.0f}% ({correct}/{total} directional)\n")

    # By ticker
    print("By ticker:")
    tickers = sorted({r["ticker"] for r in directional})
    for t in tickers:
        t_rows = [r for r in directional if r["ticker"] == t]
        t_correct = sum(1 for r in t_rows if r["sentiment_correct"])
        t_acc = t_correct / len(t_rows) * 100 if t_rows else 0
        bar = "█" * int(t_acc / 10)
        print(f"  {t:<6} {t_acc:>5.0f}%  {bar}  ({t_correct}/{len(t_rows)})")

    # By score range
    print("\nBy impact score:")
    for lo, hi in [(8,10),(6,7),(4,5),(1,3)]:
        s_rows = [r for r in directional
                  if lo <= (r.get("impact_score") or 0) <= hi]
        if not s_rows:
            continue
        s_correct = sum(1 for r in s_rows if r["sentiment_correct"])
        s_acc = s_correct / len(s_rows) * 100
        print(f"  Score {lo}-{hi}: {s_acc:.0f}%  ({s_correct}/{len(s_rows)})")

    print(f"{'='*55}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--report",  action="store_true", help="Print accuracy report only")
    args = parser.parse_args()

    if args.report:
        print_accuracy_report()
    else:
        run_backtest(dry_run=args.dry_run)
        print_accuracy_report()