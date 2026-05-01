"""
scheduler.py

Automated pipeline scheduler for BIT Capital Signal Scanner.
Runs the full pipeline on a configurable cron schedule.

Pipeline schedule:
  Every 6 hours  → full ingest + filter + report
  Every 30 min   → stock prices refresh (handled in streamlit)
  On demand      → dig deeper (triggered by UI button click)

Usage:
  python scheduler.py               # starts the scheduler (runs forever)
  python scheduler.py --once        # run once immediately then exit
  python scheduler.py --interval 4  # run every 4 hours

How it works:
  Uses Python's built-in sched module (no external dependencies).
  Each run calls run_pipeline() which handles ingest → stage1 → stage2 → report.
  Errors in one run do not stop future runs.
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import sched
import time
import logging
import argparse
import traceback
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Default schedule ──────────────────────────────────────────
DEFAULT_INTERVAL_HOURS = 6       # how often to run the full pipeline
MAX_EVENTS             = 3000    # how many Polymarket events to ingest per run


def run_once():
    """Runs the full pipeline once. Catches all errors."""
    logger.info("=" * 60)
    logger.info("SCHEDULED RUN — %s", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    logger.info("=" * 60)

    try:
        # Import here so errors in pipeline files don't crash the scheduler
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from pipeline.run_pipeline import run_pipeline

        result = run_pipeline(
            max_events=MAX_EVENTS,
            dry_run=False,
            skip_report=False,
        )

        logger.info(
            "Run complete — status=%s | signals=%d | duration=%.1fs",
            result.get("status"),
            result.get("stage2_signals", 0),
            result.get("duration_s", 0),
        )
        return result

    except Exception as e:
        logger.error("Pipeline run failed: %s", e)
        logger.error(traceback.format_exc())
        return {"status": "error", "error": str(e)}


def schedule_loop(interval_hours: float):
    """
    Runs the pipeline immediately, then on a fixed interval.
    Uses Python's sched module — no external packages needed.
    """
    scheduler    = sched.scheduler(time.time, time.sleep)
    interval_sec = interval_hours * 3600

    def scheduled_run(sc):
        run_once()
        # Schedule next run
        next_run = datetime.now(timezone.utc)
        logger.info(
            "Next run scheduled in %.1f hours (at ~%s UTC)",
            interval_hours,
            next_run.strftime("%H:%M"),
        )
        sc.enter(interval_sec, 1, scheduled_run, (sc,))

    logger.info(
        "Scheduler started — interval: %.1f hours | first run: now",
        interval_hours,
    )

    # Run immediately on start, then every interval_hours
    scheduler.enter(0, 1, scheduled_run, (scheduler,))
    scheduler.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BIT Capital Pipeline Scheduler")
    parser.add_argument(
        "--interval", type=float, default=DEFAULT_INTERVAL_HOURS,
        help=f"Hours between pipeline runs (default: {DEFAULT_INTERVAL_HOURS})",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Run once immediately and exit",
    )
    args = parser.parse_args()

    if args.once:
        result = run_once()
        sys.exit(0 if result.get("status") == "success" else 1)
    else:
        schedule_loop(interval_hours=args.interval)