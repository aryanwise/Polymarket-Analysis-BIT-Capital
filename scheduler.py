"""
scheduler.py

BIT Capital Pipeline Scheduler.
Lives at project root. Runs the full pipeline on a fixed interval.

Pipeline triggered every 6h:
  ingest → stage1_filter → stage2_filter → report_generator

Stock prices: refreshed live by Streamlit when Holdings tab loads.
Dig deeper:   triggered on demand by the Analyse button in the UI.

Usage:
  uv run scheduler.py                  # run every 6 hours (default)
  uv run scheduler.py --interval 4     # run every 4 hours
  uv run scheduler.py --once           # run once and exit
  uv run scheduler.py --dry-run        # test without DB writes

Stop with Ctrl+C — current run completes cleanly before exit.
"""

import sys
import os

# Project root is this file's directory
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "pipeline"))

import signal
import sched
import time
import logging
import argparse
import traceback
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
DEFAULT_INTERVAL_HOURS = 6
MAX_EVENTS             = 3000

# ── State ─────────────────────────────────────────────────────
_shutdown_requested = False
_run_history: list[dict] = []


# ── Graceful shutdown ─────────────────────────────────────────
def _handle_shutdown(signum, frame):
    global _shutdown_requested
    logger.info("Shutdown signal received — will stop after current run completes.")
    _shutdown_requested = True


signal.signal(signal.SIGINT,  _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


# ── Single run ────────────────────────────────────────────────
def run_once(dry_run: bool = False) -> dict:
    """
    Executes the full pipeline once.
    Catches all exceptions so the scheduler never crashes.
    """
    run_ts = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info("SCHEDULED RUN — %s", run_ts.strftime("%Y-%m-%d %H:%M UTC"))
    if dry_run:
        logger.info("MODE: DRY RUN (no DB writes)")
    logger.info("=" * 60)

    result = {"status": "error", "run_ts": run_ts.isoformat()}

    try:
        from pipeline import run_pipeline

        result = run_pipeline(
            max_events=args.max_events,
            dry_run=dry_run,
            skip_report=False,
        )

        logger.info(
            "Run complete — status=%s | ingest=%d | stage1=%d | signals=%d | duration=%.1fs",
            result.get("status", "?"),
            result.get("ingest_rows", 0),
            result.get("stage1_rows", 0),
            result.get("stage2_signals", 0),
            result.get("duration_s", 0),
        )

    except Exception as e:
        logger.error("Pipeline run failed: %s", e)
        logger.error(traceback.format_exc())
        result["error"] = str(e)

    _run_history.append(result)
    return result


# ── Scheduler loop ────────────────────────────────────────────
def schedule_loop(interval_hours: float, dry_run: bool = False):
    """
    Runs the pipeline immediately, then on a fixed interval.
    Uses Python's built-in sched — no external dependencies.
    Respects shutdown signals between runs.
    """
    scheduler    = sched.scheduler(time.time, time.sleep)
    interval_sec = interval_hours * 3600

    def scheduled_run(sc):
        if _shutdown_requested:
            logger.info("Shutdown requested — exiting scheduler loop.")
            return

        run_once(dry_run=dry_run)

        if _shutdown_requested:
            logger.info("Shutdown requested — not scheduling next run.")
            return

        next_run_dt = datetime.now(timezone.utc) + timedelta(seconds=interval_sec)
        logger.info(
            "Next run in %.1f hours — at %s UTC",
            interval_hours,
            next_run_dt.strftime("%H:%M"),
        )
        sc.enter(interval_sec, 1, scheduled_run, (sc,))

    logger.info("=" * 60)
    logger.info("BIT CAPITAL SCHEDULER STARTED")
    logger.info("  Interval   : every %.1f hours", interval_hours)
    logger.info("  Max events : %d", MAX_EVENTS)
    logger.info("  Dry run    : %s", dry_run)
    logger.info("  First run  : now")
    logger.info("  Stop with  : Ctrl+C")
    logger.info("=" * 60)

    scheduler.enter(0, 1, scheduled_run, (scheduler,))

    try:
        scheduler.run()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt — scheduler stopped.")

    _print_run_summary()


# ── Run summary ───────────────────────────────────────────────
def _print_run_summary():
    if not _run_history:
        return

    logger.info("\n%s", "=" * 60)
    logger.info("SESSION SUMMARY — %d run(s)", len(_run_history))
    logger.info("%s", "=" * 60)

    for i, r in enumerate(_run_history, 1):
        ts     = r.get("run_ts", "?")[:16].replace("T", " ")
        status = r.get("status", "error")
        sigs   = r.get("stage2_signals", 0)
        dur    = r.get("duration_s", 0)
        icon   = "✓" if status == "success" else "✗"
        logger.info("  Run %d: %s %s | signals=%d | %.1fs", i, icon, ts, sigs, dur)

    logger.info("=" * 60)


# ── Entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="BIT Capital Pipeline Scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run scheduler.py                 # default 6h interval
  uv run scheduler.py --interval 4    # every 4 hours
  uv run scheduler.py --once          # one run then exit
  uv run scheduler.py --once --dry-run  # test without DB writes
        """,
    )
    parser.add_argument(
        "--interval", type=float, default=DEFAULT_INTERVAL_HOURS,
        help=f"Hours between runs (default: {DEFAULT_INTERVAL_HOURS})",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Run once immediately and exit",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run pipeline without writing to DB",
    )
    parser.add_argument(
        "--max-events", type=int, default=MAX_EVENTS,
        help=f"Max Polymarket events to ingest (default: {MAX_EVENTS})",
    )
    args = parser.parse_args()

    if args.once:
        result = run_once(dry_run=args.dry_run)
        _print_run_summary()
        sys.exit(0 if result.get("status") == "success" else 1)
    else:
        schedule_loop(interval_hours=args.interval, dry_run=args.dry_run)