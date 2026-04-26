"""
pipeline/scheduler.py

Simple scheduler for local demo.
Runs ingestion every 6 hours, filtering every 2 hours, reports daily.
"""
import schedule
import time
import subprocess
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

def run_ingestion():
    logger.info("=" * 60)
    logger.info(f"[{datetime.now()}] Running market ingestion...")
    subprocess.run(["python", "pipeline/ingest.py"])

def run_filter():
    logger.info("=" * 60)
    logger.info(f"[{datetime.now()}] Running LLM filter pipeline...")
    subprocess.run(["python", "pipeline/filter.py"])

def run_report():
    logger.info("=" * 60)
    logger.info(f"[{datetime.now()}] Generating daily report...")
    subprocess.run(["python", "pipeline/report_generator.py"])

# Schedule
schedule.every(6).hours.do(run_ingestion)
schedule.every(2).hours.do(run_filter)
schedule.every().day.at("08:00").do(run_report)

logger.info("Scheduler started. Press Ctrl+C to stop.")
logger.info("  - Ingestion: every 6 hours")
logger.info("  - Filter:    every 2 hours")
logger.info("  - Report:    daily at 08:00")

# Run immediately on startup
run_ingestion()
run_filter()
run_report()

while True:
    schedule.run_pending()
    time.sleep(60)