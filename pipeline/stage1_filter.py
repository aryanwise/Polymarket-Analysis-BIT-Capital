"""
pipeline/stage1_filter.py

Stage 1: Rule-based filter. Fast, free, no LLM.
Removes markets that can never be BIT Capital equity signals.

Filters applied (in order):
  0a. Volume = 0 or missing
  0b. Missing YES price
  1.  Expired (end_date year < 2026)
  2.  Fully resolved (YES = 0.0 or 1.0 exactly)
  3.  Near-certain (YES < 4% or > 96%)
  4.  Contains any irrelevant tag (from irrelevant_tags.py)

Called by scheduler.py and run_pipeline.py
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import re
import ast
import logging
import pandas as pd
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

MIN_YEAR = datetime.now(timezone.utc).year
MIN_PROB = 0.04
MAX_PROB = 0.96

# Import tag blocklist
try:
    from irrelevant_tags import IRRELEVANT_TAGS
    IRRELEVANT_LOWER = {t.lower() for t in IRRELEVANT_TAGS}
except ImportError:
    logger.warning("irrelevant_tags.py not found — tag filter disabled")
    IRRELEVANT_LOWER = set()


# ── Helpers ───────────────────────────────────────────────────

def _get_yes(p) -> float | None:
    if p is None or (isinstance(p, float) and pd.isna(p)):
        return None
    try:
        return float(p)
    except (TypeError, ValueError):
        pass
    try:
        v = ast.literal_eval(str(p))
        if isinstance(v, (list, tuple)) and v:
            return float(v[0])
    except Exception:
        pass
    m = re.search(r"[\d.]+", str(p))
    return float(m.group()) if m else None


def _get_year(d) -> int | None:
    if not d or (isinstance(d, float) and pd.isna(d)):
        return None
    m = re.search(r"(\d{4})", str(d))
    return int(m.group(1)) if m else None


def _parse_tags(t) -> set:
    if not t or (isinstance(t, float) and pd.isna(t)):
        return set()
    return {x.strip().lower() for x in str(t).split(",") if x.strip()}


def _has_irrelevant_tag(tags: set) -> bool:
    return bool(tags & IRRELEVANT_LOWER)


# ── Main filter ───────────────────────────────────────────────

def run_stage1(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Applies Stage 1 filters to raw ingest DataFrame.
    Returns (filtered_df, stats_dict).
    """
    logger.info("STAGE 1 START — input: %d rows", len(df))
    initial = len(df)
    df      = df.copy()
    stats   = {}

    # Pre-compute helper columns
    df["_yes"]  = df["yes_price"].apply(_get_yes)
    df["_year"] = df["end_date"].apply(_get_year)
    df["_tags"] = df["tags"].apply(_parse_tags)

    # 0a: Volume = 0 or missing
    b = len(df)
    df = df[df["volume"].fillna(0) > 0]
    stats["zero_volume"] = b - len(df)
    logger.info("[0a] zero_volume removed: %d | remaining: %d", stats["zero_volume"], len(df))

    # 0b: Missing YES price
    b = len(df)
    df = df[df["_yes"].notna()]
    stats["missing_prices"] = b - len(df)
    logger.info("[0b] missing_prices removed: %d | remaining: %d", stats["missing_prices"], len(df))

    # 1: Expired
    b = len(df)
    df = df[(df["_year"].isna()) | (df["_year"] >= MIN_YEAR)]
    stats["expired"] = b - len(df)
    logger.info("[1]  expired removed: %d | remaining: %d", stats["expired"], len(df))

    # 2: Fully resolved
    b = len(df)
    df = df[(df["_yes"] > 0.0) & (df["_yes"] < 1.0)]
    stats["fully_resolved"] = b - len(df)
    logger.info("[2]  fully_resolved removed: %d | remaining: %d", stats["fully_resolved"], len(df))

    # 3: Near-certain
    b = len(df)
    df = df[(df["_yes"] >= MIN_PROB) & (df["_yes"] <= MAX_PROB)]
    stats["near_certain"] = b - len(df)
    logger.info("[3]  near_certain removed: %d | remaining: %d", stats["near_certain"], len(df))

    # 4: Irrelevant tags
    b = len(df)
    df = df[~df["_tags"].apply(_has_irrelevant_tag)]
    stats["irrelevant_tags"] = b - len(df)
    logger.info("[4]  irrelevant_tags removed: %d | remaining: %d", stats["irrelevant_tags"], len(df))

    # Drop helper columns
    df = df.drop(columns=["_yes", "_year", "_tags"])

    stats["total_removed"] = initial - len(df)
    stats["total_kept"]    = len(df)
    stats["keep_pct"]      = round(len(df) / initial * 100, 1) if initial else 0

    logger.info(
        "STAGE 1 COMPLETE — kept: %d / %d (%.1f%%)",
        len(df), initial, stats["keep_pct"],
    )
    return df, stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # Quick test
    from ingest import run_ingest
    raw = run_ingest(max_events=500)
    filtered, stats = run_stage1(raw)
    print(stats)
    print(filtered.head())