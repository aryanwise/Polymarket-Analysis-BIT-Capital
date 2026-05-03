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

    # 5: Deduplicate by event_id — keep only highest-volume market per event
    # Prevents 20+ Fed rate threshold variants all hitting Stage 2 as separate signals
    b = len(df)

    def _signal_quality(yes, vol):
        uncertainty = 1 - abs(float(yes or 0.5) - 0.5) * 2
        vol_score   = min(float(vol or 0) / 1_000_000, 1.0)
        return uncertainty * 0.65 + vol_score * 0.35

    df["_sq"] = df.apply(
        lambda r: _signal_quality(r["_yes"], r["volume"]), axis=1
    )
    df = (
        df.sort_values("_sq", ascending=False)
          .drop_duplicates(subset=["event_id"], keep="first")
          .drop(columns=["_sq"])
          .reset_index(drop=True)
    )
    stats["deduped"] = b - len(df)
    logger.info("[5]  dedup by event_id (signal quality) removed: %d | remaining: %d",
                stats["deduped"], len(df))

    # 6: Minimum volume floor — cut very low liquidity markets
    # Low volume = unreliable probability signal, not worth LLM call
    MIN_VOLUME = 5_000
    b = len(df)
    df = df[df["volume"] >= MIN_VOLUME]
    stats["low_volume"] = b - len(df)
    logger.info("[6]  low_volume (<$5k) removed: %d | remaining: %d", stats["low_volume"], len(df))

    # Drop helper columns
    df = df.drop(columns=["_yes", "_year", "_tags"])

    stats["total_removed"] = initial - len(df)
    stats["total_kept"]    = len(df)
    stats["keep_pct"]      = round(len(df) / initial * 100, 1) if initial else 0

    logger.info(
        "STAGE 1 COMPLETE — kept: %d / %d (%.1f%%) | deduped: %d | low_vol: %d",
        len(df), initial, stats["keep_pct"],
        stats.get("deduped", 0), stats.get("low_volume", 0),
    )

    return df, stats


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    from datetime import datetime, timezone
    from pipeline.extract import run_ingest

    # Run ingest
    print("\n--- INGEST ---")
    df_raw = run_ingest(max_events=3000)
    print(f"Ingest complete: {len(df_raw)} rows")

    # Run stage 1
    print("\n--- STAGE 1 ---")
    df_filtered, stats = run_stage1(df_raw)
    print(f"\nStats: {stats}")

    # Export debug Excel
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    filename = f"debug/stage1_debug_{ts}.xlsx"

    import os
    os.makedirs("debug", exist_ok=True)

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        # Summary sheet
        import pandas as pd
        summary = pd.DataFrame([
            {"Stage": "Ingest (raw)",       "Rows": len(df_raw),      "Notes": "All active Polymarket markets"},
            {"Stage": "Stage 1 (filtered)", "Rows": len(df_filtered), "Notes": str(stats)},
        ])
        summary.to_excel(writer, sheet_name="Summary", index=False)

        # Raw ingest
        df_raw.head(5000).to_excel(writer, sheet_name="01_Ingest_Raw", index=False)

        # Stage 1 output
        df_filtered.to_excel(writer, sheet_name="02_Stage1_Filtered", index=False)

    print(f"\nDebug file saved: {filename}")