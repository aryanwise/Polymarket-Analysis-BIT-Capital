"""
stage1_filter.py

Standalone Stage 1 filtering for Polymarket data.
Removes rows based on:
  - Irrelevant tags (from irrelevant_tags.py)
  - Year < 2026
  - Probability = 0 or 1 (fully resolved)
  - Probability < 4% or > 96% (near-certain)

Usage:
  python stage1_filter.py --input polymarket_full_lake_v6.csv --output stage1_output.csv
  python stage1_filter.py --input polymarket_full_lake_v6.csv --sample 1000  # test on sample
"""

import re
import ast
import argparse
import pandas as pd
from irrelevant_tags import IRRELEVANT_TAGS  # Import your tag blocklist

# Configuration
MIN_YEAR = 2026
MIN_PROB = 0.04  # 4%
MAX_PROB = 0.96  # 96%


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def parse_tags(t):
    """Convert comma-separated tags string into a set of lowercase tags."""
    if pd.isna(t):
        return set()
    return {x.strip().lower() for x in str(t).split(",") if x.strip()}


def get_yes_probability(p):
    """
    Extract YES probability from various possible formats:
    - Direct float: 0.45
    - List string: "['0.45', '0.55']"
    - Single value: "0.45"
    """
    if pd.isna(p):
        return None
    
    # Try direct float conversion
    try:
        return float(p)
    except (ValueError, TypeError):
        pass
    
    # Try parsing as list (e.g., "['0.0145', '0.9855']")
    try:
        v = ast.literal_eval(str(p))
        if isinstance(v, (list, tuple)) and len(v) > 0:
            return float(v[0])  # First element is YES probability
    except (ValueError, SyntaxError, TypeError):
        pass
    
    # Try extracting first number using regex
    match = re.search(r"[\d.]+", str(p))
    if match:
        return float(match.group())
    
    return None


def get_end_date_year(d):
    """Extract year from end_date string (e.g., '2026-06-30T12:00:00Z' -> 2026)"""
    if pd.isna(d):
        return None
    match = re.search(r"(\d{4})", str(d))
    return int(match.group(1)) if match else None


def has_irrelevant_tag(tags_set, irrelevant_tags):
    """Check if any tag in the row matches the irrelevant tags list."""
    if not tags_set:
        return False
    return bool(tags_set & irrelevant_tags)  # Intersection check


# ============================================================
# MAIN FILTERING FUNCTION
# ============================================================

def filter_stage1(df, verbose=True):
    """
    Apply Stage 1 filters to DataFrame.
    
    Filters applied in order:
    1. Remove expired markets (end_date year < MIN_YEAR)
    2. Remove fully resolved (YES probability = 0 or 1)
    3. Remove near-certain (<4% or >96%)
    4. Remove rows with irrelevant tags
    
    Returns filtered DataFrame and statistics dict.
    """
    original_count = len(df)
    df = df.copy()
    stats = {}
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"  STAGE 1 FILTERING - BIT Capital")
        print(f"  Input rows: {original_count:,}")
        print(f"{'='*60}\n")
    
    # Add helper columns
    df['_yes'] = df['prices'].apply(get_yes_probability)
    df['_year'] = df['end_date'].apply(get_end_date_year)
    df['_tags_set'] = df['tags'].apply(parse_tags)
    
    # --------------------------------------------------------
    # Filter 1a: Remove expired markets (year < MIN_YEAR)
    # --------------------------------------------------------
    before = len(df)
    df = df[(df['_year'].isna()) | (df['_year'] >= MIN_YEAR)]
    removed = before - len(df)
    stats['expired'] = removed
    if verbose:
        print(f"  [1a] Expired (pre-{MIN_YEAR})          : -{removed:>6,} | Remaining: {len(df):,}")
    
    # --------------------------------------------------------
    # Filter 1b: Remove fully resolved (YES = 0 or 1)
    # --------------------------------------------------------
    before = len(df)
    df = df[(df['_yes'].isna()) | ((df['_yes'] > 0.0) & (df['_yes'] < 1.0))]
    removed = before - len(df)
    stats['fully_resolved'] = removed
    if verbose:
        print(f"  [1b] Fully resolved (YES=0 or 1)   : -{removed:>6,} | Remaining: {len(df):,}")
    
    # --------------------------------------------------------
    # Filter 1c: Remove near-certain (< MIN_PROB or > MAX_PROB)
    # --------------------------------------------------------
    before = len(df)
    df = df[(df['_yes'].isna()) | ((df['_yes'] >= MIN_PROB) & (df['_yes'] <= MAX_PROB))]
    removed = before - len(df)
    stats['near_certain'] = removed
    if verbose:
        print(f"  [1c] Near-certain (<{MIN_PROB:.0%} or >{MAX_PROB:.0%})  : -{removed:>6,} | Remaining: {len(df):,}")
    
    # --------------------------------------------------------
    # Filter 1d: Remove rows with irrelevant tags
    # --------------------------------------------------------
    before = len(df)
    # Convert IRRELEVANT_TAGS to lowercase for case-insensitive matching
    irrelevant_lower = {tag.lower() for tag in IRRELEVANT_TAGS}
    df = df[~df['_tags_set'].apply(lambda tags: has_irrelevant_tag(tags, irrelevant_lower))]
    removed = before - len(df)
    stats['irrelevant_tags'] = removed
    if verbose:
        print(f"  [1d] Contains irrelevant tag       : -{removed:>6,} | Remaining: {len(df):,}")
    
    # --------------------------------------------------------
    # Clean up temporary columns
    # --------------------------------------------------------
    df = df.drop(columns=['_yes', '_year', '_tags_set'])
    
    total_removed = original_count - len(df)
    stats['total_removed'] = total_removed
    stats['total_kept'] = len(df)
    stats['keep_percentage'] = (len(df) / original_count) * 100
    
    if verbose:
        print(f"\n  {'='*40}")
        print(f"  SUMMARY")
        print(f"  {'='*40}")
        print(f"  Kept      : {len(df):,} rows ({stats['keep_percentage']:.1f}%)")
        print(f"  Removed   : {total_removed:,} rows ({100-stats['keep_percentage']:.1f}%)")
        print(f"    - Expired (year): {stats['expired']:,}")
        print(f"    - Fully resolved: {stats['fully_resolved']:,}")
        print(f"    - Near-certain  : {stats['near_certain']:,}")
        print(f"    - Irrelevant tags: {stats['irrelevant_tags']:,}")
        print(f"{'='*60}\n")
    
    return df, stats


# ============================================================
# REPORTING FUNCTION
# ============================================================

def print_filtered_samples(df, n=10):
    """Print sample of filtered (kept) rows for inspection."""
    if len(df) == 0:
        print("  No rows remaining after filtering.")
        return
    
    print(f"\n  📊 SAMPLE KEPT MARKETS (first {min(n, len(df))}):")
    print(f"  {'='*70}")
    
    for i, (_, row) in enumerate(df.head(n).iterrows()):
        question = row.get('question', 'N/A')
        if len(question) > 60:
            question = question[:57] + "..."
        
        tags = row.get('tags', 'N/A')
        if len(tags) > 40:
            tags = tags[:37] + "..."
        
        print(f"  {i+1}. {question}")
        print(f"     Tags: {tags}")
        print(f"     Date: {row.get('end_date', 'N/A')[:10] if pd.notna(row.get('end_date')) else 'N/A'}")
        print()


def print_removed_tags_frequency(df_original, df_filtered):
    """Show which irrelevant tags were most common in removed rows."""
    # This requires running filter without actually removing to analyze
    # Alternative: we can show what tags were removed
    pass


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Stage 1 Filter for Polymarket Data")
    parser.add_argument("--input", "-i", default="polymarket_full_lake_v6.csv",
                        help="Input CSV file path")
    parser.add_argument("--output", "-o", default="polymarket_stage1.csv",
                        help="Output CSV file path")
    parser.add_argument("--sample", "-s", type=int, default=None,
                        help="Process only first N rows (for testing)")
    parser.add_argument("--verbose", "-v", action="store_true", default=True,
                        help="Print detailed statistics")
    parser.add_argument("--show-samples", action="store_true", default=True,
                        help="Show sample of kept rows")
    
    args = parser.parse_args()
    
    # Load data
    print(f"\n  Loading data from: {args.input}")
    if args.sample:
        df = pd.read_csv(args.input).head(args.sample)
        print(f"  Using sample: {args.sample} rows")
    else:
        df = pd.read_csv(args.input)
    
    print(f"  Loaded: {len(df):,} rows")
    print(f"  Columns: {df.columns.tolist()}")
    
    # Check required columns exist
    required_cols = ['prices', 'end_date', 'tags', 'question']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"\n  ❌ ERROR: Missing required columns: {missing}")
        print(f"  Available columns: {df.columns.tolist()}")
        return
    
    # Run Stage 1 filter
    df_filtered, stats = filter_stage1(df, verbose=args.verbose)
    
    # Save output
    df_filtered.to_csv(args.output, index=False)
    print(f"  💾 Saved filtered data to: {args.output}")
    
    # Show samples
    if args.show_samples and len(df_filtered) > 0:
        print_filtered_samples(df_filtered, n=10)
    
    # Return for potential chaining
    return df_filtered, stats


if __name__ == "__main__":
    df_result, stats = main()
    
    # Optional: Quick analysis of what was kept
    print("\n  🎯 Next Steps:")
    print("    1. Review the sample kept markets above")
    print("    2. If satisfied, run Stage 2 with Gemini:")
    print("       python filter_pipeline.py --stage2 gemini")
    print("    3. If too many/few kept, adjust IRRELEVANT_TAGS in irrelevant_tags.py")