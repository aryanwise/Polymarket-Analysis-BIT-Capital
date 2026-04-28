import sys
import os

# Add project root to path so utils/ can be found
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import yfinance as yf
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

from utils.supabase_client import get_service_client
supabase = get_service_client()


# Must match exactly what's in your stocks table
TRACKED_TICKERS = [
    "IREN", "MSFT", "GOOGL", "LMND", "RDDT",
    "MU",   "TSM",  "HUT",   "HOOD", "DDOG"
]


# ── Fetch from Yahoo Finance ──────────────────────────────────

def fetch_prices() -> list[dict]:
    """
    Fetch the latest price snapshot for all tracked tickers.
    Uses yfinance fast_info for a single lightweight API call per ticker.
    Returns a list of rows ready to insert into stock_prices table.
    """
    rows      = []
    errors    = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    print(f"\n{'='*55}")
    print(f"  STOCK PRICE FETCH — {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*55}")

    for ticker in TRACKED_TICKERS:
        try:
            stock      = yf.Ticker(ticker)
            info       = stock.fast_info   # lightweight — no full info call

            price      = round(float(info.last_price),       4)
            prev_close = round(float(info.previous_close),   4)
            change_pct = round(((price - prev_close) / prev_close) * 100, 4) if prev_close else 0.0
            volume     = int(info.three_month_average_volume or 0)
            market_cap = float(info.market_cap or 0)

            row = {
                "ticker":     ticker,
                "price":      price,
                "change_pct": change_pct,
                "volume":     volume,
                "source":     "yahoo_finance",
                "fetched_at": fetched_at,
            }
            rows.append(row)

            arrow = "▲" if change_pct >= 0 else "▼"
            print(f"  {ticker:<6} | ${price:>10.2f} | {arrow} {change_pct:+.2f}%")

        except Exception as e:
            print(f"  {ticker:<6} | ERROR: {e}")
            errors.append(ticker)

    print(f"{'='*55}")
    print(f"  Fetched: {len(rows)}/{len(TRACKED_TICKERS)} | Failed: {len(errors)}")
    if errors:
        print(f"  Failed: {errors}")

    return rows


# ── Save to Supabase ──────────────────────────────────────────

def save_prices(rows: list[dict]) -> int:
    """
    Insert price snapshots into stock_prices table.
    We always INSERT (not upsert) to keep a full price history.
    Returns number of rows saved.
    """
    if not rows:
        return 0

    saved = 0
    for row in rows:
        try:
            supabase.table("stock_prices").insert(row).execute()
            saved += 1
        except Exception as e:
            print(f"  Failed to save {row['ticker']}: {e}")

    print(f"\n  Saved {saved} price snapshots to stock_prices table")
    return saved


# ── Get latest prices (for frontend fallback) ─────────────────

def get_latest_prices() -> list[dict]:
    """
    Fetch the most recent price row per ticker from DB.
    Used as a fallback when Yahoo Finance is unavailable.
    """
    result = []
    for ticker in TRACKED_TICKERS:
        res = (
            supabase.table("stock_prices")
            .select("ticker, price, change_pct, fetched_at")
            .eq("ticker", ticker)
            .order("fetched_at", desc=True)
            .limit(1)
            .execute()
        )
        if res.data:
            result.append(res.data[0])

    return result


# ── Main pipeline ─────────────────────────────────────────────

def run_price_pipeline():
    """Fetch live prices and save to DB."""
    rows  = fetch_prices()
    saved = save_prices(rows)
    return saved


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    run_price_pipeline()

    # Verify what was saved
    print("\n  Latest prices from DB:")
    print(f"  {'TICKER':<8} {'PRICE':>10}  {'CHANGE':>8}  {'FETCHED AT'}")
    print(f"  {'-'*55}")
    for row in get_latest_prices():
        arrow = "▲" if float(row['change_pct'] or 0) >= 0 else "▼"
        print(
            f"  {row['ticker']:<8} "
            f"${float(row['price']):>9.2f}  "
            f"{arrow} {float(row['change_pct'] or 0):>+.2f}%  "
            f"{row['fetched_at'][:19]}"
        )