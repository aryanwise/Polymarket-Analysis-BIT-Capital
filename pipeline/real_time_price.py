"""
pipeline/prices.py  (renamed from real_time_price.py)

Price enrichment for BIT Capital holdings.

Two data sources:
  Yahoo Finance — live price, 1D change, 5D change, 52-week range
  FMP           — analyst price targets, sector, additional context

fetch_prices_for_report() → dict keyed by ticker, used by report_generator.py
fetch_prices()            → list of rows for Streamlit dashboard display
run_price_pipeline()      → save snapshot to stock_prices table
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import time
import requests
import yfinance as yf
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

from utils.supabase_client import get_service_client
supabase = get_service_client()

FMP_API_KEY = os.environ.get("FMP_API_KEY", "")
FMP_BASE    = "https://financialmodelingprep.com/api/v3"

# All 14 BIT Capital holdings
TRACKED_TICKERS = [
    "IREN", "HUT",  "COIN",
    "NVDA", "TSM",  "MU",
    "MSFT", "GOOGL","AMZN", "META", "DDOG",
    "HOOD", "LMND", "RDDT",
]

# Thresholds for report context labels
MOVE_THRESHOLDS = [
    (10,  "surging"),
    (5,   "up strongly"),
    (2,   "up"),
    (-2,  "flat"),
    (-5,  "down"),
    (-10, "down sharply"),
]

def _move_label(chg: float) -> str:
    for threshold, label in MOVE_THRESHOLDS:
        if chg >= threshold:
            return label
    return "down sharply"

def _52w_position(price: float, low: float, high: float) -> str:
    """Where in the 52-week range is the current price."""
    if high <= low:
        return "—"
    pct = (price - low) / (high - low) * 100
    if pct >= 80:   return f"{pct:.0f}% of 52w range — near highs"
    if pct >= 50:   return f"{pct:.0f}% of 52w range — upper half"
    if pct >= 20:   return f"{pct:.0f}% of 52w range — lower half"
    return         f"{pct:.0f}% of 52w range — near lows"


# ─────────────────────────────────────────────────────────────
# YAHOO FINANCE
# ─────────────────────────────────────────────────────────────

def _fetch_yahoo(ticker: str) -> dict | None:
    """
    Fetch price + performance data from Yahoo Finance.
    Returns dict with price, chg_1d, chg_5d, high_52w, low_52w.
    """
    try:
        stock   = yf.Ticker(ticker)
        info    = stock.fast_info

        price      = float(info.last_price or 0)
        prev_close = float(info.previous_close or price)
        chg_1d     = round(((price - prev_close) / prev_close) * 100, 2) if prev_close else 0.0
        high_52w   = float(info.year_high or 0)
        low_52w    = float(info.year_low or 0)

        # 5D change from history
        hist   = stock.history(period="6d", interval="1d")
        chg_5d = 0.0
        if len(hist) >= 2:
            price_5d_ago = float(hist["Close"].iloc[0])
            chg_5d = round(((price - price_5d_ago) / price_5d_ago) * 100, 2) if price_5d_ago else 0.0

        return {
            "ticker":   ticker,
            "price":    round(price, 2),
            "chg_1d":   chg_1d,
            "chg_5d":   chg_5d,
            "high_52w": round(high_52w, 2),
            "low_52w":  round(low_52w, 2),
            "source":   "yahoo_finance",
        }
    except Exception as e:
        print(f"  Yahoo failed for {ticker}: {e}")
        return None


# ─────────────────────────────────────────────────────────────
# FMP ENRICHMENT
# ─────────────────────────────────────────────────────────────

def _fetch_fmp_targets(tickers: list[str]) -> dict[str, dict]:
    """
    Fetch analyst price targets from FMP for all tickers in one call.
    Returns dict keyed by ticker.
    Falls back gracefully if API key missing or call fails.
    """
    if not FMP_API_KEY:
        return {}

    enriched = {}
    for ticker in tickers:
        try:
            url  = f"{FMP_BASE}/price-target?symbol={ticker}&apikey={FMP_API_KEY}"
            resp = requests.get(url, timeout=5)
            data = resp.json()

            if data and isinstance(data, list):
                latest = data[0]
                enriched[ticker] = {
                    "analyst_target":    float(latest.get("priceTarget") or 0),
                    "analyst_publisher": latest.get("publishedDate","")[:10],
                }
            time.sleep(0.2)   # FMP rate limit

        except Exception as e:
            print(f"  FMP failed for {ticker}: {e}")
            continue

    return enriched


# ─────────────────────────────────────────────────────────────
# REPORT-READY PRICE CONTEXT
# ─────────────────────────────────────────────────────────────

def fetch_prices_for_report() -> dict[str, dict]:
    """
    Fetch enriched price data for all 14 holdings.
    Returns dict keyed by ticker, ready to inject into report prompt.

    Used by report_generator.py to give the LLM price context:
    - Is NVDA already down 18% this week? (already priced in or entry point?)
    - Is IREN near its BTC break-even? (immediate margin risk)
    - Is COIN near 52-week lows? (distressed or bottoming?)

    Example return value:
    {
      "NVDA": {
        "price": 198.45,
        "chg_1d": -0.56,
        "chg_5d": -8.2,
        "high_52w": 340.0,
        "low_52w": 102.0,
        "52w_position": "22% of 52w range — near lows",
        "move_label": "down sharply",
        "analyst_target": 280.0,
        "upside_pct": 41.1,
      }
    }
    """
    print(f"\nFetching prices for report...")
    result   = {}
    errors   = []

    # Yahoo Finance — base data
    for ticker in TRACKED_TICKERS:
        data = _fetch_yahoo(ticker)
        if data:
            result[ticker] = data
            result[ticker]["52w_position"] = _52w_position(
                data["price"], data["low_52w"], data["high_52w"]
            )
            result[ticker]["move_label"] = _move_label(data["chg_5d"])
        else:
            errors.append(ticker)
        time.sleep(0.3)   # avoid Yahoo rate limit

    # FMP — analyst targets (optional enrichment)
    if FMP_API_KEY and result:
        fmp_data = _fetch_fmp_targets(list(result.keys()))
        for ticker, fmp in fmp_data.items():
            if ticker in result and fmp.get("analyst_target"):
                result[ticker]["analyst_target"] = fmp["analyst_target"]
                price = result[ticker]["price"]
                if price > 0:
                    result[ticker]["upside_pct"] = round(
                        (fmp["analyst_target"] - price) / price * 100, 1
                    )

    if errors:
        print(f"  Failed tickers: {errors}")
    print(f"  Prices fetched: {len(result)}/14")

    return result


def build_price_context_for_prompt(prices: dict[str, dict]) -> str:
    """
    Formats price data into a clean string for the report LLM prompt.
    Groups by cluster for readability.
    """
    if not prices:
        return "Price data unavailable."

    clusters = {
        "Crypto Infrastructure": ["IREN", "HUT", "COIN"],
        "Semiconductors":        ["NVDA", "TSM", "MU"],
        "Cloud / AI Platforms":  ["MSFT", "GOOGL", "AMZN", "META", "DDOG"],
        "Fintech / Insurtech":   ["HOOD", "LMND", "RDDT"],
    }

    lines = []
    for cluster, tickers in clusters.items():
        lines.append(f"\n{cluster}:")
        for t in tickers:
            p = prices.get(t)
            if not p:
                lines.append(f"  {t:<6} — data unavailable")
                continue

            arrow_1d = "▲" if p["chg_1d"] >= 0 else "▼"
            arrow_5d = "▲" if p["chg_5d"] >= 0 else "▼"

            target_str = ""
            if p.get("analyst_target"):
                upside = p.get("upside_pct", 0)
                sign   = "+" if upside >= 0 else ""
                target_str = f" | analyst target ${p['analyst_target']:.0f} ({sign}{upside:.0f}%)"

            lines.append(
                f"  {t:<6} ${p['price']:>8.2f}  "
                f"1D {arrow_1d}{abs(p['chg_1d']):.1f}%  "
                f"5D {arrow_5d}{abs(p['chg_5d']):.1f}%  "
                f"[{p['52w_position']}]"
                f"{target_str}"
            )

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# DASHBOARD FETCH (Streamlit)
# ─────────────────────────────────────────────────────────────

def fetch_prices() -> list[dict]:
    """
    Fetch prices for Streamlit dashboard display.
    Returns list of dicts with ticker, price, change_pct.
    Keeps same interface as old real_time_price.py.
    """
    rows      = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    print(f"\n{'='*55}")
    print(f"  STOCK PRICE FETCH — {datetime.now().strftime('%H:%M:%S UTC')}")
    print(f"{'='*55}")

    for ticker in TRACKED_TICKERS:
        data = _fetch_yahoo(ticker)
        if data:
            row = {
                "ticker":     ticker,
                "price":      data["price"],
                "change_pct": data["chg_1d"],
                "chg_5d":     data["chg_5d"],
                "high_52w":   data["high_52w"],
                "low_52w":    data["low_52w"],
                "source":     "yahoo_finance",
                "fetched_at": fetched_at,
            }
            rows.append(row)
            arrow = "▲" if data["chg_1d"] >= 0 else "▼"
            print(f"  {ticker:<6} ${data['price']:>8.2f}  {arrow}{abs(data['chg_1d']):.2f}%  5D {data['chg_5d']:+.1f}%")
        else:
            print(f"  {ticker:<6} ERROR")
        time.sleep(0.3)

    print(f"{'='*55}")
    print(f"  Fetched: {len(rows)}/{len(TRACKED_TICKERS)}")
    return rows


def save_prices(rows: list[dict]) -> int:
    if not rows:
        return 0
    saved = 0
    for row in rows:
        try:
            supabase.table("stock_prices").insert({
                "ticker":     row["ticker"],
                "price":      row["price"],
                "change_pct": row["change_pct"],
                "chg_5d":     row.get("chg_5d", 0),
                "high_52w":   row.get("high_52w", 0),
                "low_52w":    row.get("low_52w", 0),
                "source":     row["source"],
                "fetched_at": row["fetched_at"],
            }).execute()
            saved += 1
        except Exception as e:
            print(f"  Failed to save {row['ticker']}: {e}")
    return saved


def run_price_pipeline():
    rows  = fetch_prices()
    saved = save_prices(rows)
    return saved


if __name__ == "__main__":
    # Test report-ready output
    prices = fetch_prices_for_report()
    print("\n" + "="*60)
    print("REPORT PRICE CONTEXT:")
    print("="*60)
    print(build_price_context_for_prompt(prices))