"""
webapp/streamlit_app.py

BIT Capital — Polymarket Signal Scanner
Dark Bloomberg-style dashboard
"""
import sys
import os

# Add project root to path so utils/ can be found
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import yfinance as yf
import streamlit as st
import pandas as pd
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

# ── Page config ───────────────────────────────────────────────
st.set_page_config(
    page_title="BIT Capital — Signal Scanner",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Supabase ──────────────────────────────────────────────────
from utils.supabase_client import get_anon_client
supabase = get_anon_client()

# ── Styling ───────────────────────────────────────────────────
st.markdown("""
<style>
/* ── Base ── */
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
    background-color: #0a0a0f;
    color: #e2e8f0;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background-color: #0f0f1a;
    border-right: 1px solid #1e2035;
}
[data-testid="stSidebar"] * { color: #94a3b8; }

/* ── Metric cards ── */
[data-testid="metric-container"] {
    background: #111827;
    border: 1px solid #1e2035;
    border-radius: 6px;
    padding: 16px;
}
[data-testid="metric-container"] label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 10px !important;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #475569 !important;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 26px !important;
    color: #f0f6ff !important;
}

/* ── Tables ── */
[data-testid="stDataFrame"] {
    border: 1px solid #1e2035;
    border-radius: 6px;
    overflow: hidden;
}

/* ── Signal cards ── */
.signal-card {
    background: #111827;
    border: 1px solid #1e2035;
    border-radius: 8px;
    padding: 16px 20px;
    margin-bottom: 10px;
    transition: border-color 0.15s;
}
.signal-card:hover { border-color: #334155; }
.signal-card.bullish { border-left: 3px solid #22c55e; }
.signal-card.bearish { border-left: 3px solid #ef4444; }
.signal-card.neutral { border-left: 3px solid #64748b; }

.signal-question {
    font-size: 14px;
    font-weight: 500;
    color: #e2e8f0;
    margin-bottom: 6px;
}
.signal-meta {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    color: #475569;
}
.badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}
.badge-bullish { background: #052e16; color: #22c55e; }
.badge-bearish { background: #2d0a0a; color: #ef4444; }
.badge-neutral { background: #0f172a; color: #64748b; }

/* ── Report ── */
.report-body {
    background: #111827;
    border: 1px solid #1e2035;
    border-radius: 8px;
    padding: 28px 32px;
    font-size: 14px;
    line-height: 1.8;
}
.report-body h1 { font-size: 20px; color: #f0f6ff; margin-bottom: 16px; }
.report-body h2 { font-size: 16px; color: #60a5fa; border-bottom: 1px solid #1e2035; padding-bottom: 6px; margin: 24px 0 12px; }
.report-body h3 { font-size: 14px; color: #f59e0b; margin: 16px 0 8px; }

/* ── Price card ── */
.price-card {
    background: #111827;
    border: 1px solid #1e2035;
    border-radius: 8px;
    padding: 16px;
    text-align: center;
}
.price-ticker {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 18px;
    font-weight: 600;
    color: #60a5fa;
}
.price-company { font-size: 11px; color: #475569; margin: 2px 0 10px; }
.price-value {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 22px;
    font-weight: 600;
    color: #f0f6ff;
}
.price-change-up   { font-family: 'IBM Plex Mono', monospace; font-size: 13px; color: #22c55e; }
.price-change-down { font-family: 'IBM Plex Mono', monospace; font-size: 13px; color: #ef4444; }

/* ── Dig deeper output ── */
.deep-dive-box {
    background: #0d1117;
    border: 1px solid #1e3a5f;
    border-radius: 8px;
    padding: 20px 24px;
    margin-top: 10px;
    font-size: 13px;
    line-height: 1.7;
}
.source-link {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    color: #3b82f6;
}

/* ── Section headers ── */
.section-header {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: #475569;
    padding: 4px 0 10px;
    border-bottom: 1px solid #1e2035;
    margin-bottom: 18px;
}

/* ── Tab styling ── */
[data-testid="stTabs"] button {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 12px;
    letter-spacing: 0.06em;
    color: #475569;
}
[data-testid="stTabs"] button[aria-selected="true"] {
    color: #60a5fa;
    border-bottom-color: #60a5fa !important;
}

/* ── Divider ── */
hr { border-color: #1e2035; }

/* ── Expander ── */
[data-testid="stExpander"] {
    background: #111827;
    border: 1px solid #1e2035;
    border-radius: 6px;
}
</style>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────

SENTIMENT_BADGE = {
    "Bullish": '<span class="badge badge-bullish">▲ Bullish</span>',
    "Bearish": '<span class="badge badge-bearish">▼ Bearish</span>',
    "Neutral": '<span class="badge badge-neutral">— Neutral</span>',
}

HOLDINGS_BIT = {
    "IREN":  "IREN Limited",
    "MSFT":  "Microsoft",
    "GOOGL": "Alphabet",
    "LMND":  "Lemonade",
    "RDDT":  "Reddit",
    "MU":    "Micron",
    "TSM":   "TSMC",
    "HUT":   "Hut 8",
    "HOOD":  "Robinhood",
    "DDOG":  "Datadog",
    "AMZN":  "Amazon",
    "COIN":  "Coinbase",
    "META":  "Meta",
    "NVDA":  "NVIDIA",
}

def fmt_vol(v):
    v = float(v or 0)
    if v >= 1_000_000: return f"${v/1_000_000:.1f}M"
    if v >= 1_000:     return f"${v/1_000:.0f}K"
    return f"${v:,.0f}"

def fmt_prob(p):
    return f"{float(p or 0):.0%}"

def sentiment_class(s):
    return (s or "neutral").lower()


# ── Data fetchers (cached) ────────────────────────────────────

@st.cache_data(ttl=120)   # refresh every 2 min
def load_signals(limit=100):
    res = (
        supabase.table("signal_feed")
        .select("*")
        .order("impact_score", desc=True)
        .limit(limit)
        .execute()
    )
    return res.data or []


@st.cache_data(ttl=300)
def load_reports(limit=20):
    res = (
        supabase.table("reports")
        .select("id, generated_at, tickers, signal_count, content")
        .order("generated_at", desc=True)
        .limit(limit)
        .execute()
    )
    return res.data or []


@st.cache_data(ttl=120)
def load_event_markets(event_id: str) -> list[dict]:
    """Fetch all markets for a given event_id."""
    res = (
        supabase.table("markets")
        .select("id, question, yes_price, no_price, volume, end_date")
        .eq("event_id", event_id)
        .eq("active", True)
        .eq("closed", False)
        .order("volume", desc=True)
        .execute()
    )
    return res.data or []


@st.cache_data(ttl=60)
def load_stats():
    markets = supabase.table("markets").select("id", count="exact").execute()
    signals = supabase.table("signals").select("id", count="exact").eq("is_relevant", True).execute()
    reports = supabase.table("reports").select("id", count="exact").execute()
    events  = supabase.table("events").select("id",  count="exact").execute()
    return {
        "markets":  markets.count or 0,
        "signals":  signals.count or 0,
        "reports":  reports.count or 0,
        "events":   events.count  or 0,
    }


@st.cache_data(ttl=300)
def load_stock_prices_db():
    """Fallback prices from DB."""
    rows = []
    for ticker in HOLDINGS_BIT:
        res = (
            supabase.table("stock_prices")
            .select("ticker, price, change_pct, fetched_at")
            .eq("ticker", ticker)
            .order("fetched_at", desc=True)
            .limit(1)
            .execute()
        )
        if res.data:
            rows.append(res.data[0])
    return rows


def fetch_live_prices():
    """Fetch live prices from Yahoo Finance."""
    prices = {}
    tickers = list(HOLDINGS_BIT.keys())
    try:
        data = yf.download(
            tickers,
            period="2d",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
        )
        for ticker in tickers:
            try:
                df = data[ticker] if len(tickers) > 1 else data
                if df.empty:
                    continue
                latest    = float(df["Close"].iloc[-1])
                prev      = float(df["Close"].iloc[-2]) if len(df) >= 2 else latest
                chg       = ((latest - prev) / prev) * 100 if prev else 0
                prices[ticker] = {"price": latest, "change_pct": chg, "live": True}
            except Exception:
                continue
    except Exception:
        pass
    return prices


def get_existing_deep_dive(signal_id):
    res = (
        supabase.table("deep_dives")
        .select("*")
        .eq("signal_id", signal_id)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    if not res.data:
        return None
    row = res.data[0]
    # Check if < 6 hours old
    import dateutil.parser
    age = (datetime.now(timezone.utc) - dateutil.parser.parse(row["created_at"])).total_seconds() / 3600
    return row if age < 6 else None


# ── Sidebar ───────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div style='padding: 8px 0 24px'>
        <div style='font-family: IBM Plex Mono, monospace; font-size: 11px;
                    letter-spacing: 0.14em; color: #3b82f6; text-transform: uppercase;
                    margin-bottom: 4px;'>BIT Capital</div>
        <div style='font-size: 18px; font-weight: 600; color: #f0f6ff;'>Signal Scanner</div>
        <div style='font-size: 11px; color: #334155; margin-top: 2px;'>Polymarket Intelligence</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    # DB stats
    stats = load_stats()
    st.metric("Active Markets",  f"{stats['markets']:,}")
    st.metric("Relevant Signals", f"{stats['signals']:,}")
    st.metric("Reports Generated", f"{stats['reports']:,}")

    st.markdown("---")
    st.markdown(
        f"<div style='font-family: IBM Plex Mono, monospace; font-size: 10px; "
        f"color: #334155;'>Last refresh<br>{datetime.now().strftime('%H:%M:%S UTC')}</div>",
        unsafe_allow_html=True
    )
    if st.button("↺  Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ── Main tabs ─────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📡  Dashboard",
    "🎯  Signal Feed",
    "📊  Stock Prices",
    "📋  Reports",
    "🔍  Dig Deeper",
])


# ════════════════════════════════════════════════════════════
# TAB 1 — DASHBOARD
# ════════════════════════════════════════════════════════════

with tab1:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots

    # ── Pipeline stats ────────────────────────────────────────
    st.markdown('<div class="section-header">Pipeline Status</div>', unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Markets Tracked",   f"{stats['markets']:,}")
    col2.metric("Active Signals",    f"{stats['signals']:,}")
    col3.metric("Events Monitored",  f"{stats['events']:,}")
    col4.metric("Reports Generated", f"{stats['reports']:,}")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Fetch historical prices for all holdings ──────────────
    st.markdown('<div class="section-header">BIT Capital Holdings Screener</div>', unsafe_allow_html=True)

    @st.cache_data(ttl=300)
    def fetch_historical_prices(period="1mo"):
        tickers = list(HOLDINGS_BIT.keys())
        try:
            data = yf.download(
                tickers,
                period=period,
                interval="1d",
                group_by="ticker",
                auto_adjust=True,
                progress=False,
            )
            result = {}
            for ticker in tickers:
                try:
                    df = data[ticker] if len(tickers) > 1 else data
                    if not df.empty:
                        result[ticker] = df["Close"].dropna()
                except Exception:
                    continue
            return result
        except Exception:
            return {}

    # Period selector
    period_map = {"1W": "5d", "1M": "1mo", "3M": "3mo", "6M": "6mo"}
    sel_period = st.radio(
        "Time period",
        options=list(period_map.keys()),
        index=1,
        horizontal=True,
        label_visibility="collapsed",
    )

    hist = fetch_historical_prices(period=period_map[sel_period])
    live = fetch_live_prices()

    # ── Animated ticker tape ──────────────────────────────────
    tape_items = []
    for ticker, name in HOLDINGS_BIT.items():
        p   = live.get(ticker, {})
        px_ = p.get("price", 0)
        chg = p.get("change_pct", 0)
        arrow = "▲" if chg >= 0 else "▼"
        color = "#22c55e" if chg >= 0 else "#ef4444"
        tape_items.append(
            f'<span style="margin-right:40px; white-space:nowrap;">'
            f'<span style="font-weight:600; color:#e2e8f0;">{ticker}</span>'
            f'&nbsp;<span style="color:#94a3b8; font-size:11px;">{name}</span>'
            f'&nbsp;&nbsp;<span style="color:#f0f6ff;">${px_:,.2f}</span>'
            f'&nbsp;<span style="color:{color}; font-size:12px;">{arrow} {abs(chg):.2f}%</span>'
            f'</span>'
        )

    tape_html = "".join(tape_items) + "&nbsp;&nbsp;&nbsp;" + "".join(tape_items)

    st.markdown(f"""
    <div style="overflow:hidden; background:#0d1117; border:1px solid #1e2035;
                border-radius:8px; padding:12px 0; margin-bottom:20px;">
        <div style="font-family:IBM Plex Mono,monospace; font-size:13px;
                    white-space:nowrap; display:inline-block;
                    animation:ticker 40s linear infinite;"
             id="ticker-tape">
            {tape_html}
        </div>
    </div>
    <style>
    @keyframes ticker {{
        0%   {{ transform: translateX(0); }}
        100% {{ transform: translateX(-50%); }}
    }}
    </style>
    """, unsafe_allow_html=True)

    # ── Screener table ────────────────────────────────────────
    screener_rows = []
    for ticker, name in HOLDINGS_BIT.items():
        p   = live.get(ticker, {})
        px_ = p.get("price", 0)
        chg = p.get("change_pct", 0)

        # 30-day return from historical
        hist_series = hist.get(ticker)
        ret_30d = 0.0
        if hist_series is not None and len(hist_series) >= 2:
            ret_30d = ((hist_series.iloc[-1] - hist_series.iloc[0]) / hist_series.iloc[0]) * 100

        # Signal sentiment for this ticker
        sigs = load_signals(limit=200)
        ticker_sigs  = [s for s in sigs if s.get("ticker") == ticker]
        bull_count   = sum(1 for s in ticker_sigs if s.get("sentiment") == "Bullish")
        bear_count   = sum(1 for s in ticker_sigs if s.get("sentiment") == "Bearish")
        if bull_count > bear_count:
            pm_signal = "🟢 Bullish"
        elif bear_count > bull_count:
            pm_signal = "🔴 Bearish"
        elif ticker_sigs:
            pm_signal = "⚪ Neutral"
        else:
            pm_signal = "— No signal"

        screener_rows.append({
            "Ticker":       ticker,
            "Company":      name,
            "Price":        f"${px_:,.2f}",
            "1D":           f"{'▲' if chg >= 0 else '▼'} {abs(chg):.2f}%",
            f"{sel_period} Return": f"{'▲' if ret_30d >= 0 else '▼'} {abs(ret_30d):.1f}%",
            "PM Signal":    pm_signal,
            "# Signals":    len(ticker_sigs),
        })

    st.dataframe(
        pd.DataFrame(screener_rows),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Time series chart ─────────────────────────────────────
    st.markdown('<div class="section-header">Price Performance — Time Series</div>', unsafe_allow_html=True)

    # Ticker selector for chart
    chart_tickers = st.multiselect(
        "Select tickers to compare",
        options=list(HOLDINGS_BIT.keys()),
        default=["IREN", "MSFT", "GOOGL", "HUT"],
        label_visibility="visible",
    )

    if chart_tickers and hist:
        # Normalise to 100 at start for fair comparison
        normalize = st.toggle("Normalize to 100 (compare performance)", value=True)

        fig = go.Figure()

        colors = [
            "#60a5fa","#22c55e","#f59e0b","#ef4444",
            "#a78bfa","#34d399","#fb923c","#f472b6",
            "#38bdf8","#4ade80",
        ]

        for i, ticker in enumerate(chart_tickers):
            series = hist.get(ticker)
            if series is None or series.empty:
                continue

            color  = colors[i % len(colors)]
            dates  = series.index
            values = series.values

            if normalize:
                values = (values / values[0]) * 100

            # Add signal markers on the chart
            ticker_sigs = [s for s in load_signals(200) if s.get("ticker") == ticker]

            fig.add_trace(go.Scatter(
                x=dates,
                y=values,
                mode="lines",
                name=f"{ticker} — {HOLDINGS_BIT[ticker]}",
                line=dict(color=color, width=2),
                hovertemplate=(
                    f"<b>{ticker}</b><br>"
                    f"Date: %{{x|%b %d, %Y}}<br>"
                    f"{'Index' if normalize else 'Price'}: %{{y:.2f}}<br>"
                    "<extra></extra>"
                ),
            ))

        fig.update_layout(
            paper_bgcolor="#0a0a0f",
            plot_bgcolor="#0d1117",
            font=dict(family="IBM Plex Mono", color="#94a3b8", size=11),
            legend=dict(
                bgcolor="#111827",
                bordercolor="#1e2035",
                borderwidth=1,
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="left",
                x=0,
            ),
            xaxis=dict(
                gridcolor="#1e2035",
                linecolor="#1e2035",
                tickformat="%b %d",
                showspikes=True,
                spikecolor="#334155",
                spikethickness=1,
            ),
            yaxis=dict(
                gridcolor="#1e2035",
                linecolor="#1e2035",
                tickprefix="" if normalize else "$",
                showspikes=True,
                spikecolor="#334155",
                spikethickness=1,
            ),
            hovermode="x unified",
            margin=dict(l=0, r=0, t=40, b=0),
            height=420,
        )

        st.plotly_chart(fig, use_container_width=True)

        # ── Mini sparklines below chart ───────────────────────
        st.markdown('<div class="section-header" style="margin-top:16px;">Individual Performance</div>', unsafe_allow_html=True)

        spark_cols = st.columns(len(chart_tickers))
        for i, ticker in enumerate(chart_tickers):
            series = hist.get(ticker)
            if series is None or series.empty:
                continue

            p      = live.get(ticker, {})
            price  = p.get("price", 0)
            chg    = p.get("change_pct", 0)
            color  = colors[i % len(colors)]
            is_up  = series.iloc[-1] >= series.iloc[0]
            line_c = "#22c55e" if is_up else "#ef4444"

            spark = go.Figure(go.Scatter(
                x=series.index,
                y=series.values,
                mode="lines",
                line=dict(color=line_c, width=1.5),
                fill="tozeroy",
                fillcolor=f"rgba({'34,197,94' if is_up else '239,68,68'},0.08)",
            ))
            spark.update_layout(
                paper_bgcolor="#111827",
                plot_bgcolor="#111827",
                margin=dict(l=0, r=0, t=0, b=0),
                height=70,
                xaxis=dict(visible=False),
                yaxis=dict(visible=False),
                showlegend=False,
            )

            ret = ((series.iloc[-1] - series.iloc[0]) / series.iloc[0]) * 100
            chg_str = f"{'▲' if chg >= 0 else '▼'} {abs(chg):.2f}%"
            ret_str = f"{'▲' if ret >= 0 else '▼'} {abs(ret):.1f}%"
            chg_col = "#22c55e" if chg >= 0 else "#ef4444"
            ret_col = "#22c55e" if ret >= 0 else "#ef4444"

            with spark_cols[i]:
                st.markdown(f"""
                <div style="background:#111827; border:1px solid #1e2035;
                            border-radius:8px; padding:12px 14px 4px;">
                    <div style="font-family:IBM Plex Mono,monospace; font-size:13px;
                                font-weight:600; color:{color};">{ticker}</div>
                    <div style="font-family:IBM Plex Mono,monospace; font-size:16px;
                                color:#f0f6ff; margin:2px 0;">${price:,.2f}</div>
                    <div style="font-size:11px; color:{chg_col};">{chg_str} today</div>
                    <div style="font-size:11px; color:{ret_col};">{ret_str} {sel_period}</div>
                </div>
                """, unsafe_allow_html=True)
                st.plotly_chart(spark, use_container_width=True, config={"displayModeBar": False})

    else:
        st.info("Select at least one ticker to display the chart.")


# ════════════════════════════════════════════════════════════
# TAB 2 — SIGNAL FEED
# ════════════════════════════════════════════════════════════

with tab2:
    st.markdown('<div class="section-header">All Signals — Grouped by Event</div>', unsafe_allow_html=True)

    signals = load_signals(limit=200)

    if not signals:
        st.info("No signals found. Run the filter pipeline first.")
    else:
        # ── Filters ───────────────────────────────────────────
        fc1, fc2, fc3, fc4 = st.columns([2, 2, 2, 2])
        all_tickers    = sorted({s.get("ticker","")   for s in signals if s.get("ticker")})
        all_categories = sorted({s.get("category","") for s in signals if s.get("category")})
        sel_ticker    = fc1.selectbox("Ticker",    ["All"] + all_tickers)
        sel_sentiment = fc2.selectbox("Sentiment", ["All", "Bullish", "Bearish", "Neutral"])
        sel_category  = fc3.selectbox("Category",  ["All"] + all_categories)
        min_score     = fc4.slider("Min Score", 1, 10, 1)

        filtered = signals
        if sel_ticker    != "All": filtered = [s for s in filtered if s.get("ticker")    == sel_ticker]
        if sel_sentiment != "All": filtered = [s for s in filtered if s.get("sentiment") == sel_sentiment]
        if sel_category  != "All": filtered = [s for s in filtered if s.get("category")  == sel_category]
        filtered = [s for s in filtered if (s.get("impact_score") or 0) >= min_score]

        # ── Group by event_id so each event appears once ──────
        # One event card = one unique market question
        # All affected tickers for that market are shown together
        from collections import defaultdict
        grouped: dict[str, dict] = {}   # event_id → event data

        for s in filtered:
            eid = s.get("event_id") or s.get("market_id", "unknown")
            mid = s.get("market_id", "")

            if eid not in grouped:
                grouped[eid] = {
                    "event_id":    eid,
                    "market_id":   mid,
                    "event_title": s.get("event_title",""),
                    "question":    s.get("question",""),
                    "category":    s.get("category",""),
                    "yes_price":   s.get("yes_price", 0),
                    "volume":      s.get("volume", 0),
                    "end_date":    s.get("end_date",""),
                    "impact_score":s.get("impact_score", 0),
                    "sentiment":   s.get("sentiment","Neutral"),
                    "tickers":     [],
                    "reasonings":  [],
                }

            # Accumulate tickers + reasonings for this event
            ticker = s.get("ticker","")
            if ticker and ticker not in grouped[eid]["tickers"]:
                grouped[eid]["tickers"].append(ticker)
            reasoning = s.get("reasoning","")
            if reasoning and reasoning not in grouped[eid]["reasonings"]:
                grouped[eid]["reasonings"].append(reasoning)

        events_list = list(grouped.values())
        st.caption(f"Showing {len(events_list)} unique events ({len(filtered)} signals)")
        st.markdown("<br>", unsafe_allow_html=True)

        # ── Render one card per event ─────────────────────────
        for ev in events_list:
            sentiment  = ev.get("sentiment", "Neutral")
            css_class  = sentiment_class(sentiment)
            badge      = SENTIMENT_BADGE.get(sentiment, "")
            expiry     = ev.get("end_date","")[:10] if ev.get("end_date") else "—"
            tickers    = ev.get("tickers", [])
            event_id   = ev.get("event_id","")

            # Build ticker chips
            ticker_chips = " ".join(
                f'<span style="background:#1e3a5f; color:#60a5fa; '
                f'font-family:IBM Plex Mono,monospace; font-size:11px; '
                f'font-weight:600; padding:2px 7px; border-radius:4px;">'
                f'{t}</span>'
                for t in tickers
            )

            st.markdown(f"""
            <div class="signal-card {css_class}">
                <div style="font-size:11px; color:#475569; margin-bottom:4px;
                            font-family:IBM Plex Mono,monospace; letter-spacing:0.06em;">
                    {ev.get('category','').upper()} &nbsp;·&nbsp; {ev.get('event_title','')}
                </div>
                <div class="signal-question">{ev.get('question','')}</div>
                <div style="margin: 8px 0 6px; display:flex; gap:6px; flex-wrap:wrap; align-items:center;">
                    {badge} &nbsp; {ticker_chips}
                </div>
                <div class="signal-meta">
                    Score: <b style="color:#f0f6ff">{ev.get('impact_score','?')}/10</b>
                    &nbsp;·&nbsp; YES: <b style="color:#60a5fa">{fmt_prob(ev.get('yes_price',0))}</b>
                    &nbsp;·&nbsp; Vol: {fmt_vol(ev.get('volume',0))}
                    &nbsp;·&nbsp; Expiry: {expiry}
                </div>
                <div style="margin-top:8px; font-size:12px; color:#64748b; font-style:italic;">
                    {ev['reasonings'][0] if ev.get('reasonings') else ''}
                </div>
            </div>
            """, unsafe_allow_html=True)

            # ── Markets expander ──────────────────────────────
            if event_id:
                with st.expander("📂  View all markets under this event"):
                    event_markets = load_event_markets(event_id)
                    if not event_markets:
                        st.caption("No markets found.")
                    else:
                        rows = []
                        for m in event_markets:
                            is_current = m.get("question","") == ev.get("question","")
                            rows.append({
                                "Question": ("★ " if is_current else "") + m.get("question",""),
                                "YES":      f"{float(m.get('yes_price',0)):.0%}",
                                "NO":       f"{float(m.get('no_price', 0)):.0%}",
                                "Volume":   fmt_vol(m.get("volume",0)),
                                "Expiry":   m.get("end_date","")[:10] if m.get("end_date") else "—",
                            })
                        st.dataframe(
                            pd.DataFrame(rows),
                            use_container_width=True,
                            hide_index=True,
                        )


# ════════════════════════════════════════════════════════════
# TAB 3 — STOCK PRICES
# ════════════════════════════════════════════════════════════

with tab3:
    st.markdown('<div class="section-header">BIT Capital Holdings — Live Prices</div>', unsafe_allow_html=True)

    with st.spinner("Fetching live prices from Yahoo Finance..."):
        live = fetch_live_prices()

    # Fallback to DB if live fetch failed
    if not live:
        st.warning("Live prices unavailable — showing last stored prices from DB.")
        db_prices = load_stock_prices_db()
        live = {r["ticker"]: {"price": r["price"], "change_pct": r["change_pct"], "live": False}
                for r in db_prices}

    # Render 2 rows of 5 cards
    tickers = list(HOLDINGS_BIT.keys())
    for row_start in [0, 7]:
        cols = st.columns(7)
        for i, ticker in enumerate(tickers[row_start:row_start+7]):
            p = live.get(ticker, {})
            price  = p.get("price",      0)
            chg    = p.get("change_pct", 0)
            is_up  = chg >= 0
            chg_str = f"{'▲' if is_up else '▼'} {abs(chg):.2f}%"
            chg_cls = "price-change-up" if is_up else "price-change-down"
            live_dot = "🟢" if p.get("live") else "🟡"

            cols[i].markdown(f"""
            <div class="price-card">
                <div class="price-ticker">{ticker}</div>
                <div class="price-company">{HOLDINGS_BIT[ticker]}</div>
                <div class="price-value">${price:,.2f}</div>
                <div class="{chg_cls}">{chg_str}</div>
                <div style="font-size:10px; color:#334155; margin-top:6px;">{live_dot} {'Live' if p.get('live') else 'Cached'}</div>
            </div>
            """, unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("---")

    # Price table
    st.markdown('<div class="section-header">Price Table</div>', unsafe_allow_html=True)
    rows = []
    for ticker, meta in HOLDINGS_BIT.items():
        p = live.get(ticker, {})
        rows.append({
            "Ticker":  ticker,
            "Company": meta,
            "Price":   f"${p.get('price',0):,.2f}",
            "1D Change": f"{'▲' if p.get('change_pct',0)>=0 else '▼'} {abs(p.get('change_pct',0)):.2f}%",
            "Source":  "Live" if p.get("live") else "Cached",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# ════════════════════════════════════════════════════════════
# TAB 4 — REPORTS
# ════════════════════════════════════════════════════════════

with tab4:
    st.markdown('<div class="section-header">Daily Alpha Reports</div>', unsafe_allow_html=True)

    reports = load_reports()

    if not reports:
        st.info("No reports yet — run report_generator.py first.")
    else:
        # Report selector on the left, content on the right
        r_col1, r_col2 = st.columns([1, 3])

        with r_col1:
            st.markdown("**Select Report**")
            report_options = {
                f"{r['generated_at'][:10]} — {len(r.get('tickers') or [])} tickers": r
                for r in reports
            }
            selected_label = st.radio(
                "",
                options=list(report_options.keys()),
                label_visibility="collapsed"
            )
            selected_report = report_options[selected_label]

        with r_col2:
            gen_at  = selected_report.get("generated_at","")[:16].replace("T"," ")
            tickers = selected_report.get("tickers") or []
            sig_cnt = selected_report.get("signal_count", 0)

            # Report header
            st.markdown(f"""
            <div style="display:flex; justify-content:space-between; align-items:center;
                        margin-bottom:16px; padding-bottom:12px; border-bottom:1px solid #1e2035;">
                <div>
                    <div style="font-size:16px; font-weight:600; color:#f0f6ff;">
                        BIT Capital Alpha Report
                    </div>
                    <div style="font-family: IBM Plex Mono, monospace; font-size:11px; color:#475569;">
                        {gen_at} UTC &nbsp;·&nbsp; {sig_cnt} signals
                    </div>
                </div>
                <div style="font-size:12px; color:#475569;">
                    {' '.join([f'<span style="background:#1e2035;padding:2px 6px;border-radius:4px;font-family:IBM Plex Mono,monospace;font-size:11px;color:#60a5fa;">{t}</span>' for t in tickers])}
                </div>
            </div>
            """, unsafe_allow_html=True)

            # Report content
            with st.container():
                st.markdown(
                    f'<div class="report-body">{selected_report.get("content","")}</div>',
                    unsafe_allow_html=True
                )

            # Download button
            st.download_button(
                label="⬇  Download as Markdown",
                data=selected_report.get("content",""),
                file_name=f"BIT_Capital_Report_{gen_at[:10]}.md",
                mime="text/markdown",
            )


# ════════════════════════════════════════════════════════════
# TAB 5 — DIG DEEPER
# ════════════════════════════════════════════════════════════

with tab5:
    st.markdown('<div class="section-header">Deep Dive Analysis</div>', unsafe_allow_html=True)
    st.markdown(
        "<p style='color:#64748b; font-size:13px;'>Click Analyze on any signal to run a "
        "real-time news + Groq analysis. Results are cached for 6 hours.</p>",
        unsafe_allow_html=True
    )

    signals = load_signals(limit=50)

    if not signals:
        st.info("No signals found. Run the filter pipeline first.")
    else:
        # Filters
        dd_col1, dd_col2 = st.columns([2, 2])
        dd_tickers    = sorted({s.get("ticker","") for s in signals if s.get("ticker")})
        dd_sel_ticker = dd_col1.selectbox("Filter by ticker", ["All"] + dd_tickers, key="dd_ticker")
        dd_sel_sent   = dd_col2.selectbox("Filter by sentiment", ["All","Bullish","Bearish","Neutral"], key="dd_sent")

        dd_filtered = signals
        if dd_sel_ticker != "All": dd_filtered = [s for s in dd_filtered if s.get("ticker") == dd_sel_ticker]
        if dd_sel_sent   != "All": dd_filtered = [s for s in dd_filtered if s.get("sentiment") == dd_sel_sent]

        st.markdown("<br>", unsafe_allow_html=True)

        for s in dd_filtered[:20]:
            signal_id = s.get("signal_id")
            sentiment = s.get("sentiment","Neutral")
            css_class = sentiment_class(sentiment)
            badge     = SENTIMENT_BADGE.get(sentiment,"")

            # Signal row
            st.markdown(f"""
            <div class="signal-card {css_class}">
                <div class="signal-question">{s.get('question','')}</div>
                <div class="signal-meta">
                    {badge} &nbsp;
                    <b style="color:#e2e8f0">{s.get('ticker','?')}</b>
                    &nbsp;·&nbsp; {s.get('company_name','')}
                    &nbsp;·&nbsp; Score: <b style="color:#f0f6ff">{s.get('impact_score','?')}/10</b>
                    &nbsp;·&nbsp; YES: <b style="color:#60a5fa">{fmt_prob(s.get('yes_price',0))}</b>
                    &nbsp;·&nbsp; Vol: {fmt_vol(s.get('volume',0))}
                </div>
            </div>
            """, unsafe_allow_html=True)

            # Analyze button — unique key per signal
            btn_key    = f"analyze_{signal_id}"
            result_key = f"result_{signal_id}"

            if st.button(f"🔍  Analyze {s.get('ticker','?')}", key=btn_key):
                # Check cache first
                cached = get_existing_deep_dive(signal_id)
                if cached:
                    st.session_state[result_key] = cached
                else:
                    with st.spinner(f"Running analysis for {s.get('ticker')}..."):
                        # Import and run dig_deeper
                        try:
                            from pipeline.dig_deeper_analysis import dig_deeper
                            result = dig_deeper(signal_id)
                            st.session_state[result_key] = result
                        except Exception as e:
                            st.error(f"Analysis failed: {e}")

            # Show result if available
            if result_key in st.session_state:
                result = st.session_state[result_key]
                if isinstance(result, dict) and "error" not in result:
                    direction  = result.get("direction", "Neutral")
                    from_cache = result.get("from_cache", False)
                    dir_color  = {"Bullish":"#22c55e","Bearish":"#ef4444","Neutral":"#64748b"}.get(direction,"#64748b")
                    sources    = result.get("source_urls") or []

                    # Build sources HTML to embed inside the box
                    sources_html = ""
                    if sources:
                        links = "".join(
                            f'<a href="{url}" target="_blank" style="display:block; '
                            f'font-family:IBM Plex Mono,monospace; font-size:11px; '
                            f'color:#3b82f6; text-decoration:none; margin-bottom:4px; '
                            f'word-break:break-all;">↗ {url[:90]}</a>'
                            for url in sources[:4]
                        )
                        sources_html = f"""
                        <div style="margin-top:16px; padding-top:14px;
                                    border-top:1px solid #1e3a5f;">
                            <div style="font-family:IBM Plex Mono,monospace; font-size:10px;
                                        letter-spacing:0.12em; color:#334155;
                                        text-transform:uppercase; margin-bottom:8px;">
                                News Sources
                            </div>
                            {links}
                        </div>"""

                    st.markdown(f"""
                    <div class="deep-dive-box">
                        <div style="display:flex; justify-content:space-between; margin-bottom:14px;">
                            <div style="font-family:IBM Plex Mono,monospace; font-size:11px; color:#475569;">
                                DEEP DIVE — {s.get('ticker')} &nbsp;·&nbsp;
                                {'⚡ Live Analysis' if not from_cache else '📦 Cached'}
                            </div>
                            <div style="font-family:IBM Plex Mono,monospace; font-size:12px;
                                        font-weight:600; color:{dir_color};">
                                {direction.upper()}
                            </div>
                        </div>
                        <div style="font-size:13px; line-height:1.7; color:#cbd5e1;">
                            {result.get('analysis_text','').replace(chr(10), '<br>')}
                        </div>
                        {sources_html}
                    </div>
                    """, unsafe_allow_html=True)

            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)