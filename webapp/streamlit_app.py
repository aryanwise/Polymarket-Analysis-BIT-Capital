"""
webapp/streamlit_app.py
BIT Capital — Polymarket Signal Scanner
Rebuilt for new ETL schema (signals, stocks, reports, deep_dives).
"""
import os, sys, json, time
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import yfinance as yf
import streamlit as st
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()
from pipeline.explore_polymarket_news import (
    fetch_event_from_text,
    fetch_news,
    run_gemini_analysis,
    extract_portfolio_impacts
)
from utils.supabase_client import get_anon_client
supabase = get_anon_client()

st.set_page_config(
    page_title="BIT Capital · Signal Scanner",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────
st.html("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

html,body,[class*="css"] { font-family:'Inter',sans-serif; background:#09090b; color:#f4f4f5; }

/* sidebar */
[data-testid="stSidebar"] { background:#0c0c0f; border-right:1px solid #18181b; }
[data-testid="stSidebar"] .stMarkdown p,
[data-testid="stSidebar"] label { color:#71717a; font-size:12px; }

/* top bar logo */
.logo-wrap { padding:4px 0 20px; }
.logo-tag  { font-family:'JetBrains Mono',monospace; font-size:10px; letter-spacing:.16em;
             text-transform:uppercase; color:#3b82f6; margin-bottom:4px; }
.logo-name { font-size:20px; font-weight:600; color:#fafafa; }
.logo-sub  { font-size:11px; color:#3f3f46; margin-top:2px; }

/* metric */
[data-testid="metric-container"] {
    background:#111113; border:1px solid #27272a; border-radius:8px; padding:16px; }
[data-testid="metric-container"] label {
    font-family:'JetBrains Mono',monospace; font-size:10px !important;
    letter-spacing:.1em; text-transform:uppercase; color:#52525b !important; }
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family:'JetBrains Mono',monospace; font-size:24px !important; color:#fafafa !important; }
[data-testid="metric-container"] [data-testid="stMetricDelta"] { font-size:11px !important; }

/* tabs */
[data-testid="stTabs"] button {
    font-family:'JetBrains Mono',monospace; font-size:11px;
    letter-spacing:.06em; color:#52525b; padding:8px 16px; }
[data-testid="stTabs"] button[aria-selected="true"] { color:#3b82f6; border-bottom:2px solid #3b82f6 !important; }
[data-testid="stTabs"] { border-bottom:1px solid #27272a; }

/* cards */
.card {
    background:#111113; border:1px solid #27272a; border-radius:10px;
    padding:18px 22px; margin-bottom:10px; transition:border-color .15s; }
.card:hover { border-color:#3f3f46; }
.card.bull  { border-left:3px solid #22c55e; }
.card.bear  { border-left:3px solid #ef4444; }
.card.neut  { border-left:3px solid #52525b; }

.card-q  { font-size:14px; font-weight:500; color:#f4f4f5; margin-bottom:6px; line-height:1.5; }
.card-ev { font-size:11px; color:#52525b; margin-bottom:8px; }
.card-meta { font-family:'JetBrains Mono',monospace; font-size:11px; color:#71717a; }
.card-reason { font-size:12px; color:#71717a; font-style:italic; margin-top:10px;
               padding-top:10px; border-top:1px solid #1c1c1e; line-height:1.6; }

/* badge */
.badge { display:inline-flex; align-items:center; gap:4px; padding:2px 9px;
         border-radius:5px; font-family:'JetBrains Mono',monospace;
         font-size:10px; font-weight:500; letter-spacing:.06em; }
.badge.bull { background:#052e16; color:#22c55e; }
.badge.bear { background:#2d0a0a; color:#ef4444; }
.badge.neut { background:#18181b; color:#71717a; }

/* chip */
.chip { display:inline-block; padding:2px 8px; background:#1c1c1e;
        border:1px solid #27272a; border-radius:4px;
        font-family:'JetBrains Mono',monospace; font-size:11px;
        font-weight:500; color:#60a5fa; margin:2px; }

/* horizon pill */
.hor-s { background:#1c1a0a; color:#eab308; padding:1px 7px; border-radius:4px;
         font-family:'JetBrains Mono',monospace; font-size:10px; }
.hor-m { background:#0a1c2e; color:#60a5fa; padding:1px 7px; border-radius:4px;
         font-family:'JetBrains Mono',monospace; font-size:10px; }
.hor-l { background:#0a1c0a; color:#4ade80; padding:1px 7px; border-radius:4px;
         font-family:'JetBrains Mono',monospace; font-size:10px; }

/* section header */
.sh { font-family:'JetBrains Mono',monospace; font-size:10px; letter-spacing:.14em;
      text-transform:uppercase; color:#52525b; padding-bottom:10px;
      border-bottom:1px solid #27272a; margin-bottom:18px; }

/* report */
.report-wrap { background:#111113; border:1px solid #27272a; border-radius:10px;
               padding:28px 36px; font-size:14px; line-height:1.85; }
.report-wrap h1 { font-size:20px; color:#fafafa; margin-bottom:12px; }
.report-wrap h2 { font-size:15px; color:#60a5fa; border-bottom:1px solid #27272a;
                  padding-bottom:6px; margin:24px 0 12px; }
.report-wrap h3 { font-size:13px; color:#f59e0b; margin:18px 0 8px; }
.report-wrap strong { color:#fafafa; }
.report-wrap p { color:#a1a1aa; }

/* price card */
.px-card { background:#111113; border:1px solid #27272a; border-radius:10px;
           padding:16px 14px; text-align:center; }
.px-tkr  { font-family:'JetBrains Mono',monospace; font-size:16px; font-weight:600; color:#60a5fa; }
.px-co   { font-size:10px; color:#52525b; margin:2px 0 10px; }
.px-val  { font-family:'JetBrains Mono',monospace; font-size:20px; font-weight:600; color:#fafafa; }
.px-up   { font-family:'JetBrains Mono',monospace; font-size:12px; color:#22c55e; }
.px-dn   { font-family:'JetBrains Mono',monospace; font-size:12px; color:#ef4444; }

/* configure form */
.cfg-card { background:#111113; border:1px solid #27272a; border-radius:10px; padding:20px 24px; }
.cfg-row  { display:flex; justify-content:space-between; align-items:center;
            padding:10px 0; border-bottom:1px solid #18181b; }
.cfg-row:last-child { border-bottom:none; }

/* deep dive */
.dd-box { background:#0c0c0f; border:1px solid #1e3a5f; border-radius:10px; padding:20px 24px; }
.dd-source { font-family:'JetBrains Mono',monospace; font-size:11px; color:#3b82f6;
             text-decoration:none; display:block; margin-bottom:4px; word-break:break-all; }

/* dataframe */
[data-testid="stDataFrame"] { border:1px solid #27272a; border-radius:8px; overflow:hidden; }

/* expander */
[data-testid="stExpander"] { background:#111113; border:1px solid #27272a; border-radius:8px; }

/* divider */
hr { border-color:#27272a; margin:20px 0; }

/* button */
.stButton>button {
    background:#18181b; border:1px solid #27272a; color:#a1a1aa;
    font-family:'JetBrains Mono',monospace; font-size:11px;
    border-radius:6px; transition:all .15s; }
.stButton>button:hover { border-color:#3b82f6; color:#3b82f6; }
</style>
""")


# ── Constants ─────────────────────────────────────────────────
CLUSTER_MAP = {
    "IREN": "Crypto Infrastructure", "HUT":  "Crypto Infrastructure",
    "COIN": "Crypto Infrastructure",
    "NVDA": "Semiconductors",        "TSM":  "Semiconductors", "MU": "Semiconductors",
    "MSFT": "Cloud / AI Platforms",  "GOOGL":"Cloud / AI Platforms",
    "AMZN": "Cloud / AI Platforms",  "META": "Cloud / AI Platforms",
    "DDOG": "Cloud / AI Platforms",
    "HOOD": "Fintech / Insurtech",   "LMND": "Fintech / Insurtech",
    "RDDT": "Fintech / Insurtech",
}

def fmt_vol(v):
    v = float(v or 0)
    if v >= 1_000_000: return f"${v/1_000_000:.1f}M"
    if v >= 1_000:     return f"${v/1_000:.0f}K"
    return f"${v:,.0f}"

def fmt_prob(p):
    try:    return f"{float(p):.0%}"
    except: return "—"

def horizon_pill(h):
    if h == "short-term": return '<span class="hor-s">SHORT</span>'
    if h == "long-term":  return '<span class="hor-l">LONG</span>'
    return '<span class="hor-m">MED</span>'

def badge(s):
    s = (s or "Neutral")
    if s == "Bullish": return '<span class="badge bull">▲ BULLISH</span>'
    if s == "Bearish": return '<span class="badge bear">▼ BEARISH</span>'
    return '<span class="badge neut">— NEUTRAL</span>'


# ── Data loaders ──────────────────────────────────────────────
@st.cache_data(ttl=120)
def load_signals(limit=200):
    res = (supabase.table("signal_feed").select("*")
           .order("impact_score", desc=True).limit(limit).execute())
    return res.data or []

@st.cache_data(ttl=300)
def load_reports(limit=20):
    res = (supabase.table("reports")
           .select("id,generated_at,tickers,signal_count,content,model_used")
           .order("generated_at", desc=True).limit(limit).execute())
    return res.data or []

@st.cache_data(ttl=60)
def load_stats():
    s = supabase.table("signals").select("id", count="exact").execute()
    r = supabase.table("reports").select("id", count="exact").execute()
    k = supabase.table("stocks").select("ticker", count="exact").execute()
    return {"signals": s.count or 0, "reports": r.count or 0, "stocks": k.count or 0}

@st.cache_data(ttl=60)
def load_stocks():
    res = supabase.table("stocks").select("*").order("ticker").execute()
    return res.data or []

def fetch_live_prices(tickers):
    prices = {}
    try:
        data = yf.download(tickers, period="2d", interval="1d",
                           group_by="ticker", auto_adjust=True, progress=False)
        for t in tickers:
            try:
                df  = data[t] if len(tickers) > 1 else data
                if df.empty: continue
                lat = float(df["Close"].iloc[-1])
                prv = float(df["Close"].iloc[-2]) if len(df) >= 2 else lat
                chg = ((lat - prv) / prv) * 100 if prv else 0
                prices[t] = {"price": lat, "chg": chg}
            except: continue
    except: pass
    return prices


# ── Sidebar ───────────────────────────────────────────────────
with st.sidebar:
    st.html("""
    <div class="logo-wrap">
        <div class="logo-tag">BIT Capital</div>
        <div class="logo-name">Signal Scanner</div>
        <div class="logo-sub">Polymarket Intelligence Platform</div>
    </div>""")
    st.markdown("---")

    stats = load_stats()
    st.metric("Active Signals",    stats["signals"])
    st.metric("Reports Generated", stats["reports"])
    st.metric("Holdings Tracked",  stats["stocks"])

    st.markdown("---")
    st.html(
        f"<div style='font-family:JetBrains Mono,monospace;font-size:10px;color:#3f3f46;'>"
        f"Last refresh<br>{datetime.now().strftime('%H:%M:%S')} local</div>")
    if st.button("↺  Refresh Data", width="stretch"):
        st.cache_data.clear(); st.rerun()


# ── Tabs ──────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📡  Overview",
    "🎯  Signal Feed",
    "📋  Reports",
    "🏦  Holdings",
    "⚙️  Configure",
])


# ════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ════════════════════════════════════════════════════════════
with tab1:
    try:
        import plotly.graph_objects as go
        HAS_PLOTLY = True
    except ImportError:
        HAS_PLOTLY = False

    st.html('<div class="sh">Portfolio Overview</div>')

    # Top metrics
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Total Signals",   stats["signals"])
    c2.metric("Reports",         stats["reports"])
    c3.metric("Holdings",        stats["stocks"])
    c4.metric("Last Run", datetime.now().strftime("%H:%M"))

    st.html("<br>")

    signals = load_signals(limit=200)

    if not signals:
        st.info("No signals yet. Run the pipeline: `python scheduler.py`")
    else:
        # Cluster signal summary — count unique markets per cluster
        st.html('<div class="sh">Active Signals by Cluster</div>')

        cluster_data = {}
        seen_mids = {}
        for s in signals:
            ticker  = s.get("ticker","")
            cluster = CLUSTER_MAP.get(ticker, "Other")
            mid     = s.get("market_id","")
            yes     = float(s.get("yes_price") or 0)
            if cluster not in cluster_data:
                cluster_data[cluster] = {"count":0,"markets":set(),"yes_sum":0}
            if mid not in cluster_data[cluster]["markets"]:
                cluster_data[cluster]["markets"].add(mid)
                cluster_data[cluster]["count"] += 1
                cluster_data[cluster]["yes_sum"] += yes

        cluster_order = ["Crypto Infrastructure","Semiconductors","Cloud/AI Platforms","Fintech/Insurtech"]
        display_clusters = {c: cluster_data[c] for c in cluster_order if c in cluster_data}

        cols = st.columns(len(display_clusters) or 1)
        for i, (cluster, data) in enumerate(display_clusters.items()):
            count   = data["count"]
            avg_yes = data["yes_sum"] / count if count else 0
            # Prob interpretation: contested = interesting
            if avg_yes >= 0.65:
                prob_lbl = "HIGH CONVICTION"; prob_col = "#f59e0b"
            elif avg_yes >= 0.40:
                prob_lbl = "CONTESTED"; prob_col = "#60a5fa"
            else:
                prob_lbl = "TAIL RISK"; prob_col = "#a78bfa"
            with cols[i]:
                st.html(f"""
                <div class="card" style="text-align:center">
                    <div style="font-size:11px;color:#52525b;margin-bottom:6px">{cluster}</div>
                    <div style="font-family:'JetBrains Mono',monospace;font-size:26px;
                                font-weight:600;color:#fafafa">{count}</div>
                    <div style="font-size:10px;color:#52525b;margin:2px 0 8px">unique markets</div>
                    <div style="font-size:10px;color:{prob_col};font-family:'JetBrains Mono',monospace">
                        avg YES {avg_yes:.0%} · {prob_lbl}
                    </div>
                </div>""")

        st.html("<br>")

        # Top signals — deduplicated by market, all tickers grouped
        st.html('<div class="sh">Top Signals This Run</div>')

        # Build one entry per unique market with all tickers
        seen_mid = set()
        market_groups = {}
        for s in signals:
            mid = s.get("market_id","")
            if mid not in market_groups:
                market_groups[mid] = {**s, "all_tickers": []}
            t = s.get("ticker","")
            if t and t not in market_groups[mid]["all_tickers"]:
                market_groups[mid]["all_tickers"].append(t)

        top_markets = list(market_groups.values())[:6]
        for m in top_markets:
            yes      = float(m.get("yes_price") or 0)
            vol      = m.get("volume", 0)
            end      = (m.get("end_date","") or "")[:10] or "—"
            tickers  = m.get("all_tickers", [m.get("ticker","?")])
            question = m.get("question","")
            event    = m.get("event_title","")
            chips    = " ".join(f'<span class="chip">{t}</span>' for t in tickers)

            import html as _html
            q_safe = _html.escape(question)
            e_safe = _html.escape(event)

            event_line = ""
            if e_safe and e_safe.lower() not in q_safe.lower()[:60]:
                event_line = f'<div style="font-size:10px;color:#3f3f46;margin-bottom:6px">↳ {e_safe}</div>'

            if yes >= 0.65:   yes_col = "#f59e0b"
            elif yes >= 0.40: yes_col = "#60a5fa"
            else:             yes_col = "#a78bfa"

            st.html(f"""
            <div class="card neut">
                <div class="card-q">{q_safe}</div>
                {event_line}
                <div style="margin:6px 0">{chips}</div>
                <div class="card-meta">
                    <span style="color:#52525b">YES</span>&nbsp;
                    <b style="color:{yes_col}">{fmt_prob(yes)}</b>
                    &nbsp;·&nbsp;
                    <span style="color:#52525b">Vol</span>&nbsp;{fmt_vol(vol)}
                    &nbsp;·&nbsp;
                    <span style="color:#52525b">Exp</span>&nbsp;{end}
                </div>
            </div>""")


# ════════════════════════════════════════════════════════════
# TAB 2 — SIGNAL FEED
# ════════════════════════════════════════════════════════════
with tab2:
    st.html('<div class="sh">Signal Feed — All Signals</div>')

        # ── Manual Market Explorer (NEW FEATURE) ──
    st.markdown("### 🔎 Explore Any Polymarket")

    col1, col2 = st.columns([4,1])

    user_market = col1.text_input(
        "Enter market (name or slug)",
        placeholder="e.g. Will Satoshi move any Bitcoin in 2026?"
    )

    run_btn = col2.button("Analyse")

    if run_btn and user_market:
        with st.spinner("Running analysis..."):
            try:
                event = fetch_event_from_text(user_market)
                news = fetch_news(user_market)
                analysis = run_gemini_analysis(event, news)
                impacts = extract_portfolio_impacts(analysis)

                st.markdown("---")

                # MARKET
                st.markdown(f"## 📊 {event.get('title','Unknown Market')}")
                if event.get("markets"):
                    st.write(event["markets"][0]["outcomes"])

                # ANALYSIS TEXT
                with st.expander("🧠 Full Analysis"):
                    st.markdown(analysis)

                # PORTFOLIO IMPACT
                # st.markdown("### 💼 Portfolio Impact")

                # if impacts:
                #     st.markdown("### 💼 Portfolio Impact")
                #     for p in impacts:
                #         st.markdown(f"- **{p['ticker']}** → {p['impact']}")

            except Exception as e:
                st.error(f"Failed: {e}")

    signals = load_signals(limit=200)

    if not signals:
        st.info("No signals in database.")
    else:
        # Filters — only on fields that are actually populated
        fc1, fc2, fc3 = st.columns([2, 2, 2])
        all_tickers  = sorted({s.get("ticker","") for s in signals if s.get("ticker")})
        all_clusters = ["All"] + sorted(set(CLUSTER_MAP.values()))

        sel_cluster = fc1.selectbox("Cluster", all_clusters, key="sf_cl")
        sel_ticker  = fc2.selectbox("Ticker",  ["All"]+all_tickers, key="sf_tk")
        min_vol_k   = fc3.number_input("Min Volume ($K)", 0, 10000, 0, step=10, key="sf_vol")

        # Build market groups FIRST from all signals (so ticker chips are complete)
        market_groups = {}
        for s in signals:
            mid = s.get("market_id","")
            if mid not in market_groups:
                market_groups[mid] = {**s, "all_tickers": [], "signal_id": s.get("signal_id")}
            t = s.get("ticker","")
            if t and t not in market_groups[mid]["all_tickers"]:
                market_groups[mid]["all_tickers"].append(t)

        all_markets = list(market_groups.values())

        # Apply filters on markets
        filtered = all_markets
        if sel_cluster != "All":
            filtered = [m for m in filtered
                        if any(CLUSTER_MAP.get(t,"") == sel_cluster for t in m["all_tickers"])]
        if sel_ticker != "All":
            filtered = [m for m in filtered if sel_ticker in m["all_tickers"]]
        if min_vol_k > 0:
            filtered = [m for m in filtered if float(m.get("volume") or 0) >= min_vol_k * 1000]

        st.caption(f"Showing {len(filtered)} unique markets")
        st.html("<br>")

        for m in filtered:
            yes      = float(m.get("yes_price") or 0)
            vol      = m.get("volume", 0)
            end      = (m.get("end_date","") or "")[:10] or "—"
            tickers  = m.get("all_tickers", [m.get("ticker","?")])
            sig_id   = m.get("signal_id")
            question = m.get("question","")
            event    = m.get("event_title","")
            chips    = " ".join(f'<span class="chip">{t}</span>' for t in tickers)

            # Escape to prevent question text breaking the HTML structure
            import html as _html
            q_safe = _html.escape(question)
            e_safe = _html.escape(event)

            # Show event only if it adds context beyond the question
            event_line = ""
            if e_safe and e_safe.lower() not in q_safe.lower()[:80]:
                event_line = f'<div style="font-size:10px;color:#3f3f46;margin-bottom:6px">↳ {e_safe}</div>'

            if yes >= 0.65:   yes_col = "#f59e0b"
            elif yes >= 0.40: yes_col = "#60a5fa"
            else:             yes_col = "#a78bfa"

            st.html(f"""
            <div class="card neut">
                <div class="card-q">{q_safe}</div>
                {event_line}
                <div style="margin:6px 0">{chips}</div>
                <div class="card-meta">
                    <span style="color:#52525b">YES</span>&nbsp;
                    <b style="color:{yes_col}">{fmt_prob(yes)}</b>
                    &nbsp;·&nbsp;
                    <span style="color:#52525b">Vol</span>&nbsp;{fmt_vol(vol)}
                    &nbsp;·&nbsp;
                    <span style="color:#52525b">Exp</span>&nbsp;{end}
                </div>
            </div>""")

            # Dig Deeper button
            if sig_id:
                btn_key = f"dd_{sig_id}"
                if st.button("🔍 Analyse", key=btn_key):
                    st.session_state[f"dd_open_{sig_id}"] = True

                if st.session_state.get(f"dd_open_{sig_id}"):
                    with st.spinner("Running deep dive analysis..."):
                        try:
                            from pipeline.dig_deeper_analysis import dig_deeper
                            result = dig_deeper(sig_id)
                            st.session_state[f"dd_result_{sig_id}"] = result
                        except Exception as e:
                            st.error(f"Analysis failed: {e}")

                result = st.session_state.get(f"dd_result_{sig_id}")
                if result and "error" not in result:
                    direction  = result.get("direction","Neutral")
                    d_color    = "#22c55e" if direction=="Bullish" else "#ef4444" if direction=="Bearish" else "#71717a"
                    cached     = result.get("from_cache", False)
                    analysis   = result.get("analysis_text","")
                    sources    = result.get("source_urls") or []
                    if isinstance(sources, str):
                        try:    sources = json.loads(sources)
                        except: sources = []
                    sources = [s for s in sources if s]

                    primary_ticker = tickers[0] if tickers else "—"
                    cached_label   = "📦 Cached" if cached else "⚡ Live"

                    # Header — show question context not just ticker
                    st.html(f"""
                    <div style="background:#111113;border:1px solid #1e3a5f;border-radius:10px 10px 0 0;
                                padding:14px 20px;margin-top:8px">
                        <div style="display:flex;justify-content:space-between;align-items:center;
                                    margin-bottom:6px">
                            <div style="font-family:'JetBrains Mono',monospace;font-size:10px;
                                        letter-spacing:.1em;text-transform:uppercase;color:#52525b">
                                Deep Dive · {primary_ticker} · {cached_label}
                            </div>
                            <div style="font-family:'JetBrains Mono',monospace;font-size:13px;
                                        font-weight:600;color:{d_color}">{direction.upper()}</div>
                        </div>
                        <div style="font-size:12px;color:#52525b;font-style:italic">
                            {question}
                        </div>
                    </div>""")

                    # Analysis body — use st.markdown so ## headings render properly
                    st.html(
                        f'<div style="background:#0c0c0f;border-left:1px solid #1e3a5f;'
                        f'border-right:1px solid #1e3a5f;padding:16px 20px;">'
                        f'</div>'
                    )
                    with st.container():
                        st.markdown(analysis)

                    # Sources footer
                    if sources:
                        src_links = "".join(
                            f'<a href="{u}" target="_blank" style="display:block;'
                            f'font-family:JetBrains Mono,monospace;font-size:11px;'
                            f'color:#3b82f6;text-decoration:none;margin-bottom:6px;'
                            f'word-break:break-all;">↗ {u[:100]}</a>'
                            for u in sources[:4]
                        )
                        st.html(f"""
                        <div style="background:#0c0c0f;border:1px solid #1e3a5f;
                                    border-top:1px solid #1e2035;border-radius:0 0 10px 10px;
                                    padding:14px 20px">
                            <div style="font-family:'JetBrains Mono',monospace;font-size:10px;
                                        letter-spacing:.1em;text-transform:uppercase;
                                        color:#3f3f46;margin-bottom:10px">News Sources</div>
                            {src_links}
                        </div>""")
                    else:
                        st.html("""
                        <div style="background:#0c0c0f;border:1px solid #1e3a5f;
                                    border-radius:0 0 10px 10px;padding:12px 20px">
                            <span style="font-family:'JetBrains Mono',monospace;
                                         font-size:11px;color:#3f3f46">No news sources found</span>
                        </div>""")


# ════════════════════════════════════════════════════════════
# TAB 3 — REPORTS
# ════════════════════════════════════════════════════════════
with tab3:
    st.html('<div class="sh">Alpha Reports</div>')

    reports = load_reports()
    if not reports:
        st.info("No reports generated yet. Run `python scheduler.py` to start the pipeline.")
    else:
        rc1, rc2 = st.columns([1, 2])
        with rc1:
            st.html('<div class="sh">Report History</div>')
            options = {}
            for r in reports:
                ts  = r.get("generated_at","")[:16].replace("T"," ")
                sc  = r.get("signal_count",0)
                lbl = f"{ts}  ·  {sc} signals"
                options[lbl] = r

            selected_lbl = st.radio("Select a report", list(options.keys()),
                                    label_visibility="collapsed")
            selected = options[selected_lbl]

            # Report metadata
            tickers = selected.get("tickers") or []
            st.html("<br>")
            st.html(f"""
            <div class="card">
                <div style="font-family:'JetBrains Mono',monospace;font-size:10px;
                            letter-spacing:.1em;text-transform:uppercase;color:#52525b;
                            margin-bottom:8px">Metadata</div>
                <div style="font-size:12px;color:#71717a">
                    <div>Signals: <b style="color:#fafafa">{selected.get('signal_count',0)}</b></div>
                    <div>Model: <b style="color:#fafafa">{selected.get('model_used','—')}</b></div>
                    <div style="margin-top:10px">Tickers covered:</div>
                    <div style="margin-top:6px">{"".join(f'<span class="chip">{t}</span>' for t in tickers)}</div>
                </div>
            </div>""")

            # Download
            st.download_button(
                "⬇  Download .md",
                data=selected.get("content",""),
                file_name=f"bit_alpha_{selected_lbl[:10].replace(' ','_')}.md",
                mime="text/markdown",
                width="stretch",
            )

        with rc2:
            st.html('<div class="sh">Report Content</div>')
            content = selected.get("content","")
            st.html(f'<div class="report-wrap">')
            st.markdown(content)
            st.html('</div>')


# ════════════════════════════════════════════════════════════
# TAB 4 — HOLDINGS (live prices + signal heatmap)
# ════════════════════════════════════════════════════════════
with tab4:
    st.html('<div class="sh">BIT Capital Holdings</div>')

    stocks  = load_stocks()
    signals = load_signals(limit=200)
    tickers = [s["ticker"] for s in stocks]

    # Live prices
    with st.spinner("Fetching live prices..."):
        prices = fetch_live_prices(tickers)

    # Signal counts per ticker
    sig_counts = {}
    sig_sents  = {}
    for s in signals:
        t = s.get("ticker","")
        sig_counts[t] = sig_counts.get(t, 0) + 1
        sents = sig_sents.setdefault(t, {"Bullish":0,"Bearish":0,"Neutral":0})
        sents[s.get("sentiment","Neutral")] = sents.get(s.get("sentiment","Neutral"),0) + 1

    # Grid — 4 per row
    per_row = 4
    rows    = [stocks[i:i+per_row] for i in range(0, len(stocks), per_row)]

    for row in rows:
        cols = st.columns(per_row)
        for i, stock in enumerate(row):
            t    = stock["ticker"]
            p    = prices.get(t, {})
            px   = p.get("price", 0)
            chg  = p.get("chg", 0)
            live = bool(px)

            # Dominant sentiment
            sents = sig_sents.get(t, {})
            n_sig = sig_counts.get(t, 0)
            bull  = sents.get("Bullish", 0)
            bear  = sents.get("Bearish", 0)
            if bull > bear:   sent_color = "#22c55e"; sent_lbl = f"▲ {bull}B / {bear}b"
            elif bear > bull: sent_color = "#ef4444"; sent_lbl = f"▼ {bear}B / {bull}b"
            else:             sent_color = "#71717a"; sent_lbl = f"— {n_sig} signals"

            chg_class = "px-up" if chg >= 0 else "px-dn"
            chg_str   = f"{'▲' if chg>=0 else '▼'} {abs(chg):.2f}%"
            px_str    = f"${px:,.2f}" if px else "—"

            with cols[i]:
                st.html(f"""
                <div class="px-card">
                    <div class="px-tkr">{t}</div>
                    <div class="px-co">{stock.get('company_name','')}</div>
                    <div class="px-val">{px_str}</div>
                    <div class="{chg_class}">{chg_str}</div>
                    <div style="margin-top:8px;font-size:10px;color:{sent_color};
                                font-family:'JetBrains Mono',monospace">{sent_lbl}</div>
                    <div style="font-size:10px;color:#3f3f46;margin-top:2px">
                        {stock.get('sector','')}
                    </div>
                </div>""")

    st.html("<br>")

    # Signal count per holding — simple clean view
    st.html('<div class="sh">Signal Coverage by Holding</div>')
    coverage_rows = []
    for stock in stocks:
        t   = stock["ticker"]
        n   = sig_counts.get(t, 0)
        px  = prices.get(t, {}).get("price", 0)
        chg = prices.get(t, {}).get("chg", 0)
        coverage_rows.append({
            "Ticker":  t,
            "Company": stock.get("company_name",""),
            "Cluster": CLUSTER_MAP.get(t,""),
            "Price":   f"${px:,.2f}" if px else "—",
            "1D Chg":  f"{'▲' if chg>=0 else '▼'} {abs(chg):.2f}%",
            "Signals": n,
            "Thesis":  stock.get("thesis",""),
        })

    st.dataframe(
        pd.DataFrame(coverage_rows),
        width="stretch",
        hide_index=True,
        column_config={
            "Thesis":  st.column_config.TextColumn(width="large"),
            "Signals": st.column_config.NumberColumn(format="%d"),
        }
    )


# ════════════════════════════════════════════════════════════
# TAB 5 — CONFIGURE
# ════════════════════════════════════════════════════════════
with tab5:
    st.html('<div class="sh">Portfolio Configuration</div>')

    cfg_tab1, cfg_tab2 = st.tabs(["🏦  Holdings", "➕  Add Holding"])

    with cfg_tab1:
        stocks = load_stocks()
        st.caption(f"{len(stocks)} holdings configured")
        st.html("<br>")

        # Group by cluster
        by_cluster = {}
        for s in stocks:
            c = CLUSTER_MAP.get(s["ticker"], "Other")
            by_cluster.setdefault(c, []).append(s)

        for cluster, cluster_stocks in by_cluster.items():
            st.markdown(f"**{cluster}**")
            for stock in cluster_stocks:
                col_a, col_b, col_c = st.columns([1, 3, 1])
                with col_a:
                    st.html(f'<span class="chip">{stock["ticker"]}</span>')
                with col_b:
                    st.markdown(
                        f'<div style="font-size:12px;color:#a1a1aa;padding:8px 0">'
                        f'{stock.get("company_name","")} · '
                        f'<span style="color:#52525b">{stock.get("sector","")}</span><br>'
                        f'<span style="font-size:11px;color:#52525b">{stock.get("thesis","")}</span>'
                        f'</div>')
                with col_c:
                    active = stock.get("active", True)
                    status = "🟢 Active" if active else "🔴 Inactive"
                    st.markdown(
                        f'<div style="font-size:11px;color:#52525b;padding:10px 0">{status}</div>')
            st.markdown("---")

    with cfg_tab2:
        st.html('<div class="sh">Add a New Holding</div>')
        st.caption("New holdings are saved to the stocks table and will be picked up on the next pipeline run.")

        from utils.supabase_client import get_service_client
        svc = get_service_client()

        with st.form("add_holding"):
            c1, c2 = st.columns(2)
            new_ticker  = c1.text_input("Ticker", placeholder="e.g. PLTR").upper()
            new_name    = c2.text_input("Company Name", placeholder="e.g. Palantir Technologies")
            c3, c4 = st.columns(2)
            new_sector  = c3.selectbox("Sector", [
                "Crypto Mining","Semiconductors","Cloud/AI","Fintech",
                "Insurtech","Social Media","Cloud Software","Crypto Exchange","Other"])
            new_cluster = c4.selectbox("Cluster", list(set(CLUSTER_MAP.values())))
            new_thesis  = st.text_area("Investment Thesis",
                placeholder="Why does BIT Capital hold this? What macro factors drive it?",
                height=80)

            submitted = st.form_submit_button("Add Holding", width="stretch")
            if submitted:
                if not new_ticker or not new_name:
                    st.error("Ticker and company name are required.")
                else:
                    try:
                        svc.table("stocks").upsert({
                            "ticker":       new_ticker,
                            "company_name": new_name,
                            "sector":       new_sector,
                            "thesis":       new_thesis,
                            "active":       True,
                        }, on_conflict="ticker").execute()
                        st.success(f"✓ {new_ticker} — {new_name} added to holdings.")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to save: {e}")

        st.markdown("---")
        st.html('<div class="sh">Pipeline Configuration</div>')

        info_col1, info_col2 = st.columns(2)
        with info_col1:
            st.html("""
**Run the pipeline manually:**
```bash
python pipeline/run_pipeline.py --dry-run
python pipeline/run_pipeline.py
```

**Start the scheduler (every 6h):**
```bash
python scheduler.py
```
""")
        with info_col2:
            st.html("""
**Pipeline stages:**
1. `ingest.py` — fetch Polymarket markets
2. `stage1_filter.py` — remove noise by tags
3. `stage2_filter.py` — Gemini LLM classification
4. `report_generator.py` — generate alpha report
""")