"""
webapp/streamlit_app.py
BIT Capital — Polymarket Signal Scanner
"""
import os, sys, json, html as _html
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import yfinance as yf
import streamlit as st
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from pipeline.explore_polymarket_news import (
    fetch_event_from_text,
    fetch_news,
    run_analysis,
    extract_portfolio_impacts
)
from utils.supabase_client import get_anon_client
supabase = get_anon_client()

# ── Page Config ──────────────────────────────────────────────
st.set_page_config(
    page_title="BIT Capital · Signal Scanner",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Design System ────────────────────────────────────────────
st.html("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');

/* ── Base ── */
:root {
    --bg-primary: #0f1117;
    --bg-secondary: #161922;
    --bg-tertiary: #1c1f2a;
    --bg-elevated: #22263a;
    --border-subtle: rgba(255,255,255,0.08);
    --border-medium: rgba(255,255,255,0.14);
    --text-primary: #f0f1f5;
    --text-secondary: #c4c8d0;     
    --text-tertiary: #8b92a0;       
    --text-muted: #5c6370;          
    --accent-cyan: #22d3ee;
    --accent-amber: #fbbf24;        /* slightly warmer gold */
    --accent-emerald: #34d399;
    --accent-rose: #fb7185;
    --accent-violet: #a78bfa;
    --accent-blue: #60a5fa;
    --shadow-sm: 0 1px 2px rgba(0,0,0,0.3);
    --shadow-md: 0 4px 12px rgba(0,0,0,0.4);
    --radius-sm: 6px;
    --radius-md: 10px;
    --radius-lg: 14px;
}

html, body, [class*="css"] { 
    font-family: 'Inter', sans-serif; 
    background: var(--bg-primary); 
    color: var(--text-primary);
    -webkit-font-smoothing: antialiased;
}

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--bg-elevated); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: var(--text-muted); }

/* ── Sidebar ── */
[data-testid="stSidebar"] { 
    background: linear-gradient(180deg, #0c0e14 0%, #11131a 100%); 
    border-right: 1px solid var(--border-subtle); 
}
[data-testid="stSidebar"] .stMarkdown p,
[data-testid="stSidebar"] label { 
    color: var(--text-tertiary); 
    font-size: 12px; 
    font-weight: 500;
}

/* ── Logo ── */
.logo-wrap { padding: 8px 0 28px; }
.logo-tag { 
    font-family: 'JetBrains Mono', monospace; 
    font-size: 10px; 
    letter-spacing: 0.2em;
    text-transform: uppercase; 
    color: var(--accent-cyan); 
    margin-bottom: 6px;
    opacity: 0.9;
}
.logo-name { 
    font-size: 22px; 
    font-weight: 700; 
    color: var(--text-primary);
    letter-spacing: -0.02em;
}
.logo-sub { 
    font-size: 12px; 
    color: var(--text-muted); 
    margin-top: 4px;
    font-weight: 400;
}

/* ── Navigation / Tabs ── */
[data-testid="stTabs"] { 
    background: transparent; 
    border-bottom: 1px solid var(--border-subtle); 
    margin-bottom: 24px;
}
[data-testid="stTabs"] button {
    font-family: 'Inter', sans-serif;
    font-size: 13px;
    font-weight: 500;
    letter-spacing: 0.01em;
    color: var(--text-tertiary);
    padding: 12px 20px;
    border: none;
    background: transparent;
    position: relative;
    transition: color 0.2s ease;
}
[data-testid="stTabs"] button:hover { color: var(--text-secondary); }
[data-testid="stTabs"] button[aria-selected="true"] { 
    color: var(--accent-cyan); 
    font-weight: 600;
}
[data-testid="stTabs"] button[aria-selected="true"]::after {
    content: '';
    position: absolute;
    bottom: 0;
    left: 20%;
    width: 60%;
    height: 2px;
    background: linear-gradient(90deg, transparent, var(--accent-cyan), transparent);
    border-radius: 2px;
}

/* ── Metrics ── */
[data-testid="metric-container"] {
    background: linear-gradient(145deg, var(--bg-secondary), var(--bg-tertiary));
    border: 1px solid var(--border-subtle);
    border-radius: var(--radius-md);
    padding: 20px;
    box-shadow: var(--shadow-sm);
    transition: transform 0.2s ease, box-shadow 0.2s ease;
}
[data-testid="metric-container"]:hover {
    transform: translateY(-1px);
    box-shadow: var(--shadow-md);
    border-color: var(--border-medium);
}
[data-testid="metric-container"] label {
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px !important;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--text-muted) !important;
    font-weight: 500;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: 'JetBrains Mono', monospace;
    font-size: 28px !important;
    font-weight: 600;
    color: var(--text-primary) !important;
    letter-spacing: -0.02em;
}
[data-testid="metric-container"] [data-testid="stMetricDelta"] { 
    font-size: 12px !important; 
    font-weight: 500;
}

/* ── Cards ── */
.card {
    background: linear-gradient(145deg, var(--bg-secondary), rgba(28,31,42,0.6));
    border: 1px solid var(--border-subtle);
    border-radius: var(--radius-md);
    padding: 20px;
    margin-bottom: 12px;
    transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
    position: relative;
    overflow: hidden;
}
.card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 1px;
    background: linear-gradient(90deg, transparent, rgba(255,255,255,0.08), transparent);
}
.card:hover {
    border-color: var(--border-medium);
    transform: translateY(-2px);
    box-shadow: var(--shadow-md);
}
.card.bull { border-left: 3px solid var(--accent-emerald); }
.card.bear { border-left: 3px solid var(--accent-rose); }
.card.neut { border-left: 3px solid var(--text-muted); }
.card.high-conviction { 
    border-left: 3px solid var(--accent-amber);
    background: linear-gradient(145deg, var(--bg-secondary), rgba(245,158,11,0.03));
}

.card-q {
    font-size: 14px;
    font-weight: 600;
    color: var(--text-primary);
    margin-bottom: 8px;
    line-height: 1.5;
    letter-spacing: -0.01em;
}
.card-ev {
    font-size: 12px;
    color: var(--text-secondary);   
    margin-bottom: 10px;
    line-height: 1.5;
    padding-left: 14px;
    border-left: 2px solid rgba(255,255,255,0.08);
}
.card-meta {
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    color: var(--text-tertiary);   
    display: flex;
    align-items: center;
    gap: 16px;
    flex-wrap: wrap;
}
.card-reason {
    font-size: 13px;
    color: var(--text-secondary);
    margin-top: 14px;
    padding-top: 14px;
    border-top: 1px solid var(--border-subtle);
    line-height: 1.7;
    font-style: normal;
}

/* ── Badges ── */
.badge {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 3px 10px;
    border-radius: 20px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.04em;
    border: 1px solid transparent;
}
.badge.bull { 
    background: rgba(52,211,153,0.10); 
    color: var(--accent-emerald);
    border-color: rgba(52,211,153,0.2);
}
.badge.bear { 
    background: rgba(251,113,133,0.10); 
    color: var(--accent-rose);
    border-color: rgba(251,113,133,0.2);
}
.badge.neut { 
    background: rgba(107,114,128,0.10); 
    color: var(--text-tertiary);
    border-color: rgba(107,114,128,0.2);
}
.badge.conviction { 
    background: rgba(245,158,11,0.10); 
    color: var(--accent-amber);
    border-color: rgba(245,158,11,0.2);
}

/* ── Chips ── */
.chip { 
    display: inline-flex;
    align-items: center;
    padding: 3px 10px;
    background: rgba(96,165,250,0.08);
    border: 1px solid rgba(96,165,250,0.15);
    border-radius: 5px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    font-weight: 500;
    color: var(--accent-blue);
    margin: 2px;
    transition: all 0.15s ease;
}
.chip:hover {
    background: rgba(96,165,250,0.15);
    border-color: rgba(96,165,250,0.3);
}

/* ── Horizon Pills ── */
.horizon-pill {
    display: inline-flex;
    align-items: center;
    padding: 2px 8px;
    border-radius: 4px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.04em;
}
.hor-s { background: rgba(234,179,8,0.12); color: #eab308; }
.hor-m { background: rgba(96,165,250,0.12); color: #60a5fa; }
.hor-l { background: rgba(74,222,128,0.12); color: #4ade80; }

/* ── Section Headers ── */
.sh {
    font-family: 'Inter', sans-serif;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--text-muted);       
    padding-bottom: 12px;
    border-bottom: 1px solid var(--border-subtle);
    margin-bottom: 20px;
    display: flex;
    align-items: center;
    gap: 8px;
}
.sh::before {
    content: '';
    display: block;
    width: 3px;
    height: 14px;
    background: var(--accent-cyan);
    border-radius: 2px;
}

/* ── Global Markdown Styling (reports, analysis, all content) ── */
[data-testid="stMarkdownContainer"] h1 {
    font-size: 22px;
    color: var(--text-primary);
    margin-bottom: 16px;
    font-weight: 700;
    letter-spacing: -0.02em;
    line-height: 1.3;
}
[data-testid="stMarkdownContainer"] h2 {
    font-size: 15px;
    color: var(--accent-blue);
    border-bottom: 1px solid var(--border-subtle);
    padding-bottom: 8px;
    margin: 28px 0 14px;
    font-weight: 600;
}
[data-testid="stMarkdownContainer"] h3 {
    font-size: 13px;
    color: var(--accent-amber);
    margin: 20px 0 10px;
    font-weight: 600;
}
[data-testid="stMarkdownContainer"] h4 {
    font-size: 12px;
    color: var(--accent-cyan);
    margin: 16px 0 8px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}
[data-testid="stMarkdownContainer"] strong {
    color: var(--text-primary);
    font-weight: 600;
}
[data-testid="stMarkdownContainer"] p {
    color: var(--text-secondary);
    line-height: 1.8;
    margin-bottom: 14px;
}
[data-testid="stMarkdownContainer"] ul, 
[data-testid="stMarkdownContainer"] ol {
    color: var(--text-secondary);
    line-height: 1.8;
    margin-bottom: 14px;
    padding-left: 20px;
}
[data-testid="stMarkdownContainer"] li {
    margin-bottom: 6px;
}
[data-testid="stMarkdownContainer"] code {
    background: var(--bg-tertiary);
    color: var(--accent-cyan);
    padding: 2px 6px;
    border-radius: 4px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
}
[data-testid="stMarkdownContainer"] pre {
    background: var(--bg-tertiary);
    border: 1px solid var(--border-subtle);
    border-radius: var(--radius-sm);
    padding: 16px;
    overflow-x: auto;
}
[data-testid="stMarkdownContainer"] pre code {
    background: transparent;
    padding: 0;
    color: var(--text-secondary);
}
[data-testid="stMarkdownContainer"] blockquote {
    border-left: 3px solid var(--accent-blue);
    margin: 16px 0;
    padding: 8px 16px;
    background: rgba(96,165,250,0.05);
    border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
}
[data-testid="stMarkdownContainer"] blockquote p {
    color: var(--text-secondary);
    margin: 0;
}
[data-testid="stMarkdownContainer"] a {
    color: var(--accent-blue);
    text-decoration: none;
    border-bottom: 1px solid rgba(96,165,250,0.2);
    transition: border-color 0.15s;
}
[data-testid="stMarkdownContainer"] a:hover {
    border-bottom-color: var(--accent-blue);
}
[data-testid="stMarkdownContainer"] hr {
    border-color: var(--border-subtle);
    margin: 24px 0;
}
[data-testid="stMarkdownContainer"] table {
    width: 100%;
    border-collapse: collapse;
    margin: 16px 0;
    font-size: 13px;
}
[data-testid="stMarkdownContainer"] th {
    background: var(--bg-tertiary);
    color: var(--text-secondary);
    font-weight: 600;
    text-align: left;
    padding: 10px 12px;
    border-bottom: 1px solid var(--border-medium);
    font-family: 'Inter', sans-serif;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}
[data-testid="stMarkdownContainer"] td {
    padding: 10px 12px;
    border-bottom: 1px solid var(--border-subtle);
    color: var(--text-primary);
}
[data-testid="stMarkdownContainer"] tr:hover td {
    background: rgba(255,255,255,0.02);
}

/* ── Price Cards ── */
.px-card { 
    background: linear-gradient(145deg, var(--bg-secondary), var(--bg-tertiary));
    border: 1px solid var(--border-subtle); 
    border-radius: var(--radius-md);
    padding: 20px 16px; 
    text-align: center;
    transition: all 0.25s ease;
    position: relative;
    overflow: hidden;
}
.px-card::after {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 2px;
    background: linear-gradient(90deg, transparent, var(--border-medium), transparent);
    opacity: 0.5;
}
.px-card:hover {
    border-color: var(--border-medium);
    transform: translateY(-2px);
    box-shadow: var(--shadow-md);
}
.px-tkr { 
    font-family: 'JetBrains Mono', monospace; 
    font-size: 16px; 
    font-weight: 600; 
    color: var(--accent-blue); 
}
.px-co { 
    font-size: 11px; 
    color: var(--text-tertiary);    
    margin: 4px 0 12px;
    font-weight: 500;
}
.px-val { 
    font-family: 'JetBrains Mono', monospace; 
    font-size: 22px; 
    font-weight: 600; 
    color: var(--text-primary);
    letter-spacing: -0.02em;
}
.px-up { 
    font-family: 'JetBrains Mono', monospace; 
    font-size: 12px; 
    color: var(--accent-emerald);
    font-weight: 500;
}
.px-dn { 
    font-family: 'JetBrains Mono', monospace; 
    font-size: 12px; 
    color: var(--accent-rose);
    font-weight: 500;
}

/* ── Configure ── */
.cfg-card { 
    background: var(--bg-secondary); 
    border: 1px solid var(--border-subtle); 
    border-radius: var(--radius-md); 
    padding: 24px; 
}
.cfg-row { 
    display: flex; 
    justify-content: space-between; 
    align-items: center;
    padding: 12px 0; 
    border-bottom: 1px solid rgba(255,255,255,0.04); 
}
.cfg-row:last-child { border-bottom: none; }

/* ── Deep Dive ── */
.dd-box { 
    background: linear-gradient(145deg, #0f1525, #131b2e); 
    border: 1px solid rgba(59,130,246,0.2); 
    border-radius: var(--radius-md); 
    padding: 24px; 
}
.dd-source { 
    font-family: 'JetBrains Mono', monospace; 
    font-size: 11px; 
    color: var(--accent-blue);
    text-decoration: none; 
    display: block; 
    margin-bottom: 6px; 
    word-break: break-all;
    opacity: 0.8;
    transition: opacity 0.15s;
}
.dd-source:hover { opacity: 1; }

/* ── DataFrame ── */
[data-testid="stDataFrame"] { 
    border: 1px solid var(--border-subtle); 
    border-radius: var(--radius-md); 
    overflow: hidden;
}
[data-testid="stDataFrame"] th {
    background: var(--bg-tertiary) !important;
    color: var(--text-tertiary) !important;  
    font-family: 'Inter', sans-serif;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
[data-testid="stDataFrame"] td {
    font-size: 13px;
    color: var(--text-primary);
    border-bottom: 1px solid var(--border-subtle) !important;
}

/* ── Expander ── */
[data-testid="stExpander"] { 
    background: var(--bg-secondary); 
    border: 1px solid var(--border-subtle); 
    border-radius: var(--radius-sm); 
}

/* ── Buttons ── */
.stButton>button {
    background: linear-gradient(145deg, var(--bg-tertiary), var(--bg-elevated));
    border: 1px solid var(--border-medium);
    color: var(--text-secondary);
    font-family: 'Inter', sans-serif;
    font-size: 12px;
    font-weight: 500;
    border-radius: var(--radius-sm);
    padding: 8px 16px;
    transition: all 0.2s ease;
}
.stButton>button:hover { 
    border-color: var(--accent-cyan); 
    color: var(--accent-cyan);
    background: linear-gradient(145deg, var(--bg-elevated), var(--bg-tertiary));
    box-shadow: 0 0 12px rgba(34,211,238,0.08);
}
.stButton>button:active { transform: scale(0.98); }

/* ── Form submit buttons (prevents stretch in columns) ── */
form button[kind="secondaryFormSubmit"] {
    width: auto !important;
    min-width: 100px !important;
    padding: 6px 16px !important;
    font-size: 11px !important;
    background: linear-gradient(145deg, var(--bg-tertiary), var(--bg-elevated)) !important;
    border: 1px solid var(--border-medium) !important;
    color: var(--text-secondary) !important;
    border-radius: var(--radius-sm) !important;
    font-family: 'Inter', sans-serif !important;
    transition: all 0.2s ease !important;
}
form button[kind="secondaryFormSubmit"]:hover {
    border-color: var(--accent-cyan) !important;
    color: var(--accent-cyan) !important;
    box-shadow: 0 0 12px rgba(34,211,238,0.08) !important;
}
        
/* ── Form Inputs ── */
.stTextInput input, .stTextArea textarea, .stSelectbox div[data-baseweb="select"] {
    background: var(--bg-tertiary) !important;
    border: 1px solid var(--border-subtle) !important;
    border-radius: var(--radius-sm) !important;
    color: var(--text-primary) !important;
    font-family: 'Inter', sans-serif !important;
}
.stTextInput input:focus, .stTextArea textarea:focus {
    border-color: var(--accent-cyan) !important;
    box-shadow: 0 0 0 2px rgba(34,211,238,0.1) !important;
}

/* ── Dividers ── */
hr { 
    border-color: var(--border-subtle); 
    margin: 24px 0; 
    opacity: 0.5;
}

/* ── Animations ── */
@keyframes fadeIn {
    from { opacity: 0; transform: translateY(8px); }
    to { opacity: 1; transform: translateY(0); }
}
.card, .px-card, [data-testid="metric-container"] {
    animation: fadeIn 0.4s ease-out forwards;
}

/* ── Signal Strength Bar ── */
.sig-strength {
    height: 3px;
    border-radius: 2px;
    background: var(--bg-elevated);
    overflow: hidden;
    margin-top: 10px;
}
.sig-strength-fill {
    height: 100%;
    border-radius: 2px;
    transition: width 0.6s ease;
}
/* ── Market monitor transitions ── */
@keyframes barGrow {
    from { width: 0%; }
}
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

CLUSTER_COLORS = {
    "Crypto Infrastructure": "#f59e0b",
    "Semiconductors": "#a78bfa",
    "Cloud / AI Platforms": "#22d3ee",
    "Fintech / Insurtech": "#34d399",
    "Other": "#f0f1f5",
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
    if h == "short-term": return '<span class="horizon-pill hor-s">SHORT</span>'
    if h == "long-term":  return '<span class="horizon-pill hor-l">LONG</span>'
    return '<span class="horizon-pill hor-m">MED</span>'

def badge(s):
    s = (s or "Neutral")
    if s == "Bullish": return '<span class="badge bull">▲ BULLISH</span>'
    if s == "Bearish": return '<span class="badge bear">▼ BEARISH</span>'
    return '<span class="badge neut">— NEUTRAL</span>'

def signal_strength_bar(yes_price):
    """Generate a visual signal strength indicator"""
    pct = float(yes_price or 0) * 100
    if pct >= 65:
        color = "var(--accent-amber)"
        width = min(pct, 100)
    elif pct >= 40:
        color = "var(--accent-blue)"
        width = pct
    else:
        color = "var(--accent-violet)"
        width = max(pct, 15)
    return f'<div class="sig-strength"><div class="sig-strength-fill" style="width:{width}%;background:{color}"></div></div>'


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
        <div style="display:flex;align-items:baseline;gap:1px;">
            <span style="font-family:'Georgia','Times New Roman',serif;font-size:25px;
                         font-weight:400;color:#f0f1f5;letter-spacing:-0.01em;">BIT</span>
            <span style="font-family:'Georgia','Times New Roman',serif;font-size:25px;
                         font-weight:200;color:#22d3ee;margin-left:1px;">/</span>
        </div>
        <div style="font-family:'Georgia','Times New Roman',serif;font-size:16px;
                     font-weight:400;color:#f0f1f5;letter-spacing:0.06em;
                     margin-top:2px;">Capital<span style="color:#f0f1f5;">.</span></div>
        <div style="font-size:14px;color:#f0f1f5;margin-top:10px;
                     font-family:'Inter',sans-serif;font-weight:400;">
            Polymarket Intelligence Platform
        </div>
    </div>""")
    st.markdown("---")


    stats = load_stats()
    
    # Styled metrics in sidebar
    st.html(f"""
    <div style="display:grid;gap:12px;margin-bottom:20px;">
        <div style="background:linear-gradient(145deg,#161922,#1c1f2a);border:1px solid rgba(255,255,255,0.06);
                    border-radius:10px;padding:16px;">
            <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#f0f1f5;
                        text-transform:uppercase;letter-spacing:0.1em;margin-bottom:6px;">Active Signals</div>
            <div style="font-family:'JetBrains Mono',monospace;font-size:24px;font-weight:600;color:#f0f1f5;">{stats["signals"]}</div>
        </div>
        <div style="background:linear-gradient(145deg,#161922,#1c1f2a);border:1px solid rgba(255,255,255,0.06);
                    border-radius:10px;padding:16px;">
            <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#f0f1f5;
                        text-transform:uppercase;letter-spacing:0.1em;margin-bottom:6px;">Reports</div>
            <div style="font-family:'JetBrains Mono',monospace;font-size:24px;font-weight:600;color:#f0f1f5;">{stats["reports"]}</div>
        </div>
        <div style="background:linear-gradient(145deg,#161922,#1c1f2a);border:1px solid rgba(255,255,255,0.06);
                    border-radius:10px;padding:16px;">
            <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#f0f1f5;
                        text-transform:uppercase;letter-spacing:0.1em;margin-bottom:6px;">Holdings</div>
            <div style="font-family:'JetBrains Mono',monospace;font-size:24px;font-weight:600;color:#f0f1f5;">{stats["stocks"]}</div>
        </div>
    </div>
    """)

    st.markdown("---")
    
    # Status indicator
    now = datetime.now().strftime('%H:%M:%S')
    st.html(
        f"<div style='font-family:JetBrains Mono,monospace;font-size:11px;color:#4b5563;"
        f"display:flex;align-items:center;gap:8px;margin-bottom:16px;'>"
        f"<span style='width:6px;height:6px;background:#34d399;border-radius:50%;"
        f"box-shadow:0 0 6px rgba(52,211,153,0.4);'></span>"
        f"System Online · {now}</div>")
    
    if st.button("↻  Refresh Data", use_container_width=True):
        st.cache_data.clear(); st.rerun()

    st.markdown("---")
    st.html("""
    <div style="font-size:11px;color:#4b5563;line-height:1.6;">
        <div style="font-weight:600;color:#f0f1f5;margin-bottom:6px;">Quick Actions</div>
        <div style="display:flex;flex-direction:column;gap:4px;">
            <a href="#" style="color:#4b5563;text-decoration:none;padding:4px 0;transition:color 0.15s;" 
               onmouseover="this.style.color='#22d3ee'" onmouseout="this.style.color='#4b5563'">→ Run Pipeline</a>
            <a href="#" style="color:#4b5563;text-decoration:none;padding:4px 0;transition:color 0.15s;"
               onmouseover="this.style.color='#22d3ee'" onmouseout="this.style.color='#4b5563'">→ Export Data</a>
            <a href="#" style="color:#4b5563;text-decoration:none;padding:4px 0;transition:color 0.15s;"
               onmouseover="this.style.color='#22d3ee'" onmouseout="this.style.color='#4b5563'">→ Settings</a>
        </div>
    </div>
    """)


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
        import plotly.express as px
        HAS_PLOTLY = True
    except ImportError:
        HAS_PLOTLY = False

        # Hero metrics — custom clean cards (no broken delta pills)
    st.html('<div class="sh">Portfolio Overview</div>')
    
    c1, c2, c3, c4 = st.columns(4)
    
    with c1:
        st.html(f"""
        <div style="background:linear-gradient(145deg,#161922,#1c1f2a);border:1px solid rgba(255,255,255,0.06);
                    border-radius:10px;padding:20px;text-align:center;transition:all 0.2s;">
            <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#f0f1f5;
                        text-transform:uppercase;letter-spacing:0.12em;margin-bottom:10px;">Total Signals</div>
            <div style="font-family:'JetBrains Mono',monospace;font-size:28px;font-weight:600;
                        color:#f0f1f5;letter-spacing:-0.02em;">{stats["signals"]}</div>
            <div style="margin-top:8px;font-size:11px;color:#34d399;font-family:'JetBrains Mono',monospace;
                        background:rgba(52,211,153,0.08);display:inline-block;padding:2px 8px;border-radius:4px;">
                ▲ {max(0, stats['signals'] - 150)} this run
            </div>
        </div>
        """)
    with c2:
        st.html(f"""
        <div style="background:linear-gradient(145deg,#161922,#1c1f2a);border:1px solid rgba(255,255,255,0.06);
                    border-radius:10px;padding:20px;text-align:center;transition:all 0.2s;">
            <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#f0f1f5;
                        text-transform:uppercase;letter-spacing:0.12em;margin-bottom:10px;">Reports</div>
            <div style="font-family:'JetBrains Mono',monospace;font-size:28px;font-weight:600;
                        color:#f0f1f5;letter-spacing:-0.02em;">{stats["reports"]}</div>
            <div style="margin-top:8px;font-size:11px;color:#f0f1f5;font-family:'JetBrains Mono',monospace;">
                Generated
            </div>
        </div>
        """)
    with c3:
        st.html(f"""
        <div style="background:linear-gradient(145deg,#161922,#1c1f2a);border:1px solid rgba(255,255,255,0.06);
                    border-radius:10px;padding:20px;text-align:center;transition:all 0.2s;">
            <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#f0f1f5;
                        text-transform:uppercase;letter-spacing:0.12em;margin-bottom:10px;">Holdings</div>
            <div style="font-family:'JetBrains Mono',monospace;font-size:28px;font-weight:600;
                        color:#f0f1f5;letter-spacing:-0.02em;">{stats["stocks"]}</div>
            <div style="margin-top:8px;font-size:11px;color:#f0f1f5;font-family:'JetBrains Mono',monospace;">
                Tracked
            </div>
        </div>
        """)
    with c4:
        now_time = datetime.now().strftime("%H:%M")
        st.html(f"""
        <div style="background:linear-gradient(145deg,#161922,#1c1f2a);border:1px solid rgba(255,255,255,0.06);
                    border-radius:10px;padding:20px;text-align:center;transition:all 0.2s;">
            <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#f0f1f5;
                        text-transform:uppercase;letter-spacing:0.12em;margin-bottom:10px;">Last Run</div>
            <div style="font-family:'JetBrains Mono',monospace;font-size:28px;font-weight:600;
                        color:#f0f1f5;letter-spacing:-0.02em;">{now_time}</div>
            <div style="margin-top:8px;font-size:11px;color:#22d3ee;font-family:'JetBrains Mono',monospace;
                        display:flex;align-items:center;justify-content:center;gap:6px;">
                <span style="width:6px;height:6px;background:#22d3ee;border-radius:50%;
                            box-shadow:0 0 6px rgba(34,211,238,0.4);"></span> Live
            </div>
        </div>
        """)

    st.html("<div style='height:8px'></div>")

    signals = load_signals(limit=200)

    if not signals:
        st.info("No signals yet. Run the pipeline: `python scheduler.py`")
    else:
        # Cluster signal summary — redesigned as visual cards
        st.html('<div class="sh">Active Signals by Cluster</div>')

        cluster_data = {}
        for s in signals:
            ticker  = s.get("ticker","")
            cluster = CLUSTER_MAP.get(ticker, "Other")
            mid     = s.get("market_id","")
            yes     = float(s.get("yes_price") or 0)
            if cluster not in cluster_data:
                cluster_data[cluster] = {"count":0,"markets":set(),"yes_sum":0,"bull_count":0,"bear_count":0}
            if mid not in cluster_data[cluster]["markets"]:
                cluster_data[cluster]["markets"].add(mid)
                cluster_data[cluster]["count"] += 1
                cluster_data[cluster]["yes_sum"] += yes
            # Sentiment tracking
            sent = s.get("sentiment", "Neutral")
            if sent == "Bullish": cluster_data[cluster]["bull_count"] += 1
            elif sent == "Bearish": cluster_data[cluster]["bear_count"] += 1

        cluster_order = ["Crypto Infrastructure","Semiconductors","Cloud / AI Platforms","Fintech / Insurtech"]
        display_clusters = {c: cluster_data[c] for c in cluster_order if c in cluster_data}

        cols = st.columns(len(display_clusters) or 1)
        for i, (cluster, data) in enumerate(display_clusters.items()):
            count   = data["count"]
            avg_yes = data["yes_sum"] / count if count else 0
            bull = data["bull_count"]
            bear = data["bear_count"]
            cluster_color = CLUSTER_COLORS.get(cluster, "#f0f1f5")
            
            if avg_yes >= 0.65:
                prob_lbl = "HIGH CONVICTION"; prob_col = "#f59e0b"
            elif avg_yes >= 0.40:
                prob_lbl = "CONTESTED"; prob_col = "#60a5fa"
            else:
                prob_lbl = "TAIL RISK"; prob_col = "#a78bfa"
                
            # Calculate total volume for cluster
            total_vol = sum(float(s.get("volume") or 0) for s in signals 
                          if CLUSTER_MAP.get(s.get("ticker",""), "Other") == cluster)
            
            with cols[i]:
                st.html(f"""
                <div class="card" style="text-align:center; border-top: 2px solid {cluster_color};">
                    <div style="font-size:12px;color:#f0f1f5;margin-bottom:8px;font-weight:500;letter-spacing:0.02em;">{cluster}</div>
                    <div style="font-family:'JetBrains Mono',monospace;font-size:32px;
                                font-weight:600;color:#f0f1f5;letter-spacing:-0.02em;">{count}</div>
                    <div style="font-size:11px;color:#f0f1f5;margin:4px 0 8px;">unique markets</div>
                    <div style="font-size:12px;color:#9aa3b8;font-family:'JetBrains Mono',monospace;
                                margin-bottom:10px;">{fmt_vol(total_vol)} vol</div>
                    <div style="font-size:10px;color:{prob_col};font-family:'JetBrains Mono',monospace;
                                background:rgba(255,255,255,0.03);padding:4px 8px;border-radius:4px;display:inline-block;">
                        avg YES {avg_yes:.0%} · {prob_lbl}
                    </div>
                </div>""")
        st.html("<div style='height:16px'></div>")

        # Top signals — deduplicated by market, all tickers grouped
        st.html('<div class="sh">Top Signals This Run</div>')

        seen_mid = set()
        market_groups = {}
        for s in signals:
            mid = s.get("market_id","")
            if mid not in market_groups:
                market_groups[mid] = {**s, "all_tickers": []}
            t = s.get("ticker","")
            if t and t not in market_groups[mid]["all_tickers"]:
                market_groups[mid]["all_tickers"].append(t)

        # Sort by impact score if available, otherwise by volume
        top_markets = sorted(market_groups.values(), 
                           key=lambda x: float(x.get("impact_score") or x.get("volume") or 0), 
                           reverse=True)[:6]
                           
        for m in top_markets:
            yes      = float(m.get("yes_price") or 0)
            vol      = m.get("volume", 0)
            end      = (m.get("end_date","") or "")[:10] or "—"
            tickers  = m.get("all_tickers", [m.get("ticker","?")])
            question = m.get("question","")
            event    = m.get("event_title","")
            chips    = " ".join(f'<span class="chip">{t}</span>' for t in tickers)
            strength = signal_strength_bar(yes)

            q_safe = _html.escape(question)
            e_safe = _html.escape(event)

            event_line = ""
            if e_safe and e_safe.lower() not in q_safe.lower()[:60]:
                event_line = f'<div class="card-ev">↳ {e_safe}</div>'

            if yes >= 0.65:   
                yes_col = "#f59e0b"
                card_class = "card high-conviction"
            elif yes >= 0.40: 
                yes_col = "#60a5fa"
                card_class = "card neut"
            else:             
                yes_col = "#a78bfa"
                card_class = "card neut"

            st.html(f"""
            <div class="{card_class}">
                <div class="card-q">{q_safe}</div>
                {event_line}
                <div style="margin:8px 0">{chips}</div>
                <div class="card-meta">
                    <span style="display:flex;align-items:center;gap:6px;">
                        <span style="color:#f0f1f5">YES</span>
                        <b style="color:{yes_col};font-size:12px;">{fmt_prob(yes)}</b>
                    </span>
                    <span style="color:#4b5563">|</span>
                    <span style="display:flex;align-items:center;gap:6px;">
                        <span style="color:#f0f1f5">Vol</span>
                        <span style="color:#f0f1f5;">{fmt_vol(vol)}</span>
                    </span>
                    <span style="color:#4b5563">|</span>
                    <span style="display:flex;align-items:center;gap:6px;">
                        <span style="color:#f0f1f5">Exp</span>
                        <span style="color:#f0f1f5;">{end}</span>
                    </span>
                </div>
                {strength}
            </div>""")


# ════════════════════════════════════════════════════════════
# TAB 2 — SIGNAL FEED
# ════════════════════════════════════════════════════════════
with tab2:
    st.html('<div class="sh">Signal Feed — All Signals</div>')


    # ── Manual Market Explorer (flat — no wrapper divs) ──
    st.html("""
    <div style="font-size:13px;font-weight:600;color:#f0f1f5;margin-bottom:12px;
                display:flex;align-items:center;gap:8px;">
        <span style="color:#22d3ee;">🔎</span> Explore Any Polymarket
    </div>
    """)
    
    col1, col2 = st.columns([5, 1])
    with col1:
        user_market = st.text_input(
            "Enter market (name or slug)",
            placeholder="e.g. Will Satoshi move any Bitcoin in 2026?",
            label_visibility="collapsed"
        )
    with col2:
        # Use a form with custom submit to avoid button stretch
        with st.form(key="explorer_form", border=False):
            st.html("<div style='height:1px'></div>")
            # Empty text to align with input
            st.html("""
            <style>
            #root > div > div > div > div > section > div > div > div > div > div > div > div > div > div > div > div > div > div > form button {
                width: auto !important;
                min-width: 120px !important;
                padding: 8px 20px !important;
            }
            </style>
            """)
            run_btn = st.form_submit_button("🔍 Analyse")

    st.html("</div>")  # close the explorer card

    if run_btn and user_market:
        with st.spinner("Running analysis..."):
            try:
                event = fetch_event_from_text(user_market)
                news = fetch_news(user_market)
                analysis = run_analysis(event, news)
                impacts = extract_portfolio_impacts(analysis)

                st.markdown("---")
                st.markdown(f"## 📊 {event.get('title','Unknown Market')}")
                if event.get("markets"):
                    st.write(event["markets"][0]["outcomes"])

                with st.expander("🧠 Full Analysis"):
                    st.markdown(analysis)

            except Exception as e:
                st.error(f"Failed: {e}")

    st.html("<hr style='border-color:rgba(255,255,255,0.06);margin:24px 0;'>")

    signals = load_signals(limit=200)

    if not signals:
        st.info("No signals in database.")
    else:
        # Filters — redesigned as a clean filter bar
        st.html("""
        <div style="background:linear-gradient(145deg,#161922,#1c1f2a);border:1px solid rgba(255,255,255,0.06);
                    border-radius:10px;padding:16px 20px;margin-bottom:20px;">
        """)
        fc1, fc2, fc3, fc4 = st.columns([2, 2, 2, 1])
        all_tickers  = sorted({s.get("ticker","") for s in signals if s.get("ticker")})
        all_clusters = ["All"] + sorted(set(CLUSTER_MAP.values()))

        with fc1:
            sel_cluster = st.selectbox("Cluster", all_clusters, key="sf_cl", label_visibility="collapsed")
        with fc2:
            sel_ticker  = st.selectbox("Ticker",  ["All"]+all_tickers, key="sf_tk", label_visibility="collapsed")
        with fc3:
            min_vol_k   = st.number_input("Min Volume ($K)", 0, 10000, 0, step=10, key="sf_vol", label_visibility="collapsed")
        with fc4:
            st.html("<div style='height:28px'></div>")
            filter_active = st.checkbox("Active Only", value=True, key="sf_active")
        st.html("</div>")

        # Build market groups
        market_groups = {}
        for s in signals:
            mid = s.get("market_id","")
            if mid not in market_groups:
                market_groups[mid] = {**s, "all_tickers": [], "signal_id": s.get("signal_id")}
            t = s.get("ticker","")
            if t and t not in market_groups[mid]["all_tickers"]:
                market_groups[mid]["all_tickers"].append(t)

        all_markets = list(market_groups.values())

        # Apply filters
        filtered = all_markets
        if sel_cluster != "All":
            filtered = [m for m in filtered
                        if any(CLUSTER_MAP.get(t,"") == sel_cluster for t in m["all_tickers"])]
        if sel_ticker != "All":
            filtered = [m for m in filtered if sel_ticker in m["all_tickers"]]
        if min_vol_k > 0:
            filtered = [m for m in filtered if float(m.get("volume") or 0) >= min_vol_k * 1000]

        st.caption(f"Showing {len(filtered)} unique markets")
        st.html("<div style='height:8px'></div>")

        for m in filtered:
            yes      = float(m.get("yes_price") or 0)
            vol      = m.get("volume", 0)
            end      = (m.get("end_date","") or "")[:10] or "—"
            tickers  = m.get("all_tickers", [m.get("ticker","?")])
            sig_id   = m.get("signal_id")
            question = m.get("question","")
            event    = m.get("event_title","")
            chips    = " ".join(f'<span class="chip">{t}</span>' for t in tickers)
            strength = signal_strength_bar(yes)

            q_safe = _html.escape(question)
            e_safe = _html.escape(event)

            event_line = ""
            if e_safe and e_safe.lower() not in q_safe.lower()[:80]:
                event_line = f'<div class="card-ev">↳ {e_safe}</div>'

            if yes >= 0.65:   
                yes_col = "#f59e0b"
                card_class = "card high-conviction"
            elif yes >= 0.40: 
                yes_col = "#60a5fa"
                card_class = "card neut"
            else:             
                yes_col = "#a78bfa"
                card_class = "card neut"

            st.html(f"""
            <div class="{card_class}">
                <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px;">
                    <div class="card-q" style="flex:1;margin-right:12px;">{q_safe}</div>
                    <div style="font-family:'JetBrains Mono',monospace;font-size:11px;color:{yes_col};
                                background:rgba(255,255,255,0.03);padding:4px 10px;border-radius:6px;
                                white-space:nowrap;font-weight:600;">
                        YES {fmt_prob(yes)}
                    </div>
                </div>
                {event_line}
                <div style="margin:8px 0">{chips}</div>
                <div class="card-meta">
                    <span style="display:flex;align-items:center;gap:6px;">
                        <span style="color:#f0f1f5">Vol</span>
                        <span style="color:#f0f1f5;">{fmt_vol(vol)}</span>
                    </span>
                    <span style="color:#4b5563">|</span>
                    <span style="display:flex;align-items:center;gap:6px;">
                        <span style="color:#f0f1f5">Exp</span>
                        <span style="color:#f0f1f5;">{end}</span>
                    </span>
                </div>
                {strength}
            </div>""")

            # Dig Deeper button — form-based to prevent stretch
            if sig_id:
                with st.form(key=f"dd_form_{sig_id}", border=False):
                    col_btn, _ = st.columns([5, 1])
                    with col_btn:
                        submitted = st.form_submit_button("🔍 Compare Against News Sources...")
                        
                if submitted:
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
                    d_color    = "#34d399" if direction=="Bullish" else "#fb7185" if direction=="Bearish" else "#f0f1f5"
                    cached     = result.get("from_cache", False)
                    analysis   = result.get("analysis_text","")
                    sources    = result.get("source_urls") or []
                    if isinstance(sources, str):
                        try:    sources = json.loads(sources)
                        except: sources = []
                    sources = [s for s in sources if s]

                    primary_ticker = tickers[0] if tickers else "—"
                    cached_label   = "📦 Cached" if cached else "⚡ Live"

                    st.html(f"""
                    <div style="margin:4px 0 16px 20px;border-left:2px solid rgba(59,130,246,0.3);padding-left:16px;">
                        <div style="background:linear-gradient(145deg,#0f1525,#131b2e);
                                    border:1px solid rgba(59,130,246,0.15);
                                    border-radius:10px;overflow:hidden;">
                            <div style="padding:16px 20px;border-bottom:1px solid rgba(59,130,246,0.1);
                                        display:flex;justify-content:space-between;align-items:center;">
                                <div style="font-family:'JetBrains Mono',monospace;font-size:10px;
                                            letter-spacing:.1em;text-transform:uppercase;color:#4b5563">
                                    Deep Dive · {primary_ticker} · {cached_label}
                                </div>
                                <div style="font-family:'JetBrains Mono',monospace;font-size:13px;
                                            font-weight:600;color:{d_color};background:rgba(255,255,255,0.03);
                                            padding:4px 10px;border-radius:6px;">{direction.upper()}</div>
                            </div>
                            <div style="padding:20px;">
                    """)
                    with st.container():
                        st.markdown(analysis)
                    
                    if sources:
                        src_links = "".join(
                            f'<a href="{u}" target="_blank" style="display:block;'
                            f'font-family:JetBrains Mono,monospace;font-size:11px;'
                            f'color:#60a5fa;text-decoration:none;margin-bottom:8px;'
                            f'word-break:break-all;opacity:0.8;transition:opacity 0.15s;" '
                            f'onmouseover="this.style.opacity=1" onmouseout="this.style.opacity=0.8">'
                            f'↗ {u[:90]}{"..." if len(u)>90 else ""}</a>'
                            for u in sources[:4]
                        )
                        st.html(f"""
                            <div style="margin-top:16px;padding-top:16px;border-top:1px solid rgba(59,130,246,0.1);">
                                <div style="font-family:'JetBrains Mono',monospace;font-size:10px;
                                            letter-spacing:.1em;text-transform:uppercase;
                                            color:#4b5563;margin-bottom:10px">News Sources</div>
                                {src_links}
                            </div>
                        """)
                    st.html("</div></div>")


# ════════════════════════════════════════════════════════════
# TAB 3 — REPORTS
# ════════════════════════════════════════════════════════════
with tab3:
    st.html('<div class="sh">LLM-Powered Signal Reports</div>')

    reports = load_reports()
    if not reports:
        st.info("No reports generated yet. Run `python scheduler.py` to start the pipeline.")
    else:
        # Build report options
        options = {}
        for r in reports:
            ts  = r.get("generated_at","")[:16].replace("T"," ")
            sc  = r.get("signal_count",0)
            tickers_short = ", ".join(r.get("tickers", [])[:3])
            lbl = f"{ts}  ·  {sc} signals"
            if tickers_short:
                lbl += f"  ·  {tickers_short}"
            options[lbl] = r

        # ── Layout: sidebar list + main content ──
        rc1, rc2 = st.columns([1, 3])

        with rc1:
            st.html("""
            <div style="font-size:11px;font-weight:600;color:#f0f1f5;letter-spacing:0.1em;
                        text-transform:uppercase;margin-bottom:12px;">Report History</div>
            """)
            
            selected_lbl = st.radio(
                "Select a report",
                list(options.keys()),
                label_visibility="collapsed"
            )
            selected = options[selected_lbl]

            # Compact metadata card
            tickers = selected.get("tickers") or []
            st.html("<div style='height:8px'></div>")
            st.html(f"""
            <div style="background:linear-gradient(145deg,#161922,#1c1f2a);border:1px solid rgba(255,255,255,0.06);
                        border-radius:10px;padding:16px;">
                <div style="font-family:'JetBrains Mono',monospace;font-size:10px;
                            letter-spacing:0.1em;text-transform:uppercase;color:#f0f1f5;
                            margin-bottom:12px;">Metadata</div>
                <div style="display:grid;gap:10px;">
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                        <span style="font-size:11px;color:#f0f1f5;">Signals</span>
                        <span style="font-family:'JetBrains Mono',monospace;font-size:12px;
                                     color:#22d3ee;font-weight:600;">{selected.get('signal_count',0)}</span>
                    </div>
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                        <span style="font-size:11px;color:#f0f1f5;">Model</span>
                        <span style="font-family:'JetBrains Mono',monospace;font-size:11px;
                                     color:#f0f1f5;">{selected.get('model_used','—')}</span>
                    </div>
                    <div style="padding-top:10px;margin-top:4px;border-top:1px solid rgba(255,255,255,0.06);">
                        <div style="font-size:10px;color:#f0f1f5;margin-bottom:8px;text-transform:uppercase;
                                    letter-spacing:0.06em;">Tickers</div>
                        <div style="display:flex;flex-wrap:wrap;gap:4px;">
                            {"".join(f'<span class="chip">{t}</span>' for t in tickers)}
                        </div>
                    </div>
                </div>
            </div>
            """)

            st.download_button(
                "⬇  Download .md",
                data=selected.get("content",""),
                file_name=f"bit_alpha_{selected.get('generated_at','')[:10]}.md",
                mime="text/markdown",
                use_container_width=True,
            )

        with rc2:
            # Header bar
            gen_time = selected.get("generated_at","")[:16].replace("T"," ")
            st.html(f"""
            <div style="display:flex;align-items:center;justify-content:space-between;
                        margin-bottom:16px;padding-bottom:12px;border-bottom:1px solid rgba(255,255,255,0.06);">
                <div>
                    <div style="font-size:10px;font-family:'JetBrains Mono',monospace;color:#f0f1f5;
                                text-transform:uppercase;letter-spacing:0.1em;margin-bottom:4px;">
                        BIT Report · {gen_time}
                    </div>
                    <div style="font-size:18px;font-weight:700;color:#f0f1f5;letter-spacing:-0.01em;">
                        Portfolio Signal Analysis
                    </div>
                </div>
                <div style="display:flex;gap:6px;">
                    <span class="badge neut">{selected.get('signal_count',0)} signals</span>
                </div>
            </div>
            """)

            # Content — render markdown cleanly, CSS handles the card styling
            content = selected.get("content","")
            st.markdown(content)

# ════════════════════════════════════════════════════════════
# TAB 4 — HOLDINGS (Bloomberg-style Market Monitor)
# ════════════════════════════════════════════════════════════
with tab4:
    st.html('<div class="sh">BIT Capital Holdings</div>')

    stocks  = load_stocks()
    signals = load_signals(limit=200)
    
    # Fetch live prices
    with st.spinner("Fetching live prices..."):
        prices = fetch_live_prices([s["ticker"] for s in stocks])
    
    # Signal counts per ticker
    sig_counts = {}
    for s in signals:
        t = s.get("ticker","")
        sig_counts[t] = sig_counts.get(t, 0) + 1

    # Build monitor rows
    monitor_rows = []
    for stock in stocks:
        t    = stock["ticker"]
        p    = prices.get(t, {})
        px   = p.get("price", 0)
        chg  = p.get("chg", 0)
        n_sig = sig_counts.get(t, 0)
        cluster = CLUSTER_MAP.get(t, "Other")
        cluster_color = CLUSTER_COLORS.get(cluster, "#6b7280")
        
        monitor_rows.append({
            "ticker": t,
            "company": stock.get("company_name", ""),
            "cluster": cluster,
            "cluster_color": cluster_color,
            "price": px,
            "chg": chg,
            "signals": n_sig,
            "sector": stock.get("sector", ""),
        })

    # Sort by cluster then ticker
    monitor_rows.sort(key=lambda x: (x["cluster"], x["ticker"]))

    # ── Cluster grouping with monitor table ──
    current_cluster = None
    for row in monitor_rows:
        # Cluster header
        if row["cluster"] != current_cluster:
            current_cluster = row["cluster"]
            st.html(f"""
            <div style="display:flex;align-items:center;gap:10px;margin:24px 0 12px;">
                <div style="width:6px;height:6px;background:{row['cluster_color']};border-radius:50%;
                            box-shadow:0 0 8px {row['cluster_color']}40;"></div>
                <div style="font-size:11px;font-weight:600;color:#6e7a94;letter-spacing:0.12em;
                            text-transform:uppercase;">{row['cluster']}</div>
                <div style="height:1px;flex:1;background:linear-gradient(90deg,{row['cluster_color']}20,transparent);"></div>
            </div>
            """)

        # Ticker row with inline bar
        t = row["ticker"]
        px = row["price"]
        chg = row["chg"]
        n_sig = row["signals"]
        
        px_str = f"${px:,.2f}" if px else "—"
        chg_str = f"{chg:+.2f}%" if px else "—"
        chg_color = "#34d399" if chg >= 0 else "#f87171"
        chg_bg = "rgba(52,211,153,0.12)" if chg >= 0 else "rgba(248,113,113,0.12)"
        
        # Bar width proportional to magnitude (capped at 100%)
        bar_width = min(abs(chg) * 8, 100) if px else 0
        bar_color = "rgba(52,211,153,0.25)" if chg >= 0 else "rgba(248,113,113,0.25)"
        
        # Signal dot
        sig_dot = f'<span style="display:inline-block;width:5px;height:5px;background:{row["cluster_color"]};' \
                  f'border-radius:50%;margin-right:6px;box-shadow:0 0 4px {row["cluster_color"]}60;"></span>' if n_sig > 0 else \
                  '<span style="display:inline-block;width:5px;height:5px;background:#3e4558;border-radius:50%;margin-right:6px;"></span>'

        st.html(f"""
        <div style="display:flex;align-items:center;padding:8px 12px;margin:2px 0;
                    background:{'rgba(255,255,255,0.015)' if px else 'transparent'};
                    border-radius:6px;transition:background 0.15s;"
             onmouseover="this.style.background='rgba(255,255,255,0.04)'" 
             onmouseout="this.style.background='{'rgba(255,255,255,0.015)' if px else 'transparent'}'">
            
            <!-- Ticker -->
            <div style="width:70px;font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600;
                        color:#f0f1f5;">{sig_dot}{t}</div>
            
            <!-- Company (hidden on narrow) -->
            <div style="flex:1;min-width:120px;font-size:12px;color:#7a8399;
                        white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
                        padding-right:16px;">{row['company']}</div>
            
            <!-- Price -->
            <div style="width:90px;text-align:right;font-family:'JetBrains Mono',monospace;
                        font-size:13px;font-weight:600;color:#f0f1f5;">{px_str}</div>
            
            <!-- Change bar + value -->
            <div style="width:160px;display:flex;align-items:center;gap:8px;margin-left:16px;">
                <div style="flex:1;height:18px;background:#1a1d28;border-radius:3px;overflow:hidden;position:relative;">
                    <div style="position:absolute;{'right' if chg >= 0 else 'left'}:0;top:0;bottom:0;
                                width:{bar_width}%;background:{bar_color};border-radius:3px;
                                transition:width 0.6s ease;"></div>
                </div>
                <div style="width:65px;text-align:right;font-family:'JetBrains Mono',monospace;
                            font-size:11px;font-weight:600;color:{chg_color};">{chg_str}</div>
            </div>
            
            <!-- Signal count -->
            <div style="width:50px;text-align:right;font-family:'JetBrains Mono',monospace;
                        font-size:10px;color:#6e7a94;">{n_sig} sig</div>
        </div>
        """)

    st.html("<div style='height:16px'></div>")

    # ── Compact summary table (optional, for export) ──
    with st.expander("📊 Raw Data View", expanded=False):
        coverage_rows = []
        for row in monitor_rows:
            coverage_rows.append({
                "Ticker":  row["ticker"],
                "Company": row["company"],
                "Cluster": row["cluster"],
                "Price":   f"${row['price']:,.2f}" if row["price"] else "—",
                "1D Chg":  f"{'▲' if row['chg']>=0 else '▼'} {abs(row['chg']):.2f}%",
                "Signals": row["signals"],
            })
        st.dataframe(
            pd.DataFrame(coverage_rows),
            use_container_width=True,
            hide_index=True,
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
        st.html("<div style='height:8px'></div>")

        by_cluster = {}
        for s in stocks:
            c = CLUSTER_MAP.get(s["ticker"], "Other")
            by_cluster.setdefault(c, []).append(s)

        for cluster, cluster_stocks in by_cluster.items():
            cluster_color = CLUSTER_COLORS.get(cluster, "#f0f1f5")
            st.html(f"""
            <div style="display:flex;align-items:center;gap:10px;margin:20px 0 12px;">
                <div style="width:8px;height:8px;background:{cluster_color};border-radius:2px;"></div>
                <div style="font-size:13px;font-weight:600;color:#f0f1f5;">{cluster}</div>
                <div style="height:1px;flex:1;background:linear-gradient(90deg,{cluster_color}30,transparent);"></div>
            </div>
            """)
            
            for stock in cluster_stocks:
                col_a, col_b, col_c = st.columns([1, 4, 1])
                with col_a:
                    st.html(f'<span class="chip">{stock["ticker"]}</span>')
                with col_b:
                    # FIXED: use st.html instead of st.markdown
                    st.html(
                        f'<div style="font-size:13px;color:#f0f1f5;font-weight:500;padding:6px 0">'
                        f'{_html.escape(stock.get("company_name",""))}'
                        f'<span style="color:#4b5563;font-weight:400;margin-left:8px;">· {_html.escape(stock.get("sector",""))}</span>'
                        f'</div>'
                        f'<div style="font-size:12px;color:#f0f1f5;line-height:1.5;">{_html.escape(stock.get("thesis",""))}</div>'
                    )
                with col_c:
                    active = stock.get("active", True)
                    status_color = "#34d399" if active else "#fb7185"
                    status_bg = "rgba(52,211,153,0.08)" if active else "rgba(251,113,133,0.08)"
                    status = "Active" if active else "Inactive"
                    st.html(
                        f'<div style="display:inline-block;padding:3px 10px;background:{status_bg};'
                        f'border-radius:20px;font-size:11px;color:{status_color};'
                        f'font-weight:600;text-align:center;margin-top:8px;">{status}</div>'
                    )
            st.html("<div style='height:8px'></div>")