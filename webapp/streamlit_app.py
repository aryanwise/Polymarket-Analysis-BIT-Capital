"""
webapp/streamlit_app.py

Polymarket Signal Scanner — Analyst Web Interface.
Displays signals, reports, and raw markets from the database.
"""
import os
import json
import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# Configuration
# ============================================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

st.set_page_config(
    page_title="Polymarket Signal Scanner",
    page_icon="🔍",
    layout="wide",
)

# ============================================================
# Styling
# ============================================================
st.markdown("""
<style>
    .signal-card {
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 16px;
        margin: 8px 0;
        background: #fafafa;
    }
    .bullish { border-left: 4px solid #4CAF50; }
    .bearish { border-left: 4px solid #F44336; }
    .neutral { border-left: 4px solid #9E9E9E; }
    .mixed   { border-left: 4px solid #FF9800; }
    .metric-box {
        text-align: center;
        padding: 12px;
        border-radius: 8px;
        background: #f0f2f6;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================
# Sidebar: Configuration
# ============================================================
with st.sidebar:
    st.header("⚙️ Configuration")

    # Load stocks from DB
    try:
        stocks = supabase.table("stocks").select("*").eq("active", True).execute()
        stock_options = {s["ticker"]: s for s in stocks.data}
    except Exception:
        stock_options = {}
        st.warning("Could not load stocks from database.")

    selected_tickers = st.multiselect(
        "Filter by ticker:",
        options=list(stock_options.keys()),
        default=list(stock_options.keys())[:5] if stock_options else [],
    )

    direction_filter = st.selectbox(
        "Signal direction:",
        options=["all", "bullish", "bearish", "neutral", "mixed"],
    )

    min_score = st.slider(
        "Minimum relevance score:",
        min_value=0.0,
        max_value=1.0,
        value=0.3,
        step=0.05,
    )

    hours_back = st.slider(
        "Hours to look back:",
        min_value=1,
        max_value=168,  # 1 week
        value=24,
        step=1,
    )

    st.divider()

    # Quick Stats
    st.markdown("### 📊 Database Stats")
    try:
        total_markets = (
            supabase.table("markets")
            .select("id", count="exact")
            .eq("active", True)
            .execute()
            .count
        )
        total_signals = (
            supabase.table("signals")
            .select("id", count="exact")
            .execute()
            .count
        )
        total_reports = (
            supabase.table("reports")
            .select("id", count="exact")
            .execute()
            .count
        )

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Active Markets", f"{total_markets:,}")
        with col2:
            st.metric("Classified Signals", f"{total_signals:,}")
        with col3:
            st.metric("Reports Generated", f"{total_reports:,}")
    except Exception:
        st.warning("Could not load stats.")

    st.divider()
    st.caption("BIT Capital — Prediction Market Intelligence")


# ============================================================
# Tabs
# ============================================================
tab1, tab2, tab3, tab4 = st.tabs([
    "📡 Live Signals",
    "📑 Signal Reports",
    "📈 Signal Analytics",
    "🗄️ Raw Markets",
])

# ============================================================
# Helper Functions
# ============================================================
DIRECTION_EMOJI = {
    "bullish": "🟢",
    "bearish": "🔴",
    "neutral": "⚪",
    "mixed": "🟡",
}

def format_volume(vol) -> str:
    """Format volume as human-readable string."""
    if vol is None:
        return "$0"
    vol = float(vol)
    if vol >= 1_000_000:
        return f"${vol/1_000_000:.1f}M"
    elif vol >= 1_000:
        return f"${vol/1_000:.0f}K"
    return f"${vol:,.0f}"


def parse_outcomes(outcomes_raw):
    """Safely parse outcomes JSON."""
    if isinstance(outcomes_raw, str):
        try:
            return json.loads(outcomes_raw)
        except Exception:
            return []
    return outcomes_raw or []


# ============================================================
# Tab 1: Live Signals
# ============================================================
with tab1:
    st.header("Current Equity-Relevant Signals")

    # Build query
    try:
        query = (
            supabase.table("signals")
            .select(
                "*, "
                "markets!inner(question, outcomes, volume_total, end_date, category)"
            )
            .order("relevance_score", desc=True)
        )

        if selected_tickers:
            query = query.in_("stock_ticker", selected_tickers)
        if direction_filter != "all":
            query = query.eq("signal_direction", direction_filter)
        if min_score > 0:
            query = query.gte("relevance_score", min_score)

        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
        query = query.gte("created_at", cutoff)

        signals = query.limit(200).execute()

    except Exception as e:
        st.error(f"Error loading signals: {e}")
        signals = type("obj", (object,), {"data": []})()

    if not signals.data:
        st.info("No signals match your filters. Try broadening the criteria or running the filter pipeline first.")
    else:
        st.caption(f"Showing {len(signals.data)} signals")

        for s in signals.data:
            market = s.get("markets", {})
            outcomes = parse_outcomes(market.get("outcomes", "[]"))
            top_outcome = outcomes[0] if outcomes else {}
            direction = s.get("signal_direction", "neutral")

            # Build card
            with st.container():
                card_class = f"signal-card {direction}"
                st.markdown(f'<div class="{card_class}">', unsafe_allow_html=True)

                cols = st.columns([1, 7, 2, 2])

                with cols[0]:
                    emoji = DIRECTION_EMOJI.get(direction, "❓")
                    st.markdown(f"## {emoji}")
                    st.caption(direction.upper())

                with cols[1]:
                    st.markdown(f"**{s['stock_ticker']}** — Score: {s['relevance_score']:.2f}")
                    st.markdown(f"*{market.get('question', 'N/A')}*")
                    st.caption(s.get('reasoning', 'No reasoning provided'))

                with cols[2]:
                    top_price = top_outcome.get("price", 0)
                    st.metric(
                        label=top_outcome.get("name", "Price"),
                        value=f"{float(top_price):.0%}",
                    )
                    st.caption(f"Vol: {format_volume(market.get('volume_total', 0))}")

                with cols[3]:
                    # Metadata chips
                    llm_score = s.get("llm_score", "N/A")
                    kw_score = s.get("keyword_score", "N/A")
                    if isinstance(llm_score, (int, float)):
                        st.metric("LLM", f"{llm_score:.2f}")
                    if isinstance(kw_score, (int, float)):
                        st.metric("KW", str(int(kw_score)))

                # Expandable details
                with st.expander("Details"):
                    detail_cols = st.columns(3)

                    with detail_cols[0]:
                        st.markdown("**Matched Keywords**")
                        keywords = s.get("matched_keywords", [])
                        if keywords:
                            st.markdown(", ".join(f"`{k}`" for k in keywords))
                        else:
                            st.caption("None")

                    with detail_cols[1]:
                        st.markdown("**Themes**")
                        themes = s.get("themes", [])
                        if themes:
                            st.markdown(", ".join(f"`{t}`" for t in themes))
                        else:
                            st.caption("None")

                    with detail_cols[2]:
                        st.markdown("**Market Info**")
                        st.caption(f"Category: {market.get('category', 'N/A')}")
                        expiry = market.get("end_date", "N/A")
                        if expiry and expiry != "N/A":
                            try:
                                exp_date = datetime.fromisoformat(str(expiry).replace("Z", "+00:00"))
                                days_left = max((exp_date - datetime.now(timezone.utc)).days, 0)
                                st.caption(f"Expires: {expiry}")
                                st.caption(f"Days left: {days_left}")
                            except Exception:
                                st.caption(f"Expires: {expiry}")
                        else:
                            st.caption("Expires: N/A")

                st.markdown('</div>', unsafe_allow_html=True)

# ============================================================
# Tab 2: Signal Reports
# ============================================================
with tab2:
    st.header("Generated Analyst Reports")

    try:
        reports = (
            supabase.table("reports")
            .select("*")
            .order("generated_at", desc=True)
            .limit(20)
            .execute()
        )
    except Exception as e:
        st.error(f"Error loading reports: {e}")
        reports = type("obj", (object,), {"data": []})()

    if not reports.data:
        st.info("No reports generated yet. Run the report generator pipeline.")
        st.code("python pipeline/report_generator.py", language="bash")
    else:
        for report in reports.data:
            tickers = report.get("tickers", [])
            content = report.get("content", "")
            gen_at = report.get("generated_at", "")[:10]

            with st.expander(f"📄 {report['title']} — {gen_at}", expanded=(reports.data.index(report) == 0)):
                if tickers:
                    ticker_tags = " ".join([f"`{t}`" for t in tickers])
                    st.markdown(f"**Covered tickers:** {ticker_tags}")
                st.divider()
                st.markdown(content)

                # Download button
                st.download_button(
                    label="Download Report (Markdown)",
                    data=content,
                    file_name=f"polymarket_report_{gen_at}.md",
                    mime="text/markdown",
                )

# ============================================================
# Tab 3: Signal Analytics
# ============================================================
with tab3:
    st.header("Signal Analytics")

    try:
        # Fetch signals for analytics
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
        analytics_signals = (
            supabase.table("signals")
            .select("stock_ticker, signal_direction, relevance_score, themes, created_at")
            .gte("created_at", cutoff)
            .execute()
        )
        data = analytics_signals.data or []
    except Exception:
        data = []

    if not data:
        st.info("No signals for analytics in the selected time window.")
    else:
        df = pd.DataFrame(data)

        # --- Row 1: Direction Distribution & Score Distribution ---
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Direction Distribution")
            direction_counts = df["signal_direction"].value_counts()
            st.bar_chart(direction_counts)

        with col2:
            st.subheader("Score Distribution by Direction")
            if not df.empty:
                import plotly.express as px
                fig = px.box(
                    df,
                    x="signal_direction",
                    y="relevance_score",
                    color="signal_direction",
                    title="Relevance Score by Direction",
                )
                st.plotly_chart(fig, use_container_width=True)

        # --- Row 2: Ticker Breakdown & Timeline ---
        col3, col4 = st.columns(2)

        with col3:
            st.subheader("Signals per Ticker")
            ticker_counts = df["stock_ticker"].value_counts().head(10)
            st.bar_chart(ticker_counts)

        with col4:
            st.subheader("Average Score by Ticker (Top 10)")
            ticker_avg = df.groupby("stock_ticker")["relevance_score"].mean().sort_values(ascending=False).head(10)
            st.bar_chart(ticker_avg)

        # --- Row 3: Theme Analysis ---
        st.subheader("Theme Analysis")
        all_themes_list = []
        for themes in df["themes"]:
            if themes and isinstance(themes, list):
                all_themes_list.extend(themes)

        if all_themes_list:
            theme_df = pd.DataFrame(all_themes_list, columns=["theme"])
            theme_counts = theme_df["theme"].value_counts().head(10)
            st.bar_chart(theme_counts)
        else:
            st.caption("No theme data available")

        # --- Row 4: Recent Signals Table ---
        st.subheader("Recent Signals Table")
        display_df = df.rename(columns={
            "stock_ticker": "Ticker",
            "signal_direction": "Direction",
            "relevance_score": "Score",
            "created_at": "Time",
        })
        display_df["Score"] = display_df["Score"].apply(lambda x: f"{x:.2f}" if isinstance(x, (int, float)) else x)
        st.dataframe(
            display_df[["Ticker", "Direction", "Score", "Time"]].head(50),
            use_container_width=True,
            hide_index=True,
        )

# ============================================================
# Tab 4: Raw Markets
# ============================================================
with tab4:
    st.header("Active Polymarket Markets")

    # Filters
    col1, col2 = st.columns(2)
    with col1:
        raw_category = st.selectbox(
            "Filter by category:",
            options=["all", "politics", "economics", "technology", "crypto", "science", "business", "world"],
        )
    with col2:
        raw_volume_min = st.number_input(
            "Minimum volume ($):",
            min_value=0,
            value=50000,
            step=10000,
            format="%d",
        )

    try:
        query = (
            supabase.table("markets")
            .select("*")
            .eq("active", True)
            .eq("closed", False)
            .gte("volume_total", raw_volume_min)
            .order("volume_total", desc=True)
            .limit(100)
        )

        if raw_category != "all":
            query = query.eq("category", raw_category)

        markets = query.execute()

    except Exception as e:
        st.error(f"Error loading markets: {e}")
        markets = type("obj", (object,), {"data": []})()

    if markets.data:
        # Build display dataframe
        rows = []
        for m in markets.data:
            outcomes = parse_outcomes(m.get("outcomes", "[]"))
            top_outcome = outcomes[0] if outcomes else {}
            rows.append({
                "Question": m.get("question", "")[:100],
                "Category": m.get("category", "N/A"),
                "Top Outcome": top_outcome.get("name", "?"),
                "Probability": f"{float(top_outcome.get('price', 0)):.0%}",
                "Volume": m.get("volume_total", 0),
                "Liquidity": m.get("liquidity", 0),
                "Expiry": str(m.get("end_date", "N/A"))[:10] if m.get("end_date") else "N/A",
                "LLM Processed": "✅" if m.get("llm_processed") else "❌",
            })

        st.dataframe(
            rows,
            column_config={
                "Question": st.column_config.TextColumn("Question", width="large"),
                "Volume": st.column_config.NumberColumn("Volume", format="$%.0f"),
                "Liquidity": st.column_config.NumberColumn("Liquidity", format="$%.0f"),
            },
            use_container_width=True,
            hide_index=True,
        )
        st.caption(f"Showing top {len(rows)} markets")
    else:
        st.info("No markets match your filters.")


# ============================================================
# Footer
# ============================================================
st.divider()
st.caption(
    f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')} | "
    "Data sourced from Polymarket Gamma API | "
    "BIT Capital — Tech-Focused Investment Fund"
)