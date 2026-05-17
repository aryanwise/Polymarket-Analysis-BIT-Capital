# BIT Capital — Polymarket Signal Scanner

> **Live Dashboard →** [bitcapitalscreener.streamlit.app](https://bitcapitalscreener.streamlit.app/)

> **Developer - Aryan Mishra**

A real-time equity intelligence platform that scans [Polymarket](https://polymarket.com) prediction markets and uses LLMs to surface actionable signals for a concentrated tech portfolio. Built for BIT Capital, a Berlin-based technology fund (~€500M AUM).

---

## What It Does

Polymarket prices collective probability estimates for future events. When those events overlap with what drives a stock's revenue, margin, or regulatory environment, they become equity signals. This pipeline automates that extraction end-to-end:

1. **Ingests** all active Polymarket markets via the Gamma API
2. **Filters** irrelevant noise (sports, politics, entertainment) using rule-based logic
3. **Classifies** remaining markets with Gemini/Groq — does this market affect our holdings?
4. **Stores** relevant signals in Supabase with affected ticker mappings
5. **Generates** a daily LLM-powered Alpha Report with cross-cluster synthesis
6. **Displays** everything in a live Streamlit dashboard with deep-dive analysis per signal

---

## Portfolio Coverage

| Cluster                   | Tickers                       | Thesis                                                    |
| ------------------------- | ----------------------------- | --------------------------------------------------------- |
| **Crypto Infrastructure** | IREN, HUT, COIN               | BTC miners + crypto exchange; rate & regulatory sensitive |
| **Semiconductors**        | NVDA, TSM, MU                 | AI chip stack; export control & Taiwan tail risks         |
| **Cloud / AI Platforms**  | MSFT, GOOGL, AMZN, META, DDOG | Azure/Gemini/AWS; antitrust, AI capex, ad market          |
| **Fintech / Insurtech**   | HOOD, LMND, RDDT              | Rate-sensitive fintech; AI regulation & data rights       |

---

## Architecture

```
scheduler.py                     ← orchestrates everything, runs every 6h
│
├── pipeline/extract.py          ← fetch all active Polymarket events
├── pipeline/stage1_filter.py    ← rule-based filter (volume, tags, resolution)
├── pipeline/stage2_filter.py    ← LLM classification (Gemini → Groq fallback)
├── pipeline/report_generator.py ← daily Alpha Report (Gemini → Groq fallback)
├── pipeline/dig_deeper_analysis.py ← on-demand deep dive w/ news search
├── pipeline/explore_polymarket_news.py ← ad-hoc market explorer
├── pipeline/real_time_price.py  ← Yahoo Finance price snapshots
├── pipeline/backtest.py         ← signal accuracy evaluation post-expiry
│
├── db/schema3.sql               ← Supabase schema (current)
├── utils/supabase_client.py     ← shared DB client
└── webapp/streamlit_app.py      ← frontend dashboard
```

### ETL Flow

```
Polymarket API  (~32,000 markets)
     │
     ▼
stage1_filter.py  →  ~75 markets  (free, rule-based, <1 second)
 [0a] Drop zero-volume
 [0b] Drop missing YES price
 [1]  Drop expired (end_date year < current year)
 [2]  Drop fully resolved (YES = 0.0 or 1.0 exactly)
 [3]  Drop near-certain (YES < 4% or > 96%)
 [4]  Drop irrelevant tags (sports, celebrity, weather, junk crypto)
 [5]  Dedup by event_id — signal quality = uncertainty×0.65 + volume×0.35
 [6]  Drop low volume (< $5k)
     │
     ▼
Incremental filter  →  ~7 new markets / ~70 known refreshed
 New markets  → sent to LLM
 Known markets → yes_price + volume PATCH only (95% LLM cost reduction)
     │
     ▼
stage2_filter.py  →  three-pass LLM
 Pass 1: Mistral Small  — binary SIGNAL/NOISE gate
         batch=20 · temp=0.0 · fail open (parse error → SIGNAL)
 Pass 2: Llama 4 Scout  — maps market to specific tickers
         batch=10 · temp=0.0 · fail closed (parse error → NOISE)
 Pass B: Llama 4 Scout  — enriches each signal
         batch=15 · temp=0.1 · sentiment + impact_score 1–10 + reasoning
     │
     ▼
Supabase  →  signals written/updated, prices refreshed
     │
     ▼
report_generator.py  →  Daily Alpha Report
 • Fetches top 30 signals ordered by impact_score
 • Injects live prices (Yahoo Finance: price, 1D/5D change, 52w range)
 • Crowd vs Reality: DDGS news search → divergence score for top 3 signals
 • Gemini 2.5 Flash (temp=0.1, 16k tokens) · Groq fallback
 • 6-section report saved to DB
     │
     ▼
streamlit_app.py  →  Live Dashboard
```

---

## Database Schema

Six core tables + one view (see `db/schema3.sql`):

| Table             | Purpose                                                     |
| ----------------- | ----------------------------------------------------------- |
| `stocks`          | 14 BIT Capital holdings with thesis                         |
| `signals`         | Enriched market × ticker pairs; central ETL output          |
| `signal_feed`     | **View** — joins `signals` + `stocks`, primary query target |
| `reports`         | Daily Alpha Reports (markdown + metadata)                   |
| `report_signals`  | Junction table: which signals fed which report              |
| `deep_dives`      | On-demand analysis results (cached 6h)                      |
| `signal_outcomes` | Backtesting results post-expiry                             |
| `stock_prices`    | Yahoo Finance price snapshots for historical lookup         |

---

## Setup

### Prerequisites

- Python ≥ 3.12
- Supabase account (free tier works)
- API keys: Gemini, Groq, Mistral, FMP (optional for analyst targets), Tavily (optional for news search)

### Installation

```bash
git clone https://github.com/your-username/polymarket-analysis-bit-capital
cd polymarket-analysis-bit-capital
uv sync  # or: pip install -r requirements.txt
```

### Environment Variables

Create a `.env` file:

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=your-anon-key
SUPABASE_SERVICE_KEY=your-service-key

GEMINI_API_KEY=your-gemini-key
GROQ_API_KEY=your-groq-key
TAVILY_API_KEY=your-tavily-key   # optional, used in explore tab
MISTRAL_API_KEY=your-mistral-key     # Pass 1 gate
FMP_API_KEY=your-fmp-key             # optional, analyst price targets
```

### Database

Run the schema against your Supabase project:

```bash
# In Supabase SQL editor, paste and run:
db/schema3.sql
```

Enable Row Level Security policies (already included in schema3.sql).

---

## Running

### One-shot run

```bash
python scheduler.py --once
```

### Scheduled (every 6h)

```bash
python scheduler.py
# or custom interval:
python scheduler.py --interval 4
```

### With custom ingest size

```bash
python run scheduler.py --once --max-events 10000
```

### Dry run (no DB writes)

```bash
python scheduler.py --once --dry-run
```

### Dashboard only

```bash
streamlit run webapp/streamlit_app.py
```

### Pipeline flags

```
--max-events N    Max Polymarket markets to ingest (default: 3000)
--dry-run         Run without writing to database
--skip-report     Skip Alpha Report generation
--once            Run once and exit
--interval N      Hours between scheduled runs (default: 6)
```

---

## Dashboard Tabs

| Tab                  | Description                                                   |
| -------------------- | ------------------------------------------------------------- |
| **Overview**         | Hero metrics, cluster summaries, top signals                  |
| **Signal Feed**      | Full signal list + inline deep-dive analysis + keyword search |
| **Reports**          | Browsable history of Alpha Reports with download              |
| **Holdings**         | Live price monitor + signal coverage per ticker               |
| **Crowd vs Reality** | News divergence analysis for high-score signals               |
| **Configure**        | Holdings management by cluster                                |

---

## Polymarket Explorer

The **Signal Feed** tab embeds an ad-hoc market explorer powered by `pipeline/explore_polymarket_news.py`. It lets you analyse **any** Polymarket event on demand — not just the ones surfaced by the automated pipeline — and instantly see how it relates to the BIT Capital portfolio.

### How to Use

Type any market name or slug into the search bar at the top of the Signal Feed tab and click **Analyse**:

```
e.g.  Will the Fed cut rates in June 2026?
      bitcoin-price-end-of-2026
      Will NVDA hit $200 by end of year?
```

The input is normalised (lowercased, special characters stripped, spaces converted to hyphens) and used to look up the event directly from the Polymarket Gamma API — no pipeline run required.

### What It Does Step by Step

```
User input
     │
     ▼
normalise_slug()
 • lowercase, strip non-alphanumeric, replace spaces with hyphens
 • "Will Fed cut rates?" → "will-fed-cut-rates"
     │
     ▼
Polymarket Gamma API
 • GET gamma-api.polymarket.com/events/slug/{slug}
 • Returns: event title, all sub-markets, YES/NO outcome prices
     │
     ├──────────────────────────────────┐
     ▼                                  ▼
Tavily news search               build_gemini_prompt()
 • advanced depth                 assembles three inputs:
 • top 5 articles                  1. market signal (title + outcomes)
 • title + content snippet         2. latest news (titles + snippets)
     │                              3. portfolio context (all 14 holdings)
     └──────────────┬───────────────┘
                    ▼
             run_analysis()
          Mistral Large (primary)
        Gemini 2.5 Flash Lite (fallback)
                    │
                    ▼
      extract_portfolio_impacts()
       • parses ## Portfolio Impact section
       • expects: TICKER → impact explanation
       • filters to valid BIT Capital tickers only
       • fallback regex catches TICKER: explanation
                    │
                    ▼
            Dashboard output
```

**Slug normalisation** converts any free-text input into the URL slug format Polymarket uses internally. This means you can paste either a plain English question or the raw slug directly from a Polymarket URL — both work.

**Event fetch** returns all sub-markets nested inside the event. A single Polymarket event (e.g. "Fed June 2026 Decision") typically contains multiple markets (e.g. "Will the Fed cut by 25bps?", "Will the Fed cut by 50bps?", "Will the Fed hold?"). All are surfaced.

**News search** uses Tavily's `advanced` depth mode, which retrieves fuller article content rather than just headlines. This grounds the LLM in current real-world information and gives it material to compare against the Polymarket probability.

**Prompt construction** enforces a strict reasoning chain. The LLM is explicitly told that for any portfolio impact it claims to identify, the following logic must hold:

```
[event outcome + probability] → [what changes] → [driver] → [business effect]
```

Vague or indirect connections are rejected. If no clear mechanism exists for a holding, the LLM is instructed to output nothing for that ticker rather than force a connection. The defined drivers are:

| Driver                                              | Applies To                |
| --------------------------------------------------- | ------------------------- |
| `crypto → BTC price / volatility / trading volumes` | IREN, HUT, COIN, HOOD     |
| `rates → margins / valuation`                       | LMND, HOOD, IREN, HUT     |
| `AI → compute demand / capex`                       | NVDA, TSM, MU, DDOG, MSFT |
| `ads → macro consumer spending`                     | GOOGL, META               |

**LLM selection** — the explorer uses **Mistral Large** as its primary model (stronger structured-output compliance for the `TICKER → impact` format) with Gemini as fallback. This is different from the scheduled pipeline, which uses Gemini primary given its better throughput for batch classification.

### Analysis Sections

The explorer produces four sections for every market:

| Section                     | What It Contains                                                                                                                                                                                                                                                   |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Agreement or Divergence** | Does the latest news align with or contradict what the Polymarket probability is pricing? A divergence between news sentiment and market probability is explicitly flagged as a potential alpha opportunity — the core insight the explorer is designed to surface |
| **Market Interpretation**   | What scenario is the crowd pricing in? What would need to happen for the probability to shift materially? Asks the LLM to reason about second-order effects, not just the headline outcome                                                                         |
| **Portfolio Impact**        | Up to 4 holdings with explicit transmission mechanisms. Each line: `TICKER → one-sentence explanation of the business effect`. Zero entries if no clear mechanism exists — the LLM is instructed not to manufacture connections                                    |
| **Trade Insight**           | One clear, direct takeaway sentence. No Bullish/Bearish labels — just the key implication in plain language                                                                                                                                                        |

### Running the Explorer Standalone (CLI)

You can also use the explorer without the dashboard:

```bash
python pipeline/explore_polymarket_news.py
# prompts:  Enter market: Will the Fed cut rates in June?
```

Terminal output format:

```
MARKET: Fed June 2026 Decision

📊 Market Signal
→ {'Yes': 0.67, 'No': 0.33}

📰 News Sentiment
→ Multiple Fed officials signal patience amid sticky inflation data...

💼 BIT Capital Impact
- HOOD → Rate cut would compress net interest income; currently earns ~$X on cash balances
- LMND → Insurance float returns decline with lower rates; margin compression likely
- IREN → Lower borrowing costs ease debt service on mining hardware; mild positive

🧠 Takeaway
→ Market pricing a 67% cut while news flow leans toward a hold — watch for repricing if June CPI surprises upside
```

### When to Use the Explorer vs the Signal Feed

| Situation                                                             | Use                                              |
| --------------------------------------------------------------------- | ------------------------------------------------ |
| Breaking event between pipeline runs (Fed decision, chip export news) | **Explorer** — instant, no wait                  |
| Market the pipeline filtered out (low volume, borderline tags)        | **Explorer** — bypasses all filters              |
| Understanding _why_ a signal appeared in the feed                     | **Deep Dive** button on the signal card          |
| Cross-referencing a signal against current news                       | **Deep Dive** button on the signal card          |
| Researching a market outside the BIT Capital watchlist                | **Explorer** — works for any Polymarket event    |
| Daily portfolio monitoring                                            | **Signal Feed** — auto-populated by the pipeline |

---

## Deep Dive (Per-Signal Analysis)

Separate from the explorer, each signal in the feed has a **"Compare Against News Sources..."** button that triggers `pipeline/dig_deeper_analysis.py`. This is anchored to a specific signal already in the database and uses the live YES probability as additional context.

**Steps:**

1. Checks `deep_dives` table for a cached result less than 6 hours old
2. Fetches the signal from `signal_feed` view
3. Builds 2–3 targeted DuckDuckGo search queries from the market question text
4. Fetches articles including body text for richer LLM context; deduplicates across queries
5. Runs Groq Llama 4 Scout to produce a three-section briefing
6. Extracts directional call (Bullish / Bearish / Neutral) scoped to the direction section only
7. Saves to `deep_dives` and renders inline under the signal card with source links

**Deep Dive sections:**

| Section                   | Content                                                                                            |
| ------------------------- | -------------------------------------------------------------------------------------------------- |
| **Agreement or Conflict** | Does news support or contradict the Polymarket probability? References specific headlines          |
| **Short-term Direction**  | Directional call for the ticker: Bullish / Bearish / Neutral + one catalyst to watch               |
| **Reasoning**             | Connects the YES% probability to what news says. What would need to change for the thesis to break |

---

## Crowd vs Reality

For the top 3 signals by impact score each run, the pipeline compares Polymarket crowd pricing against live mainstream news consensus to identify information gaps.

**How it works:**

1. DDGS multi-query news search per signal (3 targeted queries from question text)
2. Groq assesses news sentiment (Bullish / Bearish / Neutral) and confidence
3. Rule-based divergence calculation: Polymarket direction vs news direction
4. HIGH divergence = crowd and news pointing opposite directions = potential alpha
   **Divergence labels:**

| Label     | Meaning                                                            |
| --------- | ------------------------------------------------------------------ |
| 🔴 HIGH   | Polymarket and news pointing opposite directions — information gap |
| 🟡 MEDIUM | Uncertain signal, one side less confident                          |
| 🟢 LOW    | Crowd and news broadly aligned                                     |
| ⚪ NONE   | Fully aligned — consensus, no edge                                 |

---

## Backtesting

`pipeline/backtest.py` evaluates signal accuracy against resolved market outcomes.

**First data: June 17, 2026** — Fed meeting markets are the first to expire.

```bash
uv run pipeline/backtest.py           # evaluate + write results
uv run pipeline/backtest.py --report  # accuracy report only
uv run pipeline/backtest.py --dry-run # preview without DB writes
```

**Key metric:** Do impact_score 8–10 signals have higher directional accuracy than score 3–5? If yes, the scoring model generates real alpha.

---

## LLM Design Decisions

**Three-pass Stage 2.** Classification and enrichment are different cognitive tasks. Pass 1 (Mistral) is permissive and fast — binary yes/no on whether a market is thematically relevant. Pass 2 (Groq) is precise — maps to specific tickers. Pass B (Groq) reasons about P&L impact. Each model does one focused job; combining them hurts both.

**Fail open at the gate, fail closed at the classifier.** Mistral defaults to SIGNAL on parse error — missing a real signal is worse than one extra Groq call. Groq defaults to NOISE on parse error — nothing garbage should be written to the DB.

**Report reasoning deferred.** The report LLM receives all 17+ unique markets with live prices for all 14 holdings simultaneously. It can say "IREN is down 12% this week AND a new country buying Bitcoin is priced at 39% — this is an entry point." That cross-cluster synthesis is only possible with full context.

## **Incremental processing.** Markets already in `signals` are skipped by the LLM. Only new markets get classified. Known markets receive price/volume refreshes via PATCH — no LLM cost.

## Stage 1 Filter Logic

The tag blocklist in `pipeline/irrelevant_tags.py` covers ~500 tags across:

- Sports (NFL, NBA, MLB, EPL, F1, UFC, esports...)
- Entertainment (Oscars, Grammys, celebrity, box office...)
- Pure domestic politics (state primaries, local elections...)
- Weather and natural disasters
- Junk crypto (NFTs, memecoins, token launches...)
- Polymarket housekeeping tags
  The deduplication step keeps only the highest signal-quality market per event using:

```python
signal_quality = uncertainty * 0.65 + volume_score * 0.35
# uncertainty = 1 - abs(yes_price - 0.5) * 2  →  1.0 at 50%, 0.0 at extremes
# volume_score = min(volume / 1_000_000, 1.0)  →  capped at $1M
```

This prevents 20+ near-identical Fed rate threshold markets from all hitting Stage 2.

---

## Key Files

```
scheduler.py                 — entry point, orchestrator
pipeline/
  extract.py                 — Polymarket Gamma API ingestion
  stage1_filter.py           — rule-based filter
  stage2_filter.py           — LLM classifier
  irrelevant_tags.py         — tag blocklist (~500 tags)
  report_generator.py        — Alpha Report generator
  dig_deeper_analysis.py     — on-demand per-signal deep dive
  explore_polymarket_news.py — ad-hoc Polymarket explorer
  real_time_price.py         — Yahoo Finance price cache
  backtest.py                — signal accuracy evaluator
  backfill_enrichment.py     — one-time enrichment script
db/
  schema3.sql                — current production schema
  schema2.sql                — previous schema (reference)
  schema.sql                 — initial schema (reference)
utils/
  supabase_client.py         — shared Supabase client
webapp/
  streamlit_app.py           — full dashboard (~800 lines)
```

---

## Tech Stack

| Layer                   | Technology                                        |
| ----------------------- | ------------------------------------------------- |
| Data source             | Polymarket Gamma API                              |
| LLM — Pass 1 gate       | Mistral Small                                     |
| LLM — Pass 2 classifier | Groq Llama 4 Scout                                |
| LLM — Pass B enrichment | Groq Llama 4 Scout                                |
| LLM — report (primary)  | Gemini 2.5 Flash                                  |
| LLM — report (fallback) | Groq Llama 4 Scout                                |
| News search (explorer)  | Tavily (advanced depth)                           |
| News search (deep dive) | DuckDuckGo (ddgs), multi-query with deduplication |
| Stock prices            | Yahoo Finance (yfinance) + FMP analyst targets    |
| Database                | Supabase (PostgreSQL + RLS)                       |
| Frontend                | Streamlit                                         |
| Scheduling              | Python `sched` + GitHub Actions (every 6h)        |
| Deployment              | Streamlit Cloud                                   |

---

## Deployment (Streamlit Cloud)

The app is live at **[bitcapitalscreener.streamlit.app](https://bitcapitalscreener.streamlit.app/)**.

For your own deployment:

1. Fork this repo
2. Connect to [share.streamlit.io](https://share.streamlit.io)
3. Set main file: `webapp/streamlit_app.py`
4. Add all environment variables in Streamlit Secrets
5. Run the scheduler separately — Streamlit Cloud does not support background processes. Use GitHub Actions (`.github/workflows/pipeline.yml` included) or a cron job on Railway/Render

---
