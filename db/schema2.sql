-- ============================================================
-- Polymarket Signal Scanner — Schema v2 
-- ============================================================


-- ── UTILITY ──────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ============================================================
-- TABLE 1: STOCKS
-- The 10 BIT Capital holdings we care about.
-- This is the master reference table everything else links to.
-- ============================================================
CREATE TABLE stocks (
    ticker       VARCHAR(10)  PRIMARY KEY,    -- e.g. 'TSM'
    company_name TEXT         NOT NULL,       -- e.g. 'Taiwan Semiconductor'
    sector       TEXT,                        -- e.g. 'Semiconductors'
    thesis       TEXT,                        -- why BIT Capital owns it
    active       BOOLEAN      DEFAULT TRUE,   -- set FALSE to stop tracking
    added_at     TIMESTAMPTZ  DEFAULT NOW()
);

INSERT INTO stocks (ticker, company_name, sector, thesis) VALUES
  ('IREN',  'IREN Limited',        'Crypto Mining',      'Bitcoin mining and AI data centers infrastructure.'),
  ('MSFT',  'Microsoft',           'Cloud/AI',           'Core AI infrastructure and enterprise software play.'),
  ('GOOGL', 'Alphabet Inc.',       'Cloud/AI',           'Search dominance and Gemini AI ecosystem.'),
  ('LMND',  'Lemonade Inc.',       'Insurtech',          'AI-driven insurtech disruptor.'),
  ('RDDT',  'Reddit Inc.',         'Social Media',       'Data source for LLM training and ad growth.'),
  ('MU',    'Micron Technology',   'Semiconductors',     'AI memory hardware supplier.'),
  ('TSM',   'TSMC',                'Semiconductors',     'Foundry for high-end AI semiconductors.'),
  ('HUT',   'Hut 8 Corp.',         'Crypto Mining',      'Diversified crypto infrastructure.'),
  ('HOOD',  'Robinhood Markets',   'Fintech',            'Gateway for retail crypto and equity trading.'),
  ('DDOG',  'Datadog Inc.',        'Cloud Observability','Cloud observability and security monitoring.');


-- ============================================================
-- TABLE 2: STOCK PRICES
-- Daily closing price snapshots as a fallback cache.
-- Frontend uses live Yahoo Finance first; falls back to this.
-- ============================================================
CREATE TABLE stock_prices (
    id           SERIAL       PRIMARY KEY,
    ticker       VARCHAR(10)  NOT NULL REFERENCES stocks(ticker),
    price        NUMERIC(12,4) NOT NULL,
    change_pct   NUMERIC(8,4),              -- % change vs previous close
    volume       BIGINT,
    source       TEXT         DEFAULT 'yahoo_finance',
    fetched_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Index for fast "get latest price for ticker" queries
CREATE INDEX idx_stock_prices_lookup
    ON stock_prices(ticker, fetched_at DESC);


-- ============================================================
-- TABLE 3: EVENTS
-- The parent Polymarket event (e.g. "FOMC April 2026 Decision").
-- One event groups multiple related markets together.
-- This is what shows as the card title in your dashboard.
-- ============================================================
CREATE TABLE events (
    id           TEXT         PRIMARY KEY,   -- Polymarket's event id
    title        TEXT         NOT NULL,      -- shown as card title in dashboard
    category     TEXT,                       -- our label: 'Macro/Fed', 'Tariffs', etc.
    tag_ids      INTEGER[],                  -- which tag IDs surfaced this event
    active       BOOLEAN      DEFAULT TRUE,
    closed       BOOLEAN      DEFAULT FALSE,
    end_date     TIMESTAMPTZ,
    fetched_at   TIMESTAMPTZ  DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TRIGGER trg_events_updated_at
  BEFORE UPDATE ON events
  FOR EACH ROW EXECUTE FUNCTION update_timestamp();

CREATE INDEX idx_events_category  ON events(category);
CREATE INDEX idx_events_active    ON events(active, closed);


-- ============================================================
-- TABLE 4: MARKETS
-- Individual Yes/No questions inside an event.
-- Shown when a user clicks on an event card in the dashboard.
-- ============================================================
CREATE TABLE markets (
    id           TEXT         PRIMARY KEY,   -- Polymarket's market id
    event_id     TEXT         NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    question     TEXT         NOT NULL,      -- the actual Yes/No question
    yes_price    NUMERIC(6,4) DEFAULT 0,     -- probability: 0.0 to 1.0
    no_price     NUMERIC(6,4) DEFAULT 0,
    volume       NUMERIC(20,2) DEFAULT 0,
    liquidity    NUMERIC(20,2) DEFAULT 0,
    end_date     TIMESTAMPTZ,
    active       BOOLEAN      DEFAULT TRUE,
    closed       BOOLEAN      DEFAULT FALSE,
    fetched_at   TIMESTAMPTZ  DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TRIGGER trg_markets_updated_at
  BEFORE UPDATE ON markets
  FOR EACH ROW EXECUTE FUNCTION update_timestamp();

CREATE INDEX idx_markets_event    ON markets(event_id);
CREATE INDEX idx_markets_volume   ON markets(volume DESC);
CREATE INDEX idx_markets_active   ON markets(active, closed);


-- ============================================================
-- TABLE 5: SIGNALS
-- LLM output from your EquitySignal class (Groq/Llama).
-- One row per (market, ticker) — a single market can produce
-- multiple signals if it affects multiple stocks.
--
-- Example: "Will US impose 25% chip tariffs on TSMC?" creates:
--   row 1 → market_id=X, ticker='TSM', sentiment='Bearish'
--   row 2 → market_id=X, ticker='MU',  sentiment='Bearish'
--   row 3 → market_id=X, ticker='NVDA', sentiment='Bearish'
-- ============================================================
CREATE TABLE signals (
    id            SERIAL       PRIMARY KEY,
    market_id     TEXT         NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
    ticker        VARCHAR(10)  NOT NULL REFERENCES stocks(ticker),

    -- Matches your EquitySignal Pydantic model exactly
    is_relevant   BOOLEAN      NOT NULL DEFAULT FALSE,
    sentiment     VARCHAR(10)  CHECK (sentiment IN ('Bullish','Bearish','Neutral')),
    impact_score  SMALLINT     CHECK (impact_score BETWEEN 1 AND 10),
    reasoning     TEXT,

    model_used    TEXT         DEFAULT 'llama-3.3-70b-versatile',
    created_at    TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX idx_signals_market    ON signals(market_id);
CREATE INDEX idx_signals_ticker    ON signals(ticker);
CREATE INDEX idx_signals_relevant  ON signals(is_relevant, impact_score DESC);
CREATE INDEX idx_signals_created   ON signals(created_at DESC);


-- ============================================================
-- TABLE 6: DEEP DIVES
-- Output from your dig_deeper_analysis() function.
-- Stored so the dashboard can show "last analyzed 2h ago"
-- instead of re-running Groq every click.
-- ============================================================
CREATE TABLE deep_dives (
    id             SERIAL      PRIMARY KEY,
    signal_id      INTEGER     NOT NULL REFERENCES signals(id) ON DELETE CASCADE,

    -- Groq output
    analysis_text  TEXT        NOT NULL,     -- full markdown bullet analysis
    direction      VARCHAR(10) CHECK (direction IN ('Bullish','Bearish','Neutral')),

    -- News grounding (from DuckDuckGo)
    news_query     TEXT,                     -- search string used
    news_headlines TEXT,                     -- raw headlines fed to LLM
    source_urls    TEXT[],                   -- URLs returned by DuckDuckGo

    model_used     TEXT        DEFAULT 'llama-3.3-70b-versatile',
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_deep_dives_signal  ON deep_dives(signal_id);
CREATE INDEX idx_deep_dives_created ON deep_dives(created_at DESC);


-- ============================================================
-- TABLE 7: REPORTS
-- The daily BIT Capital Alpha Report generated by Groq.
-- ============================================================
CREATE TABLE reports (
    id            SERIAL      PRIMARY KEY,
    content       TEXT        NOT NULL,     -- full markdown report
    tickers       TEXT[],                   -- tickers mentioned in report
    signal_count  INTEGER,                  -- how many signals fed into it
    model_used    TEXT        DEFAULT 'llama-3.3-70b-versatile',
    generated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_reports_generated  ON reports(generated_at DESC);
CREATE INDEX idx_reports_tickers    ON reports USING GIN(tickers);


-- ============================================================
-- TABLE 8: REPORT_SIGNALS (join table)
-- Which signals were used to generate which report.
-- Gives you full traceability: "this report was based on
-- these 15 specific market signals"
-- ============================================================
CREATE TABLE report_signals (
    report_id  INTEGER NOT NULL REFERENCES reports(id)  ON DELETE CASCADE,
    signal_id  INTEGER NOT NULL REFERENCES signals(id)  ON DELETE CASCADE,
    PRIMARY KEY (report_id, signal_id)
);


-- ============================================================
-- VIEW: SIGNAL_FEED
-- What your dashboard's signal list actually queries.
-- Everything joined in one place.
-- ============================================================
CREATE OR REPLACE VIEW signal_feed AS
SELECT
    s.id             AS signal_id,
    e.title          AS event_title,
    e.category,
    m.question,
    m.yes_price,
    m.volume,
    m.end_date,
    s.ticker,
    st.company_name,
    s.sentiment,
    s.impact_score,
    s.reasoning,
    s.created_at
FROM signals       s
JOIN markets       m  ON s.market_id = m.id
JOIN events        e  ON m.event_id  = e.id
JOIN stocks        st ON s.ticker    = st.ticker
WHERE s.is_relevant = TRUE
  AND m.active      = TRUE
  AND m.closed      = FALSE
ORDER BY s.impact_score DESC, m.volume DESC;