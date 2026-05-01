-- ── 1. STOCKS ─────────────────────────────────────────────────
CREATE TABLE stocks (
    ticker       VARCHAR(10)  PRIMARY KEY,
    company_name TEXT         NOT NULL,
    sector       TEXT,
    thesis       TEXT,
    active       BOOLEAN      DEFAULT TRUE
);

INSERT INTO stocks VALUES
  ('IREN',  'IREN Limited',    'Crypto Mining',   'Bitcoin mining and AI data centers.'),
  ('MSFT',  'Microsoft',       'Cloud/AI',        'Azure + OpenAI. Enterprise AI play.'),
  ('GOOGL', 'Alphabet',        'Cloud/AI',        'Search + Gemini AI. Antitrust risk.'),
  ('LMND',  'Lemonade',        'Insurtech',       'AI-driven insurance. Rate-sensitive.'),
  ('RDDT',  'Reddit',          'Social Media',    'AI data licensing + niche ads.'),
  ('MU',    'Micron',          'Semiconductors',  'HBM memory for AI data centers.'),
  ('TSM',   'TSMC',            'Semiconductors',  'AI chip foundry. Taiwan risk.'),
  ('HUT',   'Hut 8',           'Crypto Mining',   'Crypto + AI compute infrastructure.'),
  ('HOOD',  'Robinhood',       'Fintech',         'Retail trading + crypto. Rate-sensitive.'),
  ('DDOG',  'Datadog',         'Cloud Software',  'Cloud monitoring. AI spend indicator.'),
  ('AMZN',  'Amazon',          'Cloud/AI',        'AWS + Bedrock. Import tariff exposure.'),
  ('COIN',  'Coinbase',        'Crypto Exchange', 'Crypto exchange. Regulatory risk.'),
  ('META',  'Meta Platforms',  'Social Media/AI', 'Llama AI + digital ads.'),
  ('NVDA',  'NVIDIA',          'Semiconductors',  'AI accelerators. Chip export risk.');


-- ── 2. SIGNALS ────────────────────────────────────────────────
-- Core ETL output. One row per (market × ticker).
-- Market data + LLM enrichment in one table.
-- stock_prices removed — prices come from Yahoo Finance live.
CREATE TABLE signals (
    id            SERIAL       PRIMARY KEY,

    -- Raw market data (from ingest + stage1)
    market_id     TEXT         NOT NULL,
    event_id      TEXT,
    event_title   TEXT,
    question      TEXT         NOT NULL,
    tags          TEXT,
    yes_price     NUMERIC(6,4),
    volume        NUMERIC(20,2),
    end_date      TIMESTAMPTZ,

    -- LLM enrichment (from stage2 Gemini)
    ticker        VARCHAR(10)  NOT NULL REFERENCES stocks(ticker),
    sentiment     VARCHAR(10)  CHECK (sentiment IN ('Bullish','Bearish','Neutral')),
    impact_score  SMALLINT     CHECK (impact_score BETWEEN 1 AND 10),
    impact_type   TEXT         CHECK (impact_type IN ('margin','revenue','sentiment','regulatory','operational')),
    time_horizon  TEXT         CHECK (time_horizon IN ('short-term','medium-term','long-term')),
    reasoning     TEXT,
    model_used    TEXT         DEFAULT 'gemini-flash-lite-latest',
    created_at    TIMESTAMPTZ  DEFAULT NOW(),

    UNIQUE (market_id, ticker)
);

CREATE INDEX idx_signals_ticker   ON signals(ticker);
CREATE INDEX idx_signals_score    ON signals(impact_score DESC);
CREATE INDEX idx_signals_created  ON signals(created_at DESC);
CREATE INDEX idx_signals_end_date ON signals(end_date);
CREATE INDEX idx_signals_sentiment ON signals(sentiment);


-- ── 3. REPORTS ────────────────────────────────────────────────
CREATE TABLE reports (
    id            SERIAL      PRIMARY KEY,
    content       TEXT        NOT NULL,
    tickers       TEXT[],
    signal_count  INTEGER,
    model_used    TEXT        DEFAULT 'gemini-flash-lite-latest',
    generated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_reports_generated ON reports(generated_at DESC);
CREATE INDEX idx_reports_tickers   ON reports USING GIN(tickers);


-- ── 4. REPORT_SIGNALS ─────────────────────────────────────────
CREATE TABLE report_signals (
    report_id  INTEGER NOT NULL REFERENCES reports(id)  ON DELETE CASCADE,
    signal_id  INTEGER NOT NULL REFERENCES signals(id)  ON DELETE CASCADE,
    PRIMARY KEY (report_id, signal_id)
);


-- ── 5. DEEP_DIVES ─────────────────────────────────────────────
CREATE TABLE deep_dives (
    id             SERIAL       PRIMARY KEY,
    signal_id      INTEGER      NOT NULL REFERENCES signals(id) ON DELETE CASCADE,
    analysis_text  TEXT         NOT NULL,
    direction      VARCHAR(10)  CHECK (direction IN ('Bullish','Bearish','Neutral')),
    news_query     TEXT,
    news_headlines TEXT,
    source_urls    TEXT[],
    model_used     TEXT         DEFAULT 'llama-3.3-70b-versatile',
    created_at     TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX idx_dd_signal  ON deep_dives(signal_id);
CREATE INDEX idx_dd_created ON deep_dives(created_at DESC);


-- ── 6. SIGNAL_FEED VIEW ───────────────────────────────────────
-- Joins signals with stocks for the dashboard.
-- No stock price subquery — prices come from Yahoo Finance live.
CREATE OR REPLACE VIEW signal_feed AS
SELECT
    s.id            AS signal_id,
    s.market_id,
    s.event_id,
    s.event_title,
    s.question,
    s.tags,
    s.yes_price,
    s.volume,
    s.end_date,
    s.ticker,
    st.company_name,
    st.sector,
    st.thesis,
    s.sentiment,
    s.impact_score,
    s.impact_type,
    s.time_horizon,
    s.reasoning,
    s.model_used,
    s.created_at
FROM signals s
JOIN stocks st ON s.ticker = st.ticker
ORDER BY s.impact_score DESC, s.volume DESC;


-- ── 7. RLS ────────────────────────────────────────────────────
ALTER TABLE stocks         ENABLE ROW LEVEL SECURITY;
ALTER TABLE signals        ENABLE ROW LEVEL SECURITY;
ALTER TABLE reports        ENABLE ROW LEVEL SECURITY;
ALTER TABLE report_signals ENABLE ROW LEVEL SECURITY;
ALTER TABLE deep_dives     ENABLE ROW LEVEL SECURITY;

CREATE POLICY "public read" ON stocks         FOR SELECT USING (true);
CREATE POLICY "public read" ON signals        FOR SELECT USING (true);
CREATE POLICY "public read" ON reports        FOR SELECT USING (true);
CREATE POLICY "public read" ON report_signals FOR SELECT USING (true);
CREATE POLICY "public read" ON deep_dives     FOR SELECT USING (true);