-- ============================================================
-- Polymarket Signal Scanner Schema v1
-- ============================================================

-- Utility: auto-update timestamp trigger
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- Stocks Watchlist — BIT Capital Holdings (verified)
-- ============================================================
CREATE TABLE IF NOT EXISTS stocks (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL,
    company_name TEXT,
    sector TEXT,
    active BOOLEAN DEFAULT TRUE
);

INSERT INTO stocks (ticker, company_name, sector) VALUES
  ('IREN',  'Iris Energy Ltd.',              'Bitcoin Mining & AI Compute'),
  ('GOOGL', 'Alphabet Inc.',                 'AI, Search & Cloud'),
  ('NVDA',  'NVIDIA Corporation',            'Semiconductors & AI Infrastructure'),
  ('META',  'Meta Platforms Inc.',           'Social Media & AI'),
  ('MSFT',  'Microsoft Corporation',         'Cloud & Enterprise AI'),
  ('AMZN',  'Amazon.com Inc.',               'Cloud & E-commerce'),
  ('COHR',  'Coherent Corp.',                'Photonics & Semiconductor Lasers'),
  ('LMND',  'Lemonade Inc.',                 'Insurtech & AI-Driven Insurance'),
  ('HUT',   'Hut 8 Corp.',                   'Bitcoin Mining & AI Data Centers'),
  ('HOOD',  'Robinhood Markets Inc.',        'Fintech & Crypto Trading')
ON CONFLICT (ticker) DO NOTHING;


-- Polymarket Markets
-- ============================================================
CREATE TABLE IF NOT EXISTS markets (
    id TEXT PRIMARY KEY,
    slug TEXT,
    question TEXT NOT NULL,
    category TEXT,
    tags JSONB,
    outcomes JSONB,
    volume_24hr DOUBLE PRECISION DEFAULT 0,
    volume_total DOUBLE PRECISION DEFAULT 0,
    liquidity DOUBLE PRECISION DEFAULT 0,
    end_date TIMESTAMPTZ,
    start_date TIMESTAMPTZ,
    active BOOLEAN DEFAULT TRUE,
    closed BOOLEAN DEFAULT FALSE,
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_markets_updated_at ON markets;
CREATE TRIGGER trg_markets_updated_at
  BEFORE UPDATE ON markets
  FOR EACH ROW
  EXECUTE FUNCTION update_timestamp();


-- LLM-Filtered Signals
-- ============================================================
CREATE TABLE IF NOT EXISTS signals (
    id SERIAL PRIMARY KEY,
    market_id TEXT REFERENCES markets(id) ON DELETE CASCADE,
    stock_ticker VARCHAR(10) REFERENCES stocks(ticker) ON DELETE CASCADE,
    relevance_score FLOAT CHECK (relevance_score >= 0 AND relevance_score <= 1),
    signal_direction VARCHAR(10) CHECK (signal_direction IN ('bullish','bearish','neutral','mixed')),
    reasoning TEXT,
    model_used TEXT DEFAULT 'gemini-1.5-flash',
    created_at TIMESTAMPTZ DEFAULT NOW()
);


-- Generated Analyst Reports
-- ============================================================
CREATE TABLE IF NOT EXISTS reports (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT,
    tickers TEXT[],
    generated_at TIMESTAMPTZ DEFAULT NOW()
);


-- Report ↔ Signal Join Table
-- ============================================================
CREATE TABLE IF NOT EXISTS report_signals (
    report_id INTEGER REFERENCES reports(id) ON DELETE CASCADE,
    signal_id INTEGER REFERENCES signals(id) ON DELETE CASCADE,
    PRIMARY KEY (report_id, signal_id)
);


-- Indexes
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_markets_active      ON markets(active, closed);
CREATE INDEX IF NOT EXISTS idx_markets_volume      ON markets(volume_total DESC);
CREATE INDEX IF NOT EXISTS idx_markets_end_date    ON markets(end_date);
CREATE INDEX IF NOT EXISTS idx_markets_updated     ON markets(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_signals_stock       ON signals(stock_ticker);
CREATE INDEX IF NOT EXISTS idx_signals_market      ON signals(market_id);
CREATE INDEX IF NOT EXISTS idx_signals_recent      ON signals(created_at DESC, stock_ticker);
CREATE INDEX IF NOT EXISTS idx_signals_direction   ON signals(signal_direction);
CREATE INDEX IF NOT EXISTS idx_signals_model       ON signals(model_used);
CREATE INDEX IF NOT EXISTS idx_reports_tickers     ON reports USING GIN (tickers);
CREATE INDEX IF NOT EXISTS idx_reports_date        ON reports(generated_at DESC);