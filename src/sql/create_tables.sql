-- ETF price data
CREATE TABLE IF NOT EXISTS etf_prices (
    date DATE, 
    ticker TEXT, 
    open_price REAL, 
    high_price REAL, 
    low_price REAL, 
    close_price REAL, 
    volume INTEGER, 
    adj_close REAL,  -- adjusted close. accounts for splits/dividends
    PRIMARY KEY (date, ticker)
); 

-- Economic indicators. keep track of macroeconomic time series
CREATE TABLE IF NOT EXISTS economic_indicators (
    date DATE, 
    indicator_name TEXT, 
    value REAL, 
    PRIMARY KEY (date, indicator_name)
); 

-- Factor calculations to populate later 
CREATE TABLE IF NOT EXISTS etf_factors (
    date DATE, 
    ticker TEXT, 
    momentum_3m REAL, 
    momentum_6m REAL, 
    momentum_12m REAL, 
    value_score REAL, 
    PRIMARY KEY (date, ticker)
); 

-- Cross asset correlations to populate later
-- rolling correlations between pairs of ETFs over different look-back windows.
CREATE TABLE IF NOT EXISTS cross_asset_correlations (
    date DATE,
    ticker_1 TEXT,
    ticker_2 TEXT,
    correlation_21d REAL,
    correlation_63d REAL,
    correlation_252d REAL,
    PRIMARY KEY (date, ticker_1, ticker_2)
);

-- Create indexes for fast queries 
CREATE INDEX IF NOT EXISTS idx_etf_prices_date ON etf_prices(date);
CREATE INDEX IF NOT EXISTS idx_etf_prices_ticker ON etf_prices(ticker);
CREATE INDEX IF NOT EXISTS idx_economic_date ON economic_indicators(date);
CREATE INDEX IF NOT EXISTS idx_factors_date ON etf_factors(date);