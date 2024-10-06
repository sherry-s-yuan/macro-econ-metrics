CREATE TABLE IF NOT EXISTS stock_ticker_timeseries (
    date DATE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    open NUMERIC(10, 2),
    high NUMERIC(10, 2),
    low NUMERIC(10, 2),
    close NUMERIC(10, 2),
    dividends NUMERIC(10, 2),
    stock_splits NUMERIC(10, 2),
    PRIMARY KEY (date, ticker)
);