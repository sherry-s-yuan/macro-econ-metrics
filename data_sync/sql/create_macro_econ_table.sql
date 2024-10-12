CREATE TABLE IF NOT EXISTS fred_series_timeseries (
    date DATE NOT NULL,
    series VARCHAR(30) NOT NULL,
    value NUMERIC(10, 2),
    PRIMARY KEY (date, series)
);