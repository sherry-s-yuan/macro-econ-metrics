# Database Design

## ETF information

Top level information
longBusinessSummary: string (summary string)
maxAge:int


Real time info
date: time
open: float
high: float
low: float
close: float
volume: int
dividends: float
stock_splits: float
capital_gains: float

bid: float
ask: float
bidsize: int
askSize: int

Aggregates:
1. 1/3/5 year return: float
2. P/E: float
3. average volume

## API alternatives
Alpha Vantage: https://www.alphavantage.co/
Fintage: https://finage.co.uk/pricing
EODHD (past year): https://eodhd.com/pricing
Finhub: https://finnhub.io/pricing
Twelve Data: https://twelvedata.com/pricing
Macro econ data: https://fred.stlouisfed.org/
