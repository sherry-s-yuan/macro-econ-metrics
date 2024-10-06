# Store ticker basic information into postres DB
import yfinance as yf

def get_ticker_data(tickerName: str):
    ticker = yf.Ticker(tickerName)
    ticker.history(period="max").to_csv(tickerName+"_history.csv")
    print(ticker.info)
