import yfinance as yf
from pymongo import MongoClient, ASCENDING
from datetime import datetime

client = MongoClient('mongodb://localhost:27017/')
db = client['macro_econ_metrics']
collection = db['etf']
collection.create_index([('date', ASCENDING), ('ticker', ASCENDING)],)

symbols_of_interest = ["QQQ", "^GSPC"]

def get_ticker_data(tickerName: str):
    ticker = yf.Ticker(tickerName)
    ticker.history(period="max").to_csv(tickerName+"_history.csv")
    print(ticker.info)



# get_ticker_data(tickerName="^GSPC")
