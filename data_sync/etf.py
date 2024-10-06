import yfinance as yf
from pymongo import MongoClient, ASCENDING
from datetime import datetime
import pandas as pd

client = MongoClient('mongodb://localhost:27017/')
DB_NAME = "macro_econ_metrics"
COLLECTION_NAME = "stock_ticker_timeseries"
INDICIES = [('date', ASCENDING)]
# INDICIES = [('date', ASCENDING), ('ticker', ASCENDING)]
symbols_of_interest = ["QQQ", "^GSPC"]

# Store the newest stock data for `ticker`
def store_ticker_historical_data(ticker: str):
    info = yf.Ticker(ticker)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    collection.create_index(INDICIES)
    latest_record = collection.find_one(
        {"ticker": ticker},           
        sort=[("date", -1)]           
    )
    # One of: ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']
    period = "1d"
    latest_data_date = None
    if latest_record is None:
        period = "max"
    else:
        today = datetime.now()
        latest_data_date = latest_record["date"]
        print("latest_data_date", latest_data_date)
        delta_days = (today - latest_data_date).days
        period = f"{delta_days}d"
    
    # Query the historical data
    historical_data = info.history(period=period)
    historical_data = historical_data.reset_index()
    historical_data["Date"] = pd.to_datetime(historical_data["Date"])
    historical_data["Date"] = historical_data["Date"].dt.floor("d")
    historical_data["Date"] = historical_data["Date"].dt.date
    historical_data = historical_data.rename(columns={
        "Date": "date",
        "Open":"open",
        "High": "high",
        "Low":"low",
        "Close":"close",
        "Volume": "volume",
        "Dividends": "dividents",
        "Stock Splits": "stock_splits",
        "Capital Gains": "capital_gains",
    })
    historical_data["ticker"] = ticker
    historical_data.to_csv("sample.csv")
    latest_new_data_date = historical_data["date"].max()
    # TODO: remove rows instead
    if historical_data.empty or (latest_data_date is not None and latest_new_data_date.date() == latest_data_date.date()):
        print("Data is already up to date")
        return
    new_db_records = historical_data.to_dict(orient = "records")
    # To prevent dates being converted to long
    for doc in new_db_records:
        doc['date'] = datetime.combine(doc['date'], datetime.min.time()).replace(microsecond=0)
    # print(new_db_records)
    with open("sample.json", "w") as f: 
        f.write(str(new_db_records))
    if new_db_records:
        collection.insert_many(new_db_records)
    
    
    

def get_ticker_data(tickerName: str):
    ticker = yf.Ticker(tickerName)
    ticker.history(period="max").to_csv(tickerName+"_history.csv")
    print(ticker.info)


# store_ticker_historical_data("QQQ")
store_ticker_historical_data("^GSPC")
# get_ticker_data(tickerName="^GSPC")
