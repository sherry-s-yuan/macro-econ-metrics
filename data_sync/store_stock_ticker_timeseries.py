# Stores historical stock/etc price timeseries in postgres DB.
import yfinance as yf
from datetime import datetime
import pandas as pd
from db_util import run_sql_command, connect_to_postgres

TICKER_TO_TRACK = ["QQQ", "^GSPC"]
COLUMNS_TO_KEEP = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits']
DEBUG=0
# Store the newest stock data for `ticker`
def store_ticker_historical_data(ticker: str):
    connection = connect_to_postgres()
    if connection is None:
        return
    run_sql_command(connection, "data_sync/sql/create_table.sql")
    latest_data_date = get_latest_date(connection, ticker)
    if DEBUG:
        print("latest_data_date")
        print(latest_data_date)

    info = yf.Ticker(ticker)
    # One of: ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']
    period = "1d"
    if latest_data_date is None:
        period = "max"
    else:
        today = datetime.now().date()
        delta_days = (today - latest_data_date).days
        period = round_delta_days(delta_days)
    if DEBUG:
        print("period")
        print(period)
    # Query the historical data
    historical_data = info.history(period=period)
    if historical_data is None or historical_data.empty: 
        print("No new historical data retrieved")
        return
    historical_data = historical_data.reset_index()
    historical_data = validate_and_sanitize_historical_data(historical_data)

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
        "Dividends": "dividends",
        "Stock Splits": "stock_splits",
    })
    historical_data["ticker"] = ticker
    if DEBUG:
        print("latest_new_data_date")
        print(historical_data["date"].max())
    # Remove dates that have overlap with what's already been stored
    historical_data = historical_data[~(historical_data['date'] <= latest_data_date)]
    if historical_data.empty:
        print("Data is already up to date")
        return
    if DEBUG:
        historical_data.to_csv("sample_data.csv")
    new_db_records = historical_data.to_dict(orient = "records")
    if new_db_records:
        insert_data_into_table(connection, new_db_records)

def validate_and_sanitize_historical_data(historical_data):
    for col in COLUMNS_TO_KEEP:
        if col not in historical_data:
            if col == 'Date':
                raise Exception("Invalid ticker historical data without dates")
            if col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                raise Exception("Invalid ticker historical data without basic price data")
            if col in ['Stock Splits']:
                historical_data['Stock Splits'] = 0.0
            if col in ['Dividends']:
                historical_data['Dividends'] = 0.0
    return historical_data[COLUMNS_TO_KEEP]
                
    
def round_delta_days(days: int) -> str:
    # Round days to one of ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']
    if days <= 1:
        return '1d'
    elif days <= 5:
        return '5d'
    elif days <= 28:
        return '1mo'
    elif days <= 28 + 2 * 30:
        return '3mo'
    elif days <= 28 + 5 * 30:
        return '6mo'
    elif days <= 365:
        return '1y'
    elif days <= 365 * 2:
        return '2y'
    elif days <= 365 * 5:
        return '5y'
    elif days <= 365 * 10:
        return '10y'
    return 'max'

def get_latest_date(connection, ticker):
    """Get the latest date for a given ticker, None if no date exist."""
    query = """
    SELECT MAX(date) AS latest_date
    FROM stock_ticker_timeseries
    WHERE ticker = %s;
    """
    cursor = connection.cursor()
    cursor.execute(query, (ticker,))
    result = cursor.fetchone()
    cursor.close()
    if result is None: return None
    return result[0]  # Return the latest date

def insert_data_into_table(connection, data_list):
    """Insert multiple rows of data into the table."""
    insert_query = """
    INSERT INTO stock_ticker_timeseries (date, ticker, open, high, low, close, dividends, stock_splits)
    VALUES (%(date)s, %(ticker)s, %(open)s, %(high)s, %(low)s, %(close)s, %(dividends)s, %(stock_splits)s)
    ON CONFLICT (date, ticker) DO NOTHING;
    """
    try:
        cursor = connection.cursor()
        cursor.executemany(insert_query, data_list)
        cursor.close()
        connection.commit()
    except Exception as e:
        print(f"Error inserting data: {e}")


if __name__ == '__main__':
    for ticker in TICKER_TO_TRACK:
        store_ticker_historical_data(ticker)
