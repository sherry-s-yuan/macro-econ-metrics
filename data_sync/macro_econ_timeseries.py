# Stores historical macro economics metric timeseries in postgres DB.
from fredapi import Fred
import pandas as pd
from db_util import run_sql_command, connect_to_postgres

API_KEY="8805f58d4d02b992a731e925ef01cd8e"
SERIES_TO_TRACK = [
    "UMCSENT", # consumer confidence index
    "MSPUS", # home sales
    "M2REAL", # money supply
    "IRLTLT01USM156N", # bond interest rate
    "MORTGAGE30US", # mortgage interest rate
    "DEXUSEU", # US->Euro exchange rates
    "DEXJPUS", # Us->Japan exchange rates
    "DEXCHUS", # Us->China exchange rates
    "DEXSIUS", # Us->Singapore exchange rates
    "PPIACO", # All commodities
    "WPU10", # metal commodities
    "WPS012", # grains commodities
    "DCOILWTICO", # oil commodities
]
COLUMNS_TO_KEEP = ['date', 'value']
DEBUG=1


# Store the newest stock data for `ticker`
def store_macro_econ_historical_data(series: str):
    print("getting macro econ metrics for", series)
    connection = connect_to_postgres()
    if connection is None:
        return
    run_sql_command(connection, "data_sync/sql/create_macro_econ_table.sql")
    latest_data_date = get_latest_date(connection, series)
    if DEBUG:
        print("latest_data_date")
        print(latest_data_date)

    fred = Fred(api_key=API_KEY)
    historical_data = fred.get_series_all_releases(series)
    if historical_data is None or historical_data.empty: 
        print("No new historical data retrieved")
        return
    historical_data = historical_data.dropna()
    historical_data = validate_and_sanitize_historical_data(historical_data)
    historical_data["date"] = pd.to_datetime(historical_data["date"])
    historical_data["date"] = historical_data["date"].dt.floor("d")
    historical_data["date"] = historical_data["date"].dt.date
    historical_data = historical_data.drop_duplicates(subset = 'date', keep='last')
    historical_data["series"] = series
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
            if col == 'date':
                raise Exception("Invalid historical data without dates")
            if col in ['value']:
                raise Exception("Invalid historical data without basic values")
    return historical_data[COLUMNS_TO_KEEP]
                

def get_latest_date(connection, series):
    """Get the latest date for a given ticker, None if no date exist."""
    query = """
    SELECT MAX(date) AS latest_date
    FROM fred_series_timeseries
    WHERE series = %s;
    """
    cursor = connection.cursor()
    cursor.execute(query, (series,))
    result = cursor.fetchone()
    cursor.close()
    if result is None: return None
    return result[0]  # Return the latest date

def insert_data_into_table(connection, data_list):
    """Insert multiple rows of data into the table."""
    insert_query = """
    INSERT INTO fred_series_timeseries (date, series, value)
    VALUES (%(date)s, %(series)s, %(value)s)
    ON CONFLICT (date, series) DO NOTHING;
    """
    try:
        cursor = connection.cursor()
        cursor.executemany(insert_query, data_list)
        cursor.close()
        connection.commit()
    except Exception as e:
        print(f"Error inserting data: {e}")


if __name__ == '__main__':
    for series in SERIES_TO_TRACK:
        store_macro_econ_historical_data(series)
