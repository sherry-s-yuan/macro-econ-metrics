from structs.data_source_metadata import SeriesDataSourceMetadata

# DB structs
DB_NAME = "macro_econ_metrics"
TICKER_TIMESERIES_TABLE_NAME = "stock_ticker_timeseries"
FRED_TIMESERIES_TABLE_NAME = "fred_series_timeseries"

# Data source metadata structs
STOCK_DATA_SOURCE_METADATA = SeriesDataSourceMetadata(
            table='stock_ticker_timeseries',
            series_col='ticker',
            date_col='date',
            value_col='close',
        )
MACRO_DATA_SOURCE_METADATA = SeriesDataSourceMetadata(
            table='fred_series_timeseries',
            series_col='series',
            date_col='date',
            value_col='value',
        )

TICKER_TO_TRACK = [
    "QQQ", # Technology
    "SPY", # Technology + others
    "TLT", # Bonds
    "GLD", # Gold
    "XLE", # Oil & Energy
]

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

def list_all_series():
    return TICKER_TO_TRACK + SERIES_TO_TRACK

def get_data_source_metadata(series: str) -> SeriesDataSourceMetadata:
    if series in TICKER_TO_TRACK:
        return STOCK_DATA_SOURCE_METADATA
    if series in SERIES_TO_TRACK:
        return MACRO_DATA_SOURCE_METADATA
    return None
