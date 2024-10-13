from util.db_util import connect_to_postgres
import pandas as pd


target_series = 'QQQ'
target_table = 'stock_ticker_timeseries'

def extract_windowed_metrics(target_series: str, table: str, series_col: str, date_col: str, value_col: str):
    connection = connect_to_postgres()
    if connection is None:
        return
    query = f"""
    WITH moving_averages AS (
        SELECT
            {series_col},
            {date_col},
            AVG({value_col}) OVER (PARTITION BY {series_col} ORDER BY {date_col} ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS "1_week_MA",
            AVG({value_col}) OVER (PARTITION BY {series_col} ORDER BY {date_col} ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS "1_month_MA",
            AVG({value_col}) OVER (PARTITION BY {series_col} ORDER BY {date_col} ROWS BETWEEN 90 PRECEDING AND CURRENT ROW) AS "3_month_MA",
            AVG({value_col}) OVER (PARTITION BY {series_col} ORDER BY {date_col} ROWS BETWEEN 180 PRECEDING AND CURRENT ROW) AS "6_month_MA",
            AVG({value_col}) OVER (PARTITION BY {series_col} ORDER BY {date_col} ROWS BETWEEN 365 PRECEDING AND CURRENT ROW) AS "1_year_MA",
            AVG({value_col}) OVER (PARTITION BY {series_col} ORDER BY {date_col} ROWS BETWEEN 1825 PRECEDING AND CURRENT ROW) AS "5_year_MA"
        FROM {table}
        WHERE {series_col} = '{target_series}'
        ORDER BY {series_col}, {date_col}
    )
    SELECT 
        {table}.{series_col},
        {table}.{date_col},
        {value_col},
        ({value_col} / LAG({value_col}, 7) OVER (PARTITION BY {table}.{series_col} ORDER BY {table}.{date_col}) - 1) AS "1_week_return",   -- 1 week return
        ({value_col} / LAG({value_col}, 30) OVER (PARTITION BY {table}.{series_col} ORDER BY {table}.{date_col}) - 1) AS "1_month_return",  -- 1 month return
        ({value_col} / LAG({value_col}, 90) OVER (PARTITION BY {table}.{series_col} ORDER BY {table}.{date_col}) - 1) AS "3_month_return",  -- 3 month return
        ({value_col} / LAG({value_col}, 180) OVER (PARTITION BY {table}.{series_col} ORDER BY {table}.{date_col}) - 1) AS "6_month_return", -- 6 month return
        ({value_col} / LAG({value_col}, 365) OVER (PARTITION BY {table}.{series_col} ORDER BY {table}.{date_col}) - 1)  AS "1_year_return",  -- 1 year return
        ({value_col} / LAG({value_col}, 1825) OVER (PARTITION BY {table}.{series_col} ORDER BY {table}.{date_col}) - 1)  AS "5_year_return",  -- 5 year return
        (({value_col} - "1_week_MA") / "1_week_MA") AS "percent_diff_1_week_moving_avg",
        (({value_col} - "1_month_MA") / "1_month_MA") AS "percent_diff_1_month_moving_avg",
        (({value_col} - "3_month_MA") / "3_month_MA") AS "percent_diff_3_month_moving_avg",
        (({value_col} - "6_month_MA") / "6_month_MA") AS "percent_diff_6_month_moving_avg",
        (({value_col} - "1_year_MA") / "1_year_MA") AS "percent_diff_1_year_moving_avg",
        (({value_col} - "5_year_MA") / "5_year_MA") AS "percent_diff_5_year_moving_avg"
    FROM {table} JOIN moving_averages ON {table}.{date_col} = moving_averages.{date_col} AND {table}.{series_col} = moving_averages.{series_col}
    WHERE {table}.{series_col} = '{target_series}'
    ORDER BY {series_col}, {date_col}
    """
    
    # Execute the query and return the data as a pandas DataFrame
    df = pd.read_sql(query, connection)
    return df

data = extract_windowed_metrics(target_series, target_table, "ticker", "date", "close")
print(data)

data.to_csv("sample_data.csv")