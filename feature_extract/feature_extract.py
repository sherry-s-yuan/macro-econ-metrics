from util.db_util import connect_to_postgres
import pandas as pd
from consts import get_data_source_metadata, list_all_series
from structs.data_source_metadata import SeriesDataSourceMetadata

class SeriesFeature:
    def __init__(self, series: str, df) -> None:
        self.series = series
        self.df = df

def prepare_features(target_series):
    features = None
    for series in list_all_series():
        df = extract_windowed_metrics(series, get_data_source_metadata(series)).dropna()
        print(series)
        print(df['date'].min())
        if features is None:
            features = SeriesFeature(series, df)
            continue
        merged_df = pd.merge(features.df, df, on='date', how='inner', suffixes=(f"_{features.series}" if len(features.series) > 0 else "", f"_{series}"))
        features = SeriesFeature("", merged_df)
        # print(merged_df)
    if features is None:
        return None, None
    labels = extract_labels(target_series, get_data_source_metadata(target_series))
    features_columns = features.df.columns
    label_columns = labels.columns
    result = pd.merge(features.df, labels, on='date', how='inner')
    return result[features_columns], result[label_columns]

def extract_windowed_metrics(target_series: str, source_metadata: SeriesDataSourceMetadata):
    table = source_metadata.table
    series_col = source_metadata.series_col
    date_col = source_metadata.date_col
    value_col = source_metadata.value_col
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
        {table}.{date_col},
        {value_col} as value,
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
    ORDER BY {table}.{series_col}, {date_col}
    """
    
    # Execute the query and return the data as a pandas DataFrame
    df = pd.read_sql(query, connection)
    return df

def extract_labels(target_series: str, source_metadata: SeriesDataSourceMetadata):
    table = source_metadata.table
    series_col = source_metadata.series_col
    date_col = source_metadata.date_col
    value_col = source_metadata.value_col
    connection = connect_to_postgres()
    if connection is None:
        return
    query = f"""
    WITH price_diff AS (
        SELECT
            {date_col},
            {value_col},
            LEAD({value_col}, 7) OVER (PARTITION BY {series_col} ORDER BY {date_col}) AS close_1w,
            LEAD({value_col}, 30) OVER (PARTITION BY {series_col} ORDER BY {date_col}) AS close_1m,
            LEAD({value_col}, 90) OVER (PARTITION BY {series_col} ORDER BY {date_col}) AS close_3m,
            LEAD({value_col}, 180) OVER (PARTITION BY {series_col} ORDER BY {date_col}) AS close_6m
        FROM {table}
        WHERE {series_col} = '{target_series}'
    ),
    std_values AS (
        SELECT
            {date_col},
            stddev({value_col}) OVER (PARTITION BY {series_col} ORDER BY {date_col} ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS rolling_std_1w,
            stddev({value_col}) OVER (PARTITION BY {series_col} ORDER BY {date_col} ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS rolling_std_1m,
            stddev({value_col}) OVER (PARTITION BY {series_col} ORDER BY {date_col} ROWS BETWEEN 180 PRECEDING AND CURRENT ROW) AS rolling_std_3m,
            stddev({value_col}) OVER (PARTITION BY {series_col} ORDER BY {date_col} ROWS BETWEEN 365 PRECEDING AND CURRENT ROW) AS rolling_std_6m
        FROM {table}
        WHERE {series_col} = '{target_series}'
    )
    SELECT
        p.{date_col},
        CASE
            WHEN p.close_1w - p.{value_col} > s.rolling_std_1w THEN 1
            ELSE 0
        END AS price_increase_1w,
        CASE
            WHEN ABS(p.close_1w - p.{value_col}) < s.rolling_std_1w THEN 1
            ELSE 0
        END AS price_no_change_1w,
        CASE
            WHEN p.close_1w - p.{value_col} < -s.rolling_std_1w THEN 1
            ELSE 0
        END AS price_decrease_1w,
        CASE
            WHEN p.close_1m - p.{value_col} > s.rolling_std_1m THEN 1
            ELSE 0
        END AS price_increase_1m,
        CASE
            WHEN ABS(p.close_1m - p.{value_col}) < s.rolling_std_1m THEN 1
            ELSE 0
        END AS price_no_change_1m,
        CASE
            WHEN p.close_1m - p.{value_col} < -s.rolling_std_1m THEN 1
            ELSE 0
        END AS price_decrease_1m,
        CASE
            WHEN p.close_3m - p.{value_col} > s.rolling_std_3m THEN 1
            ELSE 0
        END AS price_increase_3m,
        CASE
            WHEN ABS(p.close_3m - p.{value_col}) < s.rolling_std_3m THEN 1
            ELSE 0
        END AS price_no_change_3m,
        CASE
            WHEN p.close_3m - p.{value_col} < -s.rolling_std_3m THEN 1
            ELSE 0
        END AS price_decrease_3m,
        CASE
            WHEN p.close_6m - p.{value_col} > s.rolling_std_6m THEN 1
            ELSE 0
        END AS price_increase_6m,
        CASE
            WHEN ABS(p.close_6m - p.{value_col}) < s.rolling_std_6m THEN 1
            ELSE 0
        END AS price_no_change_6m,
        CASE
            WHEN p.close_6m - p.{value_col} < -s.rolling_std_6m THEN 1
            ELSE 0
        END AS price_decrease_6m
    FROM price_diff p
    JOIN std_values s ON p.{date_col} = s.{date_col}
    ORDER BY p.{date_col};
    """
    df = pd.read_sql(query, connection)
    return df

# data = extract_windowed_metrics(target_series, target_table, "ticker", "date", "close")
X, y = prepare_features("QQQ")
X = X.drop(columns=['date'])
y = y.drop(columns=['date'])
print(X)

X.to_csv("sample_data_features.csv", index = False)
y.to_csv("sample_data_labels.csv", index = False)


"""
Alternate query for -1, 0, 1

SELECT
    p.date,
    p.ticker,
    CASE
        WHEN ABS(p.close_1w - p.close) > s.rolling_std_1w THEN (p.close_1w - p.close)/ABS(p.close_1w - p.close)
        ELSE 0
    END AS price_1w_change,
    CASE
        WHEN ABS(p.close_1m - p.close) > s.rolling_std_1m THEN (p.close_1m - p.close)/ABS(p.close_1m - p.close)
        ELSE 0
    END AS price_1m_change,
    CASE
        WHEN ABS(p.close_3m - p.close) > s.rolling_std_3m THEN (p.close_3m - p.close)/ABS(p.close_3m - p.close)
        ELSE 0
    END AS price_3m_change,
    CASE
        WHEN ABS(p.close_6m - p.close) > s.rolling_std_6m THEN (p.close_6m - p.close)/ABS(p.close_6m - p.close)
        ELSE 0
    END AS price_6m_change
FROM price_diff p
JOIN std_values s ON p.ticker = s.ticker AND p.date = s.date
WHERE p.ticker = 'QQQ'
ORDER BY p.ticker, p.date;
"""