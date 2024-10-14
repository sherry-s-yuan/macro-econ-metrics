class SeriesDataSourceMetadata:
    def __init__(self, table: str, series_col: str, date_col: str, value_col: str) -> None:
        self.table = table
        self.series_col = series_col
        self.date_col = date_col
        self.value_col = value_col