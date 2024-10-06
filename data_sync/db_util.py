import psycopg2

DB_NAME = "macro_econ_metrics"
TICKER_TIMESERIES_TABLE_NAME = "stock_ticker_timeseries"

def run_sql_command(connection, fn: str):
    try:
        with open(fn, 'r') as file:
            sql_query = file.read()
        
        cursor = connection.cursor()
        cursor.execute(sql_query)
        cursor.close()
        print(f"Executed SQL from {fn}")
    except Exception as e:
        print(f"Error executing SQL file: {e}")

def connect_to_postgres():
    """Establish connection to the PostgreSQL database."""
    try:
        connection = psycopg2.connect(
            user="postgres",
            password="1234",
            host="localhost",
            port="5432",
            database=DB_NAME
        )
        connection.autocommit = True
        return connection
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

