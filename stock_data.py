import requests
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import requests
import pytz

# Database Configuration
DB_URL = "postgresql+psycopg2://aryanpatel:12345@localhost:5432/stockanalysis"

# API Base URL
BASE_URL = "https://groww.in/v1/api/charting_service/v4/chart/exchange/NSE/segment/CASH"
# Database Engine
engine = create_engine(DB_URL)

def create_table(stock, interval_key):
    schema_name = stock
    table_name = f"candle_{interval_key}"

    create_schema_sql = f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
            epochtime BIGSERIAL NOT NULL, 
            open FLOAT, 
            high FLOAT, 
            low FLOAT, 
            close FLOAT, 
            volume FLOAT, 
            PRIMARY KEY (epochtime)
        );
    """
    with engine.connect() as conn:
        conn.execute(text(create_schema_sql))
        conn.execute(text(create_table_sql))

# Main function to fetch one year of data
def fetch_one_year_data(stocks, intervals, start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        start_millis, end_millis = get_market_time_millis(current_date)
        for stock in stocks:
            for interval_key, interval_min in intervals.items():
                fetch_and_store(stock, interval_key, interval_min, start_millis, end_millis)
        current_date += timedelta(days=1)  # Move to the next day


# Function to fetch and store data for a single day
def fetch_and_store(stock, interval_key, interval_min, start_millis, end_millis):
    create_table(stock, interval_key)
    url = f"{BASE_URL}/{stock}"
    params = {
        "intervalInMinutes": interval_min,
        "startTimeInMillis": start_millis,
        "endTimeInMillis": end_millis,
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    candles = data.get("candles", [])

    if not candles:
        print(f"No data available for {stock} on this day.")
        return

    insert_data = [
        {
            "epochtime": candle[0],
            "open": candle[1],
            "high": candle[2],
            "low": candle[3],
            "close": candle[4],
            "volume": candle[5],
        }
        for candle in candles
    ]

    insert_sql = f"""
        INSERT INTO "{stock}"."candle_{interval_key}" (epochtime, open, high, low, close, volume)
        VALUES (:epochtime, :open, :high, :low, :close, :volume)
        ON CONFLICT (epochtime) DO NOTHING;
    """
    with engine.connect() as conn:
        for record in insert_data:
            conn.execute(text(insert_sql), **record)

    print(f"Data for stock '{stock}' and interval '{interval_key}' has been stored successfully.")
# Define IST timezone
IST = pytz.timezone("Asia/Kolkata")

# Function to calculate daily start and end millis
def get_market_time_millis(date):
    """
    Get the start and end timestamps in milliseconds for the Indian stock market hours.
    :param date: A datetime object representing the date.
    :return: (start_millis, end_millis)
    """
    start_time = IST.localize(datetime(date.year, date.month, date.day, 9, 15))
    end_time = IST.localize(datetime(date.year, date.month, date.day, 15, 30))
    start_millis = int(start_time.timestamp() * 1000)
    end_millis = int(end_time.timestamp() * 1000)
    return start_millis, end_millis

# Example Usage
if __name__ == "__main__":
    # Define stocks and intervals
    # stocks = [
    # "ADANIENT", "ADANIGREEN", "ADANIPORTS", "ADANIPOWER", "AMBUJACEM", 
    # "APOLLOHOSP", "ASIANPAINT", "AXISBANK", "INOXWIND", "BAJFINANCE", 
    # "BAJAJFINSV", "BANDHANBNK", "BEL", "BPCL", "BHARTIARTL", "BIOCON", 
    # "BRITANNIA", "CIPLA", "COALINDIA", "DABUR", "DIVISLAB", "DRREDDY", 
    # "EICHERMOT", "GAIL", "GODREJCP", "GRASIM", "HCLTECH", "HDFCAMC",
    # "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HAL", "HINDUNILVR",
    # "ICICIBANK", "ICICIGI", "ICICIPRULI", "IOC", "INDUSINDBK", "INFY", "INDIGO", 
    # "ITC", "JSWSTEEL", "KOTAKBANK", "LT", "LICHSGFIN", "M&M", "MARUTI", "NESTLEIND", 
    # "NTPC", "ONGC", "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SUNPHARMA", "TCS", 
    # "TATACONSUM", "TATAMOTORS", "TATAPOWER", "TATASTEEL", "TECHM", "TITAN", "ULTRACEMCO", 
    # "UPL", "WIPRO", "ZOMATO"
    # ]    
    stocks = ["GAIL"]
    intervals = {
    "1m": 1, "2m": 2, "3m": 3, "5m": 5, "10m": 10,
    "15m": 15, "30m": 30, "1h": 60, "4h": 240,
    "1d": 1440, "1w": 10080, "1mo": 43200, "1yr": 525960
    }
    
    # Define date range
    start_date = datetime(2024, 4, 25)
    end_date = datetime(2025, 4, 25)
    
    # Fetch data for one year
    fetch_one_year_data(stocks, intervals, start_date, end_date)











# import requests
# from sqlalchemy import create_engine, Column, Float, BigInteger, MetaData, Table, text
# from sqlalchemy.orm import sessionmaker
# import time
# from datetime import datetime

# # -----------------------------------------
# # DATABASE SETUP
# # -----------------------------------------
# DATABASE_URL = "postgresql://aryanpatel:12345@localhost/stockanalysis"
# engine = create_engine(DATABASE_URL)
# SessionLocal = sessionmaker(bind=engine)

# # -----------------------------------------
# # CONSTANTS
# # -----------------------------------------
# STOCKS = [
#     "ADANIENT", "ADANIGREEN", "ADANIPORTS", "ADANIPOWER", "AMBUJACEM", 
#     "APOLLOHOSP", "ASIANPAINT", "AXISBANK", "INOXWIND", "BAJFINANCE", 
#     "BAJAJFINSV", "BANDHANBNK", "BEL", "BPCL", "BHARTIARTL", "BIOCON", 
#     "BRITANNIA", "CIPLA", "COALINDIA", "DABUR", "DIVISLAB", "DRREDDY", 
#     "EICHERMOT", "GAIL", "GODREJCP", "GRASIM", "HCLTECH", "HDFCAMC",
#     "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HAL", "HINDUNILVR",
#     "ICICIBANK", "ICICIGI", "ICICIPRULI", "IOC", "INDUSINDBK", "INFY", "INDIGO", 
#     "ITC", "JSWSTEEL", "KOTAKBANK", "LT", "LICHSGFIN", "M&M", "MARUTI", "NESTLEIND", 
#     "NTPC", "ONGC", "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SUNPHARMA", "TCS", 
#     "TATACONSUM", "TATAMOTORS", "TATAPOWER", "TATASTEEL", "TECHM", "TITAN", "ULTRACEMCO", 
#     "UPL", "WIPRO", "ZOMATO"
# ]

# INTERVALS = {
#     "1m": 1, "2m": 2, "3m": 3, "5m": 5, "10m": 10,
#     "15m": 15, "30m": 30, "1h": 60, "4h": 240,
#     "1d": 1440, "1w": 10080, "1mo": 43200, "1yr": 525960
# }

# def create_schema(stock_name):
#     # Create schema if not exists
#     with engine.connect() as conn:
#         conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {stock_name}"))
# def create_table(stock_name, interval):
#     # Create the schema if it doesn't exist
#     create_schema(stock_name)
    
#     # Define the table name based on stock and interval
#     table_name = f"candle_{interval}"

#     # SQL to create the table
#     create_table_sql = f"""
#     CREATE TABLE IF NOT EXISTS "{stock_name}"."{table_name}" (
#         epochtime BIGSERIAL NOT NULL, 
#         open FLOAT, 
#         high FLOAT, 
#         low FLOAT, 
#         close FLOAT, 
#         volume FLOAT, 
#         PRIMARY KEY (epochtime)
#     );
#     """

#     # Execute the SQL to create the table
#     with engine.connect() as conn:
#         conn.execute(text(create_table_sql))

# # Convert readable date to millis
# def to_millis(dt_str):
#     dt = datetime.strptime(dt_str, "%Y-%m-%d")
#     return int(dt.timestamp() * 1000)

# # -----------------------------------------
# # DATABASE FUNCTIONS
# # -----------------------------------------

# def create_candle_table(stock: str, interval_key: str):
#     # First, create schema explicitly
    

#     with engine.connect() as conn:
#         conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{stock}"'))  # ✅ fix: add quotes to handle case sensitivity

#     # Now create table inside that schema
#     metadata = MetaData(schema=stock)
#     table = Table(
#         f"candle_{interval_key}",
#         metadata,
#         Column("epochtime", BigInteger, primary_key=True),
#         Column("open", Float),
#         Column("high", Float),
#         Column("low", Float),
#         Column("close", Float),
#         Column("volume", Float),
#     )

#     metadata.create_all(bind=engine)
#     print(f"[✓] Created table: {stock}.candle_{interval_key}")


# def insert_candles(stock: str, interval_key: str, candles: list):
#     table_name = f"{stock}.candle_{interval_key}"
#     session = SessionLocal()
#     try:
#         for candle in candles:
#             query = text(f"""
#                 INSERT INTO {table_name} (epochtime, open, high, low, close, volume)
#                 VALUES (:t, :o, :h, :l, :c, :v)
#                 ON CONFLICT (epochtime) DO NOTHING
#             """)
#             session.execute(query, {
#                 "t": candle[0], "o": candle[1], "h": candle[2],
#                 "l": candle[3], "c": candle[4], "v": candle[5]
#             })
#         session.commit()
#         print(f"[+] Inserted {len(candles)} into {table_name}")
#     finally:
#         session.close()

# # -----------------------------------------
# # FETCH & STORE FUNCTION
# # -----------------------------------------

# def fetch_and_store(stock: str, interval_key: str, interval_min: int, start_millis: int, end_millis: int):
#     create_table(stock, interval_key)
#     url = f"https://groww.in/v1/api/charting_service/v4/chart/exchange/NSE/segment/CASH/{stock}"
#     params = {
#         "intervalInMinutes": interval_min,
#         "startTimeInMillis": start_millis,
#         "endTimeInMillis": end_millis,
#     }

#     try:
#         res = requests.get(url, params=params)
#         if res.status_code == 200:
#             json_data = res.json()
#             candles = json_data.get("candles", [])
#             if candles:
#                 create_candle_table(stock, interval_key)
#                 insert_candles(stock, interval_key, candles)
#             else:
#                 print(f"[!] No candles for {stock} {interval_key}")
#         else:
#             print(f"[X] Failed: {stock} {interval_key} => {res.status_code}")
#     except Exception as e:
#         print(f"[!] Exception fetching {stock} {interval_key}: {e}")

# # -----------------------------------------
# # MAIN LOOP
# # -----------------------------------------

# if __name__ == "__main__":
#     # 1-year range
#     start_millis = to_millis("2024-04-24")
#     end_millis = to_millis("2025-04-25")

#     for stock in STOCKS:
#         for interval_key, interval_val in INTERVALS.items():
#             fetch_and_store(stock, interval_key, interval_val, start_millis, end_millis)
