import requests
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import pytz

# Database Configuration
DB_URL = "postgresql+psycopg2://aryanpatel:12345@localhost:5432/stockanalysis"
BASE_URL = "https://groww.in/v1/api/charting_service/v4/chart/exchange/NSE/segment/CASH"
engine = create_engine(DB_URL)
def create_table(stock, interval_key):
    """Create schema and table dynamically."""
    schema_name = stock  # Schema name will be the stock name (e.g., "GAIL")
    table_name = f"candle_{interval_key}"  # Table name based on interval (e.g., "candle_1m")

    # SQL to create schema if it does not exist
    create_schema_sql = f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'

    # SQL to create table if it does not exist
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
            epochtime BIGINT NOT NULL, 
            open FLOAT, 
            high FLOAT, 
            low FLOAT, 
            close FLOAT, 
            volume FLOAT, 
            PRIMARY KEY (epochtime)
        );
    """

    # Execute SQL statements
    with engine.connect() as conn:
        # Create schema
        conn.execute(text(create_schema_sql))
        print(f"Schema '{schema_name}' created or already exists.")
        # Create table
        conn.execute(text(create_table_sql))
        print(f"Table '{table_name}' created or already exists in schema '{schema_name}'.")
        
        
        
def get_market_time_intervals(date):
    """
    Generate 1-minute intervals for the Indian stock market hours.
    :param date: A datetime object representing the date.
    :return: List of (start_millis, end_millis) tuples for each 1-minute interval.
    """
    IST = pytz.timezone("Asia/Kolkata")
    start_time = IST.localize(datetime(date.year, date.month, date.day, 9, 15))
    end_time = IST.localize(datetime(date.year, date.month, date.day, 15, 30))
    
    intervals = []
    current_time = start_time
    while current_time < end_time:
        next_time = current_time + timedelta(minutes=1)
        intervals.append((
            int(current_time.timestamp() * 1000),  # startTimeInMillis
            int(next_time.timestamp() * 1000)     # endTimeInMillis
        ))
        current_time = next_time
    return intervals

def fetch_and_store(stock, interval_key, interval_min, intervals):
    """Fetch and store data for each 1-minute interval."""
    create_table(stock, interval_key)  # Ensure schema and table exist

    for start_millis, end_millis in intervals:
        url = f"{BASE_URL}/{stock}"
        params = {
            "intervalInMinutes": interval_min,
            "startTimeInMillis": start_millis,
            "endTimeInMillis": end_millis,
        }
        print(url,params)

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            candles = data.get("candles", [])
            
            if not candles:
                print(f"No data available for {stock} from {start_millis} to {end_millis}.")
                continue

            # Prepare data for insertion with epochtime in milliseconds
            insert_data = [
                {
                    "epochtime": candle[0] * 1000,  # Convert seconds to milliseconds
                    "open": candle[1],
                    "high": candle[2],
                    "low": candle[3],
                    "close": candle[4],
                    "volume": candle[5],
                }
                for candle in candles
            ]

            # SQL Insert Query
            insert_sql = text(f"""
                INSERT INTO "{stock}"."candle_{interval_key}" 
                (epochtime, open, high, low, close, volume)
                VALUES (:epochtime, :open, :high, :low, :close, :volume)
                ON CONFLICT (epochtime) DO NOTHING;
            """)

            # Execute Insert Query
            with engine.connect() as conn:
                for record in insert_data:
                    # Debugging: Print the record being inserted
                    print(f"Inserting Record: {record}")
                    conn.execute(insert_sql, parameters=record)

            print(f"Data for stock '{stock}' and interval '{interval_key}' from {start_millis} to {end_millis} has been stored successfully.")
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch data for {stock}: {e}")
        except Exception as e:
            print(f"An error occurred while storing data for {stock}: {e}")

def fetch_one_day_data(stock, interval_key, interval_min, date):
    """
    Fetch data for an entire day in 1-minute intervals.
    :param stock: Stock symbol (e.g., 'GAIL').
    :param interval_key: Interval key (e.g., '1m').
    :param interval_min: Interval duration in minutes.
    :param date: Date for which the data is to be fetched.
    """
    intervals = get_market_time_intervals(date)
    fetch_and_store(stock, interval_key, interval_min, intervals)

# Example Usage
if __name__ == "__main__":
    stocks = ["GAIL"]  # Add other stocks as needed
    interval_key = "1m"  # 1-minute interval
    interval_min = 1  # 1-minute interval duration
    date = datetime(2025, 4, 24)  # Example date

    for stock in stocks:
        fetch_one_day_data(stock, interval_key, interval_min, date)
        
        
