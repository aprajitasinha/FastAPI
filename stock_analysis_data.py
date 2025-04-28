import psycopg2
import requests
from datetime import datetime, timedelta
import pytz
from psycopg2.extras import execute_values

# Stocks List
stocks_list = [
    "ADANIENT", "ADANIGREEN"]

# Interval Mapping
interval_map = {
    "1m": 1, "2m": 2, "3m": 3, "5m": 5, "10m": 10, "15m": 15,
    "30m": 30, "1h": 60, "4h": 240, "1d": 1440, "1w": 10080
}

BASE_URL = "https://groww.in/v1/api/charting_service/v4/chart/exchange/NSE/segment/CASH/{stock}"

# Database connection
def connection():
    conn = psycopg2.connect(
        database="stockanalysis",
        user='aryanpatel',
        password="12345",
        host="localhost",
        port="5432"
    )
    return conn

# Create table
def create_table(conn, stock_name, interval_name):
    
    conn.cursor().execute(f'CREATE SCHEMA IF NOT EXISTS "{stock_name}"')  # Use quotes to handle special characters

    conn.cursor().execute(f"""
        CREATE TABLE IF NOT EXISTS "{stock_name}".candle_{interval_name} (
            id SERIAL PRIMARY KEY,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume FLOAT,
            epochtime BIGINT UNIQUE NOT NULL
        )
    """)
    conn.commit()
    

# Insert data
def insert_data_to_table(stock_name, interval_name, data , conn):
    """
    insert multiple data into table
    """
    ## bulk insert
    insert_query = f"""INSERT INTO "{stock_name}".candle_{interval_name} (epochtime , open, high, low, close, volume)
                    VALUES %s
                ON CONFLICT (epochtime) DO NOTHING
            """
    execute_values(conn.cursor(), insert_query, data)
    
    conn.commit()
    


# Fetch data
def fetch_groww_data(stock, interval_minutes, interval_epoch_time):
    
    data = []
    for each_interval in interval_epoch_time:
        url = BASE_URL.format(stock=stock)
        params = {
            "intervalInMinutes": interval_minutes,
            "startTimeInMillis": int(each_interval[0]),
            "endTimeInMillis": int(each_interval[1]),
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            response_json = response.json()
            candles = response_json.get("candles", [])
            
        else:
            print(f"Failed to fetch data for {stock} ({interval_minutes} min). Status: {response.status_code}")
            return []
        data.extend(candles)
    return data

# Generate intraday intervals
def generate_weekly_intervals():
    """
    this function take the argument as start_date and end_date
    
    input is start_Date  a datetime object
    input is end_date  a datetime object
    
    """
    start_date = datetime(2025 , 1, 1) # Example start date    
    #create intervals every week after 1 week and end week is today
    
    today = datetime.now()
    ##end date should be startdate + 7 days until today
    intervals = []
    
    while start_date < today:
        intervals.append((start_date, start_date + timedelta(days=7)))
        start_date = start_date + timedelta(days=7)
        
    return intervals
        

def epoch_time_converter(intervals):
    """
    this function take the argument as start_date and end_date
    
    input is start_Date  a datetime object
    input is end_date  a datetime object
    
    """
    epoch_intervals = []
    
    for start, end in intervals:
        start_epoch = int(start.timestamp() * 1000)
        end_epoch = int(end.timestamp() * 1000)
        epoch_intervals.append((start_epoch, end_epoch))
        
    return epoch_intervals
    
def insert_data(week_intervals_epoch):
    """
    this function take the argument as start_date and end_date
    
    input is start_Date  a datetime object
    input is end_date  a datetime object
    
    """
    try:
    
        conn = connection()
        
        for each_stocks in stocks_list:
            for interval_key , interval_value in interval_map.items():
                
                ### create table if not exists
                create_table(conn, each_stocks, interval_key)
                print(f"Table '{each_stocks}.candle_{interval_key}' created or already exists.")
                
                
                # Fetch data
                
                
                data = fetch_groww_data(each_stocks, interval_value, week_intervals_epoch)
                
                insert_data_to_table(each_stocks , interval_key , data , conn)
                
                if not data:
                    print(f"No data available for {each_stocks} from {week_intervals_epoch[0][0]} to {week_intervals_epoch[0][1]}.")
                    break
                
                
            
            # Create table if it doesn't exist
        
            
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        if conn:
            conn.close()
    
    
# Main flow
if __name__ == "__main__":
    

    week_intervals = generate_weekly_intervals()
    
    week_intervals_epoch = epoch_time_converter(week_intervals)
    
    insert_data(week_intervals_epoch)


    
