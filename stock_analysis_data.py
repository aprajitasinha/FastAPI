from datetime import datetime, timedelta
import asyncio 
import asyncpg
import logging
import httpx

logging.basicConfig(level=logging.INFO)


# Stocks List
stocks_list = ["ADANIENT", "ADANIGREEN", "ADANIPORTS", "ADANIPOWER","AMBUJACEM", "APOLLOHOSP", "ASIANPAINT","AXISBANK", "INOXWIND","BAJFINANCE","BAJAJFINSV","BANDHANBNK","BEL",
    "BPCL","BHARTIARTL","BIOCON","BRITANNIA", "CIPLA","COALINDIA","DABUR", "DIVISLAB","DRREDDY","EICHERMOT","GAIL","GODREJCP", "GRASIM", "HCLTECH","HDFCAMC","HDFCBANK",
    "HDFCLIFE","HEROMOTOCO", "HINDALCO", "HAL","HINDUNILVR","ICICIBANK","ICICIGI", "ICICIPRULI","IOC", "INDUSINDBK","INFY", "INDIGO","ITC","JSWSTEEL","KOTAKBANK", "LT",
    "LICHSGFIN", "M&M","MARUTI", "NESTLEIND", "NTPC", "ONGC", "POWERGRID","RELIANCE", "SBILIFE","SBIN","SUNPHARMA", "TCS", "TATACONSUM","TATAMOTORS", "TATAPOWER", "TATASTEEL",
    "TECHM", "TITAN", "ULTRACEMCO","UPL", "WIPRO","ZOMATO"]

# Interval Mapping
interval_map = {
    "1m": 1, "2m": 2, "3m": 3, "5m": 5, "10m": 10, "15m": 15,
    "30m": 30, "1h": 60, "4h": 240, "1d": 1440, "1w": 10080
}

BASE_URL = "https://groww.in/v1/api/charting_service/v4/chart/exchange/NSE/segment/CASH/{stock}"

# Database connection
async def connection():
    try:
        conn = await asyncpg.connect(
            database="stockanalysis",
            user='aryanpatel',
            password="12345",
            host="localhost",
            port="5432"
        )
        logging.info("Connected to the database successfully.")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        # return None
        raise e


# Create table
async def create_table(conn, stock_name, interval_name):
    
    await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{stock_name}"')  # Use quotes to handle special characters

    await conn.execute(f"""
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
    
    

# Insert data
async def insert_data_to_table(stock_name, interval_name, data , conn):
    """
    insert multiple data into table
    """
    if conn is None:
        raise ValueError("Database connection is None in insert_data_to_table")

    table_name = f'"{stock_name}"."candle_{interval_name}"'

    ## bulk insert
    
    insert_query = f'''INSERT INTO {table_name}
                            (epochtime, open, high, low, close, volume)
                            VALUES($1, $2, $3, $4, $5, $6)
                            N CONFLICT (epochtime) UPDATE
                            SET open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                close = EXCLUDED.close,
                                volume = EXCLUDED.volume;'''
        #logging.info(f"Insert query: {data}")
    try:   
        await conn.executemany(insert_query,data)
        
    except Exception as e:
        logging.error(f"Error inserting data into table: {e}")    
    


# Fetch data
async def fetch_groww_data(stock, interval_minutes, interval_epoch_time):
    
    data = []
    #interval_epoch_time=interval_epoch_time[:1]
    async with httpx.AsyncClient() as client:
        for start_time, end_time  in interval_epoch_time:
            response= await client.get(BASE_URL.format(stock=stock), params={
                "intervalInMinutes": interval_minutes,
                "startTimeInMillis": start_time,
                "endTimeInMillis": end_time
            })
        
            if response.status_code == 200:
                response_json=response.json()
                logging.info(f"Response JSON: {response_json}")
                data.extend(response_json["candles"])
            else:
                data.extend([])
        return data
    
        
            

# Generate intraday intervals
def generate_weekly_intervals():
    """
    this function take the argument as start_date and end_date
    
    input is start_Date  a datetime object
    input is end_date  a datetime object
    
    """
    start_date = datetime(2025 , 2, 1) # Example start date    
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
    
async def insert_data(week_intervals_epoch,stock_name,interval_name,interval_value):
    """
    this function take the argument as start_date and end_date
    
    input is start_Date  a datetime object
    input is end_date  a datetime object
    
    """
    conn = None 
    try:
        conn= await connection()   
        data=await fetch_groww_data(stock_name, interval_value, week_intervals_epoch)  
        await create_table(conn, stock_name, interval_name)   
        await insert_data_to_table(stock_name, interval_name, data, conn) 
    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        if conn:
            await conn.close()
    
async def main():
    task=[]
    for each_stock in stocks_list:
        for interval_key, interval_value in interval_map.items():
           week_intervals = generate_weekly_intervals()
           week_intervals_epoch = epoch_time_converter(week_intervals)
           task.append(insert_data(week_intervals_epoch, each_stock, interval_key,interval_value))
    await asyncio.gather(*task)
    
    
# Main flow
if __name__ == "__main__":
     

    # week_intervals = generate_weekly_intervals()
    
    # week_intervals_epoch = epoch_time_converter(week_intervals)
    
    # insert_data(week_intervals_epoch)
    asyncio.run(main())


    
