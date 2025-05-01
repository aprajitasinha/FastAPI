# main.py
import datetime
from fastapi import FastAPI, HTTPException, Query
import psycopg2
from pydantic import BaseModel
from typing import Union

import requests
from app.db import SessionLocal, engine
from fastapi.responses import RedirectResponse
from psycopg2.extras import execute_values


#define a pydantic model
class Item(BaseModel):
    name: str
    description: str 
    price: float
    tax: float 
    is_offer: bool 
    
# Database configuration
def get_connection():
    return psycopg2.connect(
        database="stockanalysis",
        user="aryanpatel",
        password="12345",
        host="localhost",
        port="5432"
    )
BASE_URL = "https://groww.in/v1/api/charting_service/v4/chart/exchange/NSE/segment/CASH/{stock}"
interval_map = {
    "1m": 1, "2m": 2, "3m": 3, "5m": 5, "10m": 10, "15m": 15,
    "30m": 30, "1h": 60, "4h": 240, "1d": 1440, "1w": 10080
}
stocks_list = [
    "ADANIENT", "ADANIGREEN", "ADANIPORTS", "ADANIPOWER","AMBUJACEM", "APOLLOHOSP", "ASIANPAINT","AXISBANK", "INOXWIND","BAJFINANCE","BAJAJFINSV","BANDHANBNK","BEL",
    "BPCL","BHARTIARTL","BIOCON","BRITANNIA", "CIPLA","COALINDIA","DABUR", "DIVISLAB","DRREDDY","EICHERMOT","GAIL","GODREJCP", "GRASIM", "HCLTECH","HDFCAMC","HDFCBANK",
    "HDFCLIFE","HEROMOTOCO", "HINDALCO", "HAL","HINDUNILVR","ICICIBANK","ICICIGI", "ICICIPRULI","IOC", "INDUSINDBK","INFY", "INDIGO","ITC","JSWSTEEL","KOTAKBANK", "LT",
    "LICHSGFIN", "M&M","MARUTI", "NESTLEIND", "NTPC", "ONGC", "POWERGRID","RELIANCE", "SBILIFE","SBIN","SUNPHARMA", "TCS", "TATACONSUM","TATAMOTORS", "TATAPOWER", "TATASTEEL",
    "TECHM", "TITAN", "ULTRACEMCO","UPL", "WIPRO","ZOMATO"
]

# Database connection helper
# def get_db():


# Base.metadata.create_all(bind=engine)
# DB ENGINE

app = FastAPI()
# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        

@app.get("/")
def read_root():
    return RedirectResponse(url="/docs")

@app.get("/items/{item_id}")
def read_item(item_id: int, q: str = None):
    pass
    

@app.put("/items/{item_id}")
def update_item(item_id: int, item: Item):
    return {"item_name": item.name, "item_id": item_id}




#create API indpoint
@app.post("/items/")
async def create_item(item: Item):
    total_price = item.price + (item.tax if item.tax else 0)
    return{
        "name": item.name,
        "description": item.description,
        "price": item.price,
        "tax": item.tax,
        "total_price": total_price
    }

@app.get("/stocklist/{stock}/{interval}/{start}/{end}")
async def getstock_list(stock: str, 
                        interval: str, 
                        start: str, 
                        end: str ):
    
        ## Validate interval
        if interval not in interval_map:
            raise  HTTPException(status_code=400, detail="Invalid interval. Valid intervals are: " + ", ".join(interval_map.keys()))
        
        ##  Validate start and end dates
        try:
            start_date=datetime.datetime.strptime(start, "%Y-%m-%d")
            end_date=datetime.datetime.strptime(end, "%Y-%m-%d")
            if end_date < start_date:
                raise HTTPException(status_code=400, detail="End date must be after start date.")
    
        except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
         ## Convert dates to epoch time
        start_epoch = int(start_date.timestamp() * 1000)
        end_epoch = int(end_date.timestamp() * 1000)
        #fetch data from groww API
        try:
            conn = get_connection()
            cursor = conn.cursor()
            fetch_query = f"""
                SELECT * FROM "{stock}".candle_{interval}
                WHERE epochtime >= %s AND epochtime <= %s
            """
            cursor.execute(fetch_query, (start_epoch, end_epoch))
            rows = cursor.fetchall()
            
            ## If no data exists, fetch from external API
            if not rows:
                url=BASE_URL.format(stock=stock)
                params = {
                    "intervalInMinutes": interval_map[interval],
                    "startTimeInMillis": start_epoch,
                    "endTimeInMillis": end_epoch,
                }
                response = requests.get(url, params=params)
                if response.status_code == 200:
                    response_json = response.json()
                    candles = response_json.get("candles", [])
                ## Prepare data for insertion
                    data = [
                        (candle[0], candle[1], candle[2], candle[3], candle[4], candle[5])
                        for candle in candles
                    ]
            
                ## Insert data into database
                    insert_query = f"""
                    INSERT INTO "{stock}".candle_{interval} (epochtime, open, high, low, close, volume)
                    VALUES %s
                    ON CONFLICT (epochtime) DO NOTHING
                    """
            
                    execute_values(cursor, insert_query, data)
                    conn.commit()
                    return {"message": f"Data fetched and inserted for {stock} ({interval}).", "data": candles}
                else:
                    raise HTTPException(status_code=500, detail=f"Failed to fetch data from external API. Status: {response.status_code}")

            return {"data": rows}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()

                
            

            
            
        
        
        
            
        

        
        
        
        
    


