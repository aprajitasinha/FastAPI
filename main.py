# main.py
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Union
from sqlalchemy.orm import Session
from typing import List
from datetime import datetime, timedelta
import time
import httpx
from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection
from sqlalchemy import Table, Column, MetaData, BigInteger, Float, text
import app.model as models  
# from app.db import SessionLocal, engine
from app.db import Base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from app.db import SessionLocal, engine
import app.schemas as schemas




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
        
        
class Item(BaseModel):
    name: str
    price: float
    is_offer: Union[bool, None] = None

@app.get("/")
def read_root():
    return {"message": "Hello, FastAPI!"}

@app.get("/items/{item_id}")
def read_item(item_id: int, q: str = None):
    return {"item_id": item_id, "q": q}

@app.put("/items/{item_id}")
def update_item(item_id: int, item: Item):
    return {"item_name": item.name, "item_id": item_id}



# CONFIGS
# DATABASE_URL = "postgresql+asyncpg://aryanpatel:12345@localhost:5432/stockanalysis"
# STOCKS = [ "ADANIENT", "ADANIGREEN", "ADANIPORTS", "ADANIPOWER", "AMBUJACEM", 
#           "APOLLOHOSP", "ASIANPAINT", "AXISBANK", "INOXWIND", "BAJFINANCE", 
#           "BAJAJFINSV", "BANDHANBNK", "BEL", "BPCL", "BHARTIARTL", "BIOCON", 
#           "BRITANNIA", "CIPLA", "COALINDIA", "DABUR", "DIVISLAB", "DRREDDY", 
#           "EICHERMOT", "GAIL", "GODREJCP", "GRASIM", "HCLTECH", "HDFCAMC",
#           "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HAL", "HINDUNILVR",
#           "ICICIBANK", "ICICIGI", "ICICIPRULI", "IOC", "INDUSINDBK", "INFY", "INDIGO", 
#           "ITC", "JSWSTEEL", "KOTAKBANK", "LT", "LICHSGFIN", "M&M", "MARUTI", "NESTLEIND", "NTPC", "ONGC", "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", "TATAPOWER", "TATASTEEL", "TECHM", "TITAN", "ULTRACEMCO", "UPL", "WIPRO", "ZOMATO" ]
# INTERVALS = {
#     "1m": 1, "2m": 2, "3m": 3, "5m": 5, "10m": 10,
#     "15m": 15, "30m": 30, "1h": 60, "4h": 240,
#     "1d": 1440, "1w": 10080, "1mo": 43200,"1yr": 525960
# }
# BASE_URL = "https://groww.in/v1/api/charting_service/v4/chart/exchange/NSE/segment/CASH/{stock}"

# # DB ENGINE


# def to_ms(dt: datetime) -> int:
#     return int(time.mktime(dt.timetuple()) * 1000)


# def get_chunk_size(interval_min: int) -> int:
#     """Return chunk duration in minutes based on interval."""
#     if interval_min < 60:
#         return 60 * 24 * 7   # 1 week
#     elif interval_min <= 1440:
#         return 60 * 24 * 30  # 1 month
#     else:
#         return 60 * 24 * 90  # 3 months for large intervals


# async def create_schema_and_table(conn: AsyncConnection, schema: str, interval_name: str):
#     table_name = f"candle_{interval_name}"
#     metadata = MetaData(schema=schema)

#     table = Table(
#         table_name,
#         metadata,
#         Column("epochtime", BigInteger, primary_key=True),
#         Column("open", Float),
#         Column("high", Float),
#         Column("low", Float),
#         Column("close", Float),
#         Column("volume", Float),
#     )

#     await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
#     await conn.run_sync(metadata.create_all)


# async def insert_data(conn: AsyncConnection, schema: str, interval_name: str, candles: List[list]):
#     table_name = f"{schema}.candle_{interval_name}"

#     for c in candles:
#         try:
#             await conn.execute(
#                 text(f"""
#                     INSERT INTO {table_name} (epochtime, open, high, low, close, volume)
#                     VALUES (:t, :o, :h, :l, :c, :v)
#                     ON CONFLICT (epochtime) DO NOTHING
#                 """),
#                 {
#                     "t": c[0],
#                     "o": c[1],
#                     "h": c[2],
#                     "l": c[3],
#                     "c": c[4],
#                     "v": c[5],
#                 }
#             )
#         except Exception as e:
#             print(f"Error inserting: {e}")


# @app.get("/fetch-all/")
# async def fetch_all_data():
#     print("STARTING FETCH ALL...")
#     now = datetime.now()
#     one_year_ago = now - timedelta(days=365)
#     results = {}

#     async with engine.begin() as conn:
#         async with httpx.AsyncClient() as client:
#             for stock in STOCKS:
#                 results[stock] = {}
#                 for interval_name, interval_min in INTERVALS.items():
#                     await create_schema_and_table(conn, stock, interval_name)

#                     chunk_min = get_chunk_size(interval_min)
#                     start_time = one_year_ago
#                     stock_data = []

#                     while start_time < now:
#                         end_time = min(start_time + timedelta(minutes=chunk_min), now)
#                         start_ms = to_ms(start_time)
#                         end_ms = to_ms(end_time)

#                         url = BASE_URL.format(stock=stock)
#                         params = {
#                             "startTimeInMillis": start_ms,
#                             "endTimeInMillis": end_ms,
#                             "intervalInMinutes": interval_min,
#                         }

#                         try:
#                             resp = await client.get(url, params=params)
#                             resp.raise_for_status()
#                             data = resp.json() if hasattr(resp, "json") else {}
#                             candles = data.get("candles", [])
#                             await insert_data(conn, stock, interval_name, candles)
#                             stock_data.extend(candles)
#                             print(f"Requesting URL: {url} with params: {params}")

#                         except Exception as e:
#                             print(f"[{stock} - {interval_name}] Error: {e}")
#                             break

#                         start_time = end_time

#                     results[stock][interval_name] = f"Inserted {len(stock_data)} rows"

#     return results
