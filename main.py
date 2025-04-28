# main.py
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Union
from app.db import SessionLocal, engine




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
    pass

@app.get("/items/{item_id}")
def read_item(item_id: int, q: str = None):
    pass
    

@app.put("/items/{item_id}")
def update_item(item_id: int, item: Item):
    return {"item_name": item.name, "item_id": item_id}



#define a pydantic model
class Item(BaseModel):
    name: str
    description: str 
    price: float
    tax: float 
    is_offer: bool 

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



