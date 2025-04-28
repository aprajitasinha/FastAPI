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
        
        
class Item(BaseModel):
    name: str
    price: float
    is_offer: Union[bool, None] = None

@app.get("/")
def read_root():
    async def read_root():
        return {"message": "Hello, FastAPI!"}

@app.get("/items/{item_id}")
def read_item(item_id: int, q: str = None):
    async def read_item(item_id: int, q: str = None):
        """
        Read item by ID and optional query parameter.
        :param item_id: Item ID.
        :param q: Optional query parameter.
        :return: Dictionary with item ID and query parameter.
        """
         # Return a dictionary with item ID and query parameter
         # This is a placeholder implementation
         # In a real application, you would fetch the item from a database or other data source
         # return {"item_id": item_id, "q": q}
    return {"item_id": item_id, "q": q}

@app.put("/items/{item_id}")
def update_item(item_id: int, item: Item):
    return {"item_name": item.name, "item_id": item_id}



