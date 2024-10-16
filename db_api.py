from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from db_manager import DbManager


app = FastAPI()

db_manager = DbManager()


class DatabaseRequest(BaseModel):
    db_name: str
    table_name: str = None
    values: list = None
    columns: list = None
    _id: int = None


@app.on_event("startup")
async def startup_event():
    db_manager.load()


@app.get("/databases")
async def fetch_databases_and_tables():
    return {"databases": db_manager.fetch_databases_and_tables()}


@app.post("/create_database")
async def create_database(request: DatabaseRequest):
    if not request.db_name:
        raise HTTPException(status_code=400, detail="Database name is required")
    db_manager.create_database(request.db_name)
    return {"status": "Database created"}


@app.post("/create_table")
async def create_table(request: DatabaseRequest):
    if not all([request.db_name, request.table_name, request.columns]):
        raise HTTPException(status_code=400, detail="Missing required fields")
    db_manager.create_table(request.db_name, request.table_name, request.columns)
    return {"status": "Table created"}


@app.post("/insert_row")
async def insert_row(request: DatabaseRequest):
    if not all([request.db_name, request.table_name, request.values]):
        raise HTTPException(status_code=400, detail="Missing required fields")
    db_manager.insert_row(request.db_name, request.table_name, request.values)
    return {"status": "Record inserted"}

@app.put("/update_row")
async def update_row(request: DatabaseRequest):
    if not all([request.db_name, request.table_name, request._id, request.values]):
        raise HTTPException(status_code=400, detail="Missing required fields")
    db_manager.update_row(request.db_name, request.table_name, request._id, request.values)
    return {"status": "Record updated"}


@app.delete("/delete_row")
async def delete_row(request: DatabaseRequest):
    if not all([request.db_name, request.table_name, request._id]):
        raise HTTPException(status_code=400, detail="Missing required fields")
    db_manager.delete_row(request.db_name, request.table_name, request._id)
    return {"status": "Record deleted"}


@app.delete("/delete_table")
async def delete_table(request: DatabaseRequest):
    if not all([request.db_name, request.table_name]):
        raise HTTPException(status_code=400, detail="Missing required fields")
    db_manager.delete_table(request.db_name, request.table_name)
    return {"status": "Table deleted"}


@app.delete("/drop_database")
async def drop_database(request: DatabaseRequest):
    if not request.db_name:
        raise HTTPException(status_code=400, detail="Database name is required")
    db_manager.drop_database(request.db_name)
    return {"status": "Database deleted"}


@app.get("/table_data")
async def get_table_data(db_name: str, table_name: str):
    if not all([db_name, table_name]):
        raise HTTPException(status_code=400, detail="Missing required fields")
    _, columns, rows = db_manager.get_table_data(db_name, table_name)
    return {"columns": columns, "rows": rows}


@app.post("/delete_repeated")
async def delete_repeated(request: DatabaseRequest):
    if not all([request.db_name, request.table_name]):
        raise HTTPException(status_code=400, detail="Missing required fields")
    num = db_manager.delete_repeated(request.db_name, request.table_name)
    return {"num": num}
