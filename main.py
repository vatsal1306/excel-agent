from fastapi import FastAPI
from src.database import init_db

app = FastAPI(title="Excel Email Automation")

@app.on_event("startup")
def startup():
    init_db()

@app.get("/health")
def health():
    return {"status": "ok"}