"""
Mission Data Grid Dashboard
Real-time multi-view video + alerts + intel + search
"""
import os, json, asyncio, logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import redis.asyncio as aioredis
import httpx

app = FastAPI(title="Mission Dashboard")
templates = Jinja2Templates(directory="templates")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
GRID_API = os.getenv("GRID_API_URL", "http://mission-grid:8002")
VSS_API = os.getenv("VSS_API_URL", "http://vss-agent:8000")

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request,
                                                       "grid_api": GRID_API,
                                                       "vss_api": VSS_API})

@app.get("/api/alerts")
async def get_alerts():
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{GRID_API}/alerts/recent?limit=20")
        return r.json()

@app.get("/api/stats")
async def get_stats():
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{GRID_API}/stats")
        return r.json()

@app.post("/api/search")
async def search(request: Request):
    body = await request.json()
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(f"{VSS_API}/search", json=body)
        return r.json()

@app.post("/api/query")
async def query(request: Request):
    body = await request.json()
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(f"{VSS_API}/query", json=body)
        return r.json()

@app.get("/health")
def health():
    return {"status": "ok", "service": "dashboard"}