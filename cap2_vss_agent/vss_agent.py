"""
CAP 2: Video Search & Summarization Agent
Uses NVIDIA NIM (Cosmos Reason VLM + Llama LLM + NeMo Retriever)
via OpenAI-compatible API
REST API: POST /search, POST /summarize, POST /query
"""
import os, json, base64, asyncio, logging
from datetime import datetime
from typing import Optional
import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import AsyncOpenAI
from kafka import KafkaProducer, KafkaConsumer
import asyncpg

log = logging.getLogger("VSS-Agent")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Mission VSS Agent", version="2.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

NVIDIA_API_KEY = os.getenv("NVIDIA_API_KEY", "")
NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://mission:mission_secret@postgres:5432/mission_grid")

# NVIDIA NIM clients (OpenAI-compatible)
vlm_client = AsyncOpenAI(api_key=NVIDIA_API_KEY, base_url=NVIDIA_BASE_URL)
llm_client = AsyncOpenAI(api_key=NVIDIA_API_KEY, base_url=NVIDIA_BASE_URL)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode()
)

# ── Models ─────────────────────────────────────────────────────────────
class SearchRequest(BaseModel):
    query: str
    cameras: Optional[list[str]] = None
    time_range_minutes: Optional[int] = 30
    max_results: Optional[int] = 10

class SummarizeRequest(BaseModel):
    event_ids: list[str]
    mission_context: Optional[str] = "general surveillance"

class QueryRequest(BaseModel):
    question: str
    camera_id: Optional[str] = None
    time_range_minutes: Optional[int] = 60

# ── Frame Encoder ──────────────────────────────────────────────────────
def encode_frame_b64(frame_bytes: bytes) -> str:
    return base64.b64encode(frame_bytes).decode("utf-8")

# ── VLM Frame Analysis ─────────────────────────────────────────────────
async def analyze_frame_with_vlm(frame_b64: str, query: str) -> dict:
    """Use Cosmos Reason VLM to analyze a video frame."""
    try:
        response = await vlm_client.chat.completions.create(
            model="nvidia/cosmos-reason1-8b",
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{frame_b64}"}
                    },
                    {
                        "type": "text",
                        "text": f"""Mission intelligence query: {query}
                        
Analyze this video frame and provide:
1. Objects detected and their positions
2. Activities or behaviors observed
3. Threat or alert indicators (if any)
4. Relevance score (0.0-1.0) to the query
5. Recommended mission action

Respond in JSON format."""
                    }
                ]
            }],
            max_tokens=1024,
            temperature=0.1
        )
        content = response.choices[0].message.content
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return {"raw_analysis": content, "relevance_score": 0.5}
    except Exception as e:
        log.error(f"VLM analysis error: {e}")
        return {"error": str(e), "relevance_score": 0.0}

# ── LLM Mission Summarizer ─────────────────────────────────────────────
async def summarize_with_llm(events: list[dict], context: str) -> str:
    """Use Llama 3.1 to create an actionable mission brief from events."""
    events_json = json.dumps(events[:20], indent=2)  # cap at 20 events
    try:
        response = await llm_client.chat.completions.create(
            model="meta/llama-3.1-70b-instruct",
            messages=[
                {
                    "role": "system",
                    "content": """You are a mission intelligence analyst. 
Create concise, actionable mission briefs from video surveillance events.
Focus on: threat level, key entities, timeline, recommended actions.
Format: structured markdown with priority levels."""
                },
                {
                    "role": "user",
                    "content": f"""Mission Context: {context}

Events to analyze:
{events_json}

Generate a comprehensive mission intelligence brief."""
                }
            ],
            max_tokens=2048,
            temperature=0.2
        )
        return response.choices[0].message.content
    except Exception as e:
        log.error(f"LLM summarization error: {e}")
        return f"Summarization error: {e}"

# ── API Endpoints ──────────────────────────────────────────────────────
@app.post("/search")
async def search_video(req: SearchRequest):
    """Semantic video search across indexed events."""
    redis_client = aioredis.from_url(REDIS_URL)
    # Retrieve recent events from Redis index
    pattern = f"event:*"
    keys = await redis_client.keys(pattern)
    events = []
    for key in keys[:200]:  # scan recent 200 events
        raw = await redis_client.get(key)
        if raw:
            event = json.loads(raw)
            # Filter by camera if specified
            if req.cameras and event.get("camera_id") not in req.cameras:
                continue
            events.append(event)

    # Re-rank by semantic relevance using LLM scoring
    if events:
        ranking_prompt = f"""Query: "{req.query}"
        
Rate each event for relevance (0-1). Events:
{json.dumps([{"id": e.get("event_id"), "label": e.get("label"), 
              "timestamp": e.get("timestamp"), 
              "confidence": e.get("confidence")} for e in events[:50]], indent=2)}

Return JSON: {{"rankings": [{{"id": "...", "score": 0.0}}]}}"""
        
        resp = await llm_client.chat.completions.create(
            model="meta/llama-3.1-70b-instruct",
            messages=[{"role": "user", "content": ranking_prompt}],
            max_tokens=512, temperature=0.0
        )
        try:
            rankings = json.loads(resp.choices[0].message.content)
            score_map = {r["id"]: r["score"] for r in rankings.get("rankings", [])}
            for e in events:
                e["relevance_score"] = score_map.get(e.get("event_id"), 0.0)
            events.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
        except Exception:
            pass

    results = events[:req.max_results]
    # Publish search results to mission grid
    producer.send("vss_summaries", {
        "type": "SEARCH_RESULT",
        "query": req.query,
        "result_count": len(results),
        "timestamp": datetime.utcnow().isoformat()
    })
    return {"query": req.query, "results": results, "total_found": len(events)}

@app.post("/summarize")
async def summarize_events(req: SummarizeRequest):
    """Generate mission intelligence brief from event IDs."""
    redis_client = aioredis.from_url(REDIS_URL)
    events = []
    for eid in req.event_ids:
        raw = await redis_client.get(f"event:{eid}")
        if raw:
            events.append(json.loads(raw))

    if not events:
        raise HTTPException(status_code=404, detail="No events found")

    brief = await summarize_with_llm(events, req.mission_context)
    result = {
        "brief": brief,
        "event_count": len(events),
        "mission_context": req.mission_context,
        "generated_at": datetime.utcnow().isoformat()
    }
    producer.send("vss_summaries", {"type": "MISSION_BRIEF", **result})
    return result

@app.post("/query")
async def natural_language_query(req: QueryRequest):
    """Answer natural language questions about video feeds."""
    # Get recent events for context
    search_result = await search_video(SearchRequest(
        query=req.question,
        cameras=[req.camera_id] if req.camera_id else None,
        time_range_minutes=req.time_range_minutes,
        max_results=20
    ))
    answer = await summarize_with_llm(
        search_result["results"],
        f"Answer this question: {req.question}"
    )
    return {
        "question": req.question,
        "answer": answer,
        "supporting_events": len(search_result["results"])
    }

@app.post("/upload")
async def upload_video(file: UploadFile = File(...)):
    """Accept uploaded video for processing."""
    upload_path = f"/uploads/{file.filename}"
    with open(upload_path, "wb") as f:
        content = await file.read()
        f.write(content)
    return {"filename": file.filename, "path": upload_path, "status": "uploaded"}

@app.get("/health")
def health():
    return {"status": "ok", "service": "vss-agent"}