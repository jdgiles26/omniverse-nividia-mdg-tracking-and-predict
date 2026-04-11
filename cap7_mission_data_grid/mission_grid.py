"""
CAP 7: Mission Data Grid — Master Orchestrator
Consumes fused events → Correlate → Prioritize → Predict → Alert
WebSocket broadcast to dashboard
REST API for operator queries
"""
import os, json, uuid, asyncio, logging
from datetime import datetime, timezone
from typing import Optional
import redis.asyncio as aioredis
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from kafka import KafkaConsumer, KafkaProducer
import threading
import asyncpg

log = logging.getLogger("MissionGrid")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Mission Data Grid", version="3.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
COSMOS_URL = os.getenv("COSMOS_API_URL", "http://cosmos-predictor:8001")
VSS_URL = os.getenv("VSS_API_URL", "http://vss-agent:8000")
CORRELATION_WINDOW = int(os.getenv("CORRELATION_WINDOW_SEC", "60"))
THREAT_THRESHOLD = float(os.getenv("THREAT_THRESHOLD", "0.75"))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode()
)

# ── WebSocket Manager ──────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
        log.info(f"Dashboard connected. Total: {len(self.active)}")

    def disconnect(self, ws: WebSocket):
        self.active.remove(ws)

    async def broadcast(self, message: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.active.remove(ws)

manager = ConnectionManager()

# ── Correlation Engine ─────────────────────────────────────────────────
class CorrelationEngine:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.window = CORRELATION_WINDOW

    async def correlate(self, event: dict) -> list[dict]:
        """Find related events across cameras within time window."""
        related = []
        pattern = "event:*"
        keys = await self.redis.keys(pattern)

        event_time_str = event.get("timestamp", datetime.utcnow().isoformat())
        try:
            from dateutil.parser import parse
            event_time = parse(event_time_str).timestamp()
        except Exception:
            event_time = datetime.utcnow().timestamp()

        event_label = event.get("modalities", {}).get("video", {}).get("label", "")
        event_cam = event.get("modalities", {}).get("video", {}).get("camera_id", -1)

        for key in keys[-500:]:  # check last 500 events
            raw = await self.redis.get(key)
            if not raw:
                continue
            try:
                stored = json.loads(raw)
                stored_id = stored.get("event_id", "")
                if stored_id == event.get("event_id"):
                    continue  # skip self

                stored_time_str = stored.get("timestamp", "")
                try:
                    stored_time = parse(stored_time_str).timestamp()
                except Exception:
                    continue

                time_diff = abs(event_time - stored_time)
                stored_label = stored.get("modalities", {}).get(
                    "video", {}).get("label", "")
                stored_cam = stored.get("modalities", {}).get(
                    "video", {}).get("camera_id", -2)

                # Correlate if: same label within window, OR different camera same time
                if time_diff <= self.window and (
                    stored_label == event_label or
                    (stored_cam != event_cam and time_diff < 10)
                ):
                    stored["correlation_score"] = max(
                        0.0, 1.0 - time_diff / self.window
                    )
                    related.append(stored)
            except Exception:
                continue

        related.sort(key=lambda x: x.get("correlation_score", 0), reverse=True)
        return related[:10]

# ── Prioritization Engine ─────────────────────────────────────────────
MISSION_RULES = [
    {
        "name": "weapon_detection",
        "weight": 100,
        "priority": "CRITICAL",
        "condition": lambda e: e.get("modalities", {}).get("video", {}).get(
            "label", "").lower() in ["knife", "gun", "weapon", "rifle", "pistol"]
    },
    {
        "name": "high_confidence_person_threat",
        "weight": 80,
        "priority": "CRITICAL",
        "condition": lambda e: (
            e.get("modalities", {}).get("video", {}).get("label", "") == "person" and
            e.get("fused_confidence", 0) > 0.9 and
            "CLOSE_PROXIMITY" in e.get("threat_indicators", [])
        )
    },
    {
        "name": "vehicle_intrusion",
        "weight": 70,
        "priority": "HIGH",
        "condition": lambda e: (
            e.get("modalities", {}).get("video", {}).get("label", "") in
            ["car", "truck", "motorcycle"] and
            any("ZONE" in i for i in e.get("threat_indicators", []))
        )
    },
    {
        "name": "loud_noise_activity",
        "weight": 60,
        "priority": "HIGH",
        "condition": lambda e: "LOUD_NOISE" in e.get("threat_indicators", [])
    },
    {
        "name": "door_open_person",
        "weight": 50,
        "priority": "MEDIUM",
        "condition": lambda e: (
            "DOOR_OPEN" in e.get("threat_indicators", []) and
            e.get("modalities", {}).get("video", {}).get("label", "") == "person"
        )
    },
    {
        "name": "high_confidence_any",
        "weight": 30,
        "priority": "MEDIUM",
        "condition": lambda e: e.get("fused_confidence", 0) > 0.8
    },
]

def prioritize_event(event: dict) -> str:
    for rule in sorted(MISSION_RULES, key=lambda r: r["weight"], reverse=True):
        try:
            if rule["condition"](event):
                log.info(f"Rule '{rule['name']}' → {rule['priority']}")
                return rule["priority"]
        except Exception:
            continue
    return "LOW"

# ── Main Grid Processor ───────────────────────────────────────────────
class MissionGridProcessor:
    def __init__(self):
        self.redis = None
        self.correlation_engine = None
        self._loop = None

    async def init(self):
        self.redis = aioredis.from_url(REDIS_URL)
        self.correlation_engine = CorrelationEngine(self.redis)
        log.info("Mission Grid initialized")

    async def process_event(self, raw_event: dict):
        """Full pipeline: correlate → prioritize → predict → alert."""
        # Step 1: Prioritize
        priority = prioritize_event(raw_event)
        raw_event["grid_priority"] = priority
        raw_event["grid_timestamp"] = datetime.utcnow().isoformat()

        # Step 2: Correlate
        related_events = await self.correlation_engine.correlate(raw_event)
        raw_event["correlated_events"] = len(related_events)
        raw_event["correlation_chain"] = [
            e.get("event_id") for e in related_events[:5]
        ]

        # Step 3: Predict (async call to Cosmos service)
        prediction = {}
        if priority in ("HIGH", "CRITICAL"):
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    pred_resp = await client.post(
                        f"{COSMOS_URL}/assess",
                        json={
                            "events": [raw_event] + related_events[:3],
                            "scene_context": f"priority={priority}, "
                                             f"correlated={len(related_events)}"
                        }
                    )
                    prediction = pred_resp.json()
            except Exception as e:
                log.warning(f"Cosmos prediction unavailable: {e}")
                prediction = {"threat_level": priority, "error": str(e)}

        raw_event["prediction"] = prediction

        # Step 4: Store in Redis
        await self.redis.setex(
            f"grid_event:{raw_event.get('event_id', str(uuid.uuid4()))}",
            7200,  # 2 hour TTL
            json.dumps(raw_event)
        )

        # Step 5: Alert and broadcast
        mission_event = {
            "type": "MISSION_EVENT",
            "priority": priority,
            "event": raw_event,
            "related_count": len(related_events),
            "prediction": prediction,
            "timestamp": raw_event["grid_timestamp"]
        }

        # Publish alert to Kafka
        if priority in ("CRITICAL", "HIGH"):
            producer.send("mission_alerts", {
                **mission_event,
                "alert_type": f"{priority}_DETECTION",
                "requires_human_review": True
            })
            log.warning(f"[{priority}] ALERT: {raw_event.get('modalities', {}).get('video', {}).get('label', 'unknown')} | "
                        f"confidence={raw_event.get('fused_confidence', 0):.2f} | "
                        f"correlated={len(related_events)}")

        # Broadcast to dashboard via WebSocket (thread-safe)
        asyncio.run_coroutine_threadsafe(
            manager.broadcast(mission_event),
            self._loop
        )
        return mission_event

grid_processor = MissionGridProcessor()

# ── Kafka Consumer Thread ─────────────────────────────────────────────
def start_kafka_consumer():
    consumer = KafkaConsumer(
        os.getenv("KAFKA_TOPIC_FUSED", "fused_events"),
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode()),
        group_id="mission-grid",
        auto_offset_reset="latest"
    )
    log.info("Mission Grid consuming from Kafka fused_events...")
    loop = grid_processor._loop
    for message in consumer:
        try:
            asyncio.run_coroutine_threadsafe(
                grid_processor.process_event(message.value),
                loop
            )
        except Exception as e:
            log.error(f"Grid processing error: {e}")

# ── FastAPI Lifecycle ─────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    grid_processor._loop = asyncio.get_event_loop()
    await grid_processor.init()
    thread = threading.Thread(target=start_kafka_consumer, daemon=True)
    thread.start()
    log.info("Mission Data Grid is LIVE")

# ── WebSocket ────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            # Handle operator commands from dashboard
            try:
                cmd = json.loads(data)
                if cmd.get("type") == "QUERY":
                    async with httpx.AsyncClient(timeout=10.0) as client:
                        resp = await client.post(
                            f"{VSS_URL}/query",
                            json={"question": cmd.get("question", "")}
                        )
                        await websocket.send_json({
                            "type": "QUERY_RESPONSE",
                            "result": resp.json()
                        })
            except Exception as e:
                await websocket.send_json({"type": "ERROR", "message": str(e)})
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# ── REST API ──────────────────────────────────────────────────────────
@app.get("/alerts/recent")
async def recent_alerts(limit: int = 50):
    redis_client = aioredis.from_url(REDIS_URL)
    keys = await redis_client.keys("grid_event:*")
    events = []
    for key in keys[-limit:]:
        raw = await redis_client.get(key)
        if raw:
            e = json.loads(raw)
            if e.get("grid_priority") in ("CRITICAL", "HIGH"):
                events.append(e)
    events.sort(key=lambda x: x.get("grid_timestamp", ""), reverse=True)
    return {"alerts": events[:limit]}

@app.get("/stats")
async def grid_stats():
    redis_client = aioredis.from_url(REDIS_URL)
    total = len(await redis_client.keys("grid_event:*"))
    raw_total = len(await redis_client.keys("event:*"))
    return {
        "grid_events": total,
        "raw_events": raw_total,
        "active_dashboard_connections": len(manager.active),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health")
def health():
    return {"status": "ok", "service": "mission-grid"}