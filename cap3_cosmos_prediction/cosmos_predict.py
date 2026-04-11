"""
CAP 3: Cosmos World Foundation Model — Prediction Engine
Provides 30-second future state prediction using NVIDIA NIM
REST API: POST /predict, POST /assess, GET /health
"""
import os, json, base64, logging
from datetime import datetime
from typing import Optional
import numpy as np
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import AsyncOpenAI
from kafka import KafkaProducer
import redis.asyncio as aioredis

log = logging.getLogger("CosmosPrediction")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Cosmos Prediction Engine", version="2.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

NVIDIA_API_KEY = os.getenv("NVIDIA_API_KEY", "")
NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
THREAT_THRESHOLD = float(os.getenv("THREAT_THRESHOLD", "0.75"))

client = AsyncOpenAI(api_key=NVIDIA_API_KEY, base_url=NVIDIA_BASE_URL)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode()
)

# ── Models ─────────────────────────────────────────────────────────────
class TrajectoryPoint(BaseModel):
    timestamp: str
    x: float
    y: float
    label: str
    camera_id: int
    confidence: float

class PredictRequest(BaseModel):
    global_id: int
    trajectory: list[TrajectoryPoint]
    horizon_seconds: Optional[int] = 30
    mission_context: Optional[str] = "general_surveillance"
    restricted_zones: Optional[list[dict]] = []

class ThreatAssessRequest(BaseModel):
    events: list[dict]
    scene_context: Optional[str] = ""

# ── Trajectory Predictor ───────────────────────────────────────────────
def compute_velocity(trajectory: list[TrajectoryPoint]) -> tuple[float, float]:
    """Compute average velocity from trajectory."""
    if len(trajectory) < 2:
        return 0.0, 0.0
    recent = trajectory[-min(10, len(trajectory)):]
    dx = recent[-1].x - recent[0].x
    dy = recent[-1].y - recent[0].y
    dt = len(recent)  # frames as time proxy
    return dx / dt if dt else 0.0, dy / dt if dt else 0.0

def predict_trajectory_physics(
    trajectory: list[TrajectoryPoint],
    horizon_seconds: int,
    fps: float = 30.0
) -> list[dict]:
    """Physics-based trajectory prediction (fallback/augment)."""
    if not trajectory:
        return []
    vx, vy = compute_velocity(trajectory)
    last = trajectory[-1]
    steps = int(horizon_seconds * fps / 10)  # predict every 10 frames
    predicted = []
    for i in range(1, steps + 1):
        dt = i * 10 / fps
        predicted.append({
            "step": i,
            "time_offset_sec": round(dt, 2),
            "predicted_x": round(last.x + vx * i * 10, 2),
            "predicted_y": round(last.y + vy * i * 10, 2),
            "confidence": max(0.1, 1.0 - (i / steps) * 0.7)  # degrades over time
        })
    return predicted

async def predict_with_cosmos_nim(
    trajectory: list[TrajectoryPoint],
    horizon_seconds: int,
    mission_context: str,
    restricted_zones: list[dict]
) -> dict:
    """Use Cosmos Reason VLM via NIM for intelligent prediction."""
    traj_data = [
        {"t": p.timestamp, "x": p.x, "y": p.y,
         "label": p.label, "cam": p.camera_id, "conf": p.confidence}
        for p in trajectory[-20:]  # last 20 points
    ]
    zones_str = json.dumps(restricted_zones) if restricted_zones else "none defined"

    prompt = f"""You are a mission intelligence prediction system.

Object trajectory (last {len(traj_data)} observations):
{json.dumps(traj_data, indent=2)}

Mission context: {mission_context}
Restricted/monitored zones: {zones_str}
Prediction horizon: {horizon_seconds} seconds

Analyze the trajectory and predict:
1. Most likely future position and path
2. Probability of entering restricted zones (0.0-1.0)
3. Predicted behavior/intent
4. Threat level: LOW/MEDIUM/HIGH/CRITICAL
5. Recommended mission action
6. Confidence in prediction (0.0-1.0)

Respond as valid JSON with keys:
predicted_path, zone_entry_probability, behavior, threat_level, 
recommended_action, prediction_confidence, reasoning"""

    try:
        response = await client.chat.completions.create(
            model="nvidia/cosmos-reason1-8b",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1024,
            temperature=0.1
        )
        content = response.choices[0].message.content
        # Strip markdown code blocks if present
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0]
        elif "```" in content:
            content = content.split("```")[1].split("```")[0]
        return json.loads(content.strip())
    except Exception as e:
        log.error(f"Cosmos NIM prediction error: {e}")
        return {
            "threat_level": "UNKNOWN",
            "prediction_confidence": 0.3,
            "recommended_action": "MONITOR",
            "error": str(e)
        }

# ── Endpoints ──────────────────────────────────────────────────────────
@app.post("/predict")
async def predict_object_future(req: PredictRequest):
    """Predict future state of a tracked object."""
    physics_prediction = predict_trajectory_physics(
        req.trajectory, req.horizon_seconds
    )
    cosmos_prediction = await predict_with_cosmos_nim(
        req.trajectory, req.horizon_seconds,
        req.mission_context, req.restricted_zones
    )
    result = {
        "global_id": req.global_id,
        "timestamp": datetime.utcnow().isoformat(),
        "horizon_seconds": req.horizon_seconds,
        "physics_trajectory": physics_prediction,
        "cosmos_assessment": cosmos_prediction,
        "threat_level": cosmos_prediction.get("threat_level", "LOW"),
        "prediction_confidence": cosmos_prediction.get("prediction_confidence", 0.5),
        "zone_entry_probability": cosmos_prediction.get("zone_entry_probability", 0.0),
        "recommended_action": cosmos_prediction.get("recommended_action", "MONITOR")
    }

    # Publish to Kafka
    producer.send("cosmos_predictions", result)

    # Alert if threat threshold exceeded
    zone_prob = result["zone_entry_probability"]
    if isinstance(zone_prob, (int, float)) and zone_prob > THREAT_THRESHOLD:
        producer.send("mission_alerts", {
            "alert_type": "PREDICTED_ZONE_ENTRY",
            "global_id": req.global_id,
            "probability": zone_prob,
            "priority": "CRITICAL" if zone_prob > 0.9 else "HIGH",
            "prediction": result,
            "timestamp": result["timestamp"]
        })
        log.warning(f"[ALERT] Object {req.global_id} predicted zone entry: {zone_prob:.2%}")

    return result

@app.post("/assess")
async def assess_scene_threat(req: ThreatAssessRequest):
    """Assess overall scene threat from multiple events."""
    prompt = f"""Mission threat assessment for {len(req.events)} events.

Scene context: {req.scene_context}
Events summary: {json.dumps(req.events[:10], indent=2)}

Provide:
1. Overall scene threat level: LOW/MEDIUM/HIGH/CRITICAL
2. Key threat indicators
3. Correlated patterns
4. Immediate recommended actions
5. Predicted escalation risk (0.0-1.0)

Respond as JSON."""

    response = await client.chat.completions.create(
        model="meta/llama-3.1-70b-instruct",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1024, temperature=0.1
    )
    assessment = response.choices[0].message.content
    try:
        result = json.loads(assessment)
    except Exception:
        result = {"raw_assessment": assessment}

    result["event_count"] = len(req.events)
    result["assessed_at"] = datetime.utcnow().isoformat()
    producer.send("cosmos_predictions", {"type": "SCENE_ASSESSMENT", **result})
    return result

@app.get("/health")
def health():
    return {"status": "ok", "service": "cosmos-predictor"}