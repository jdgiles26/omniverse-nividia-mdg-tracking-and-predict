"""
CAP 6: Multimodal Fusion Engine
Fuses: video detections + audio + telemetry + LiDAR
Publishes: enriched fused mission events to Kafka
"""
import os, json, time, uuid, asyncio, logging, threading
from datetime import datetime
from collections import defaultdict
import numpy as np
import sounddevice as sd
import soundfile as sf
from kafka import KafkaConsumer, KafkaProducer
import redis

log = logging.getLogger("FusionEngine")
logging.basicConfig(level=logging.INFO)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
FUSION_WINDOW_MS = 500  # Fuse events within 500ms

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode()
)
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# ── Audio Monitor ─────────────────────────────────────────────────────
class AudioMonitor:
    def __init__(self, sample_rate=16000, channels=1):
        self.sample_rate = sample_rate
        self.channels = channels
        self.audio_buffer = []
        self.is_running = False

    def detect_audio_events(self, audio_chunk: np.ndarray) -> dict:
        rms = float(np.sqrt(np.mean(audio_chunk**2)))
        peak = float(np.max(np.abs(audio_chunk)))
        event = {
            "type": "audio",
            "rms_level": round(rms, 6),
            "peak_level": round(peak, 6),
            "timestamp": datetime.utcnow().isoformat(),
        }
        if rms > 0.1:
            event["alert"] = "LOUD_NOISE"
            event["severity"] = "HIGH" if rms > 0.3 else "MEDIUM"
        elif rms > 0.01:
            event["alert"] = "ACTIVITY_DETECTED"
            event["severity"] = "LOW"
        else:
            event["alert"] = "SILENCE"
            event["severity"] = "NONE"
        return event

    def start(self, callback):
        def audio_callback(indata, frames, time_info, status):
            event = self.detect_audio_events(indata[:, 0])
            callback(event)

        try:
            self.stream = sd.InputStream(
                samplerate=self.sample_rate,
                channels=self.channels,
                callback=audio_callback,
                blocksize=4096
            )
            self.stream.start()
            self.is_running = True
            log.info("Audio monitor started")
        except Exception as e:
            log.warning(f"Audio monitor unavailable (headless): {e}")
            self.is_running = False

# ── Telemetry Simulator (replace with real sensors) ───────────────────
class TelemetrySensor:
    """Simulate environmental/telemetry data. Replace with real sensor SDK."""
    def read(self) -> dict:
        return {
            "type": "telemetry",
            "timestamp": datetime.utcnow().isoformat(),
            "temperature_c": round(20.0 + np.random.normal(0, 0.5), 2),
            "humidity_pct": round(45.0 + np.random.normal(0, 2), 2),
            "motion_detected": bool(np.random.random() > 0.8),
            "vibration_level": round(abs(np.random.normal(0, 0.1)), 4),
            "door_status": "OPEN" if np.random.random() > 0.9 else "CLOSED"
        }

# ── LiDAR Stub (replace with real LiDAR SDK) ──────────────────────────
class LiDARSensor:
    """Stub for LiDAR point cloud. Replace with OUSTER/Velodyne SDK."""
    def read(self) -> dict:
        n_points = int(np.random.randint(100, 500))
        return {
            "type": "lidar",
            "timestamp": datetime.utcnow().isoformat(),
            "point_count": n_points,
            "nearest_object_m": round(abs(np.random.normal(3.0, 1.0)), 2),
            "occupied_zones": ["zone_a"] if np.random.random() > 0.7 else [],
            "movement_detected": bool(np.random.random() > 0.75)
        }

# ── Fusion Logic ──────────────────────────────────────────────────────
class FusionEngine:
    def __init__(self):
        self.pending: dict[str, list] = defaultdict(list)
        self.audio_monitor = AudioMonitor()
        self.telemetry = TelemetrySensor()
        self.lidar = LiDARSensor()
        self.current_audio_event = None
        self.audio_monitor.start(self._on_audio)

    def _on_audio(self, event):
        self.current_audio_event = event
        # Also publish raw audio events
        producer.send("raw_audio", event)

    def fuse(self, video_event: dict) -> dict:
        """Fuse a video detection event with all available modalities."""
        audio = self.current_audio_event or {"type": "audio", "alert": "NO_DATA"}
        telemetry = self.telemetry.read()
        lidar = self.lidar.read()

        # Compute fused confidence (weighted average)
        video_conf = video_event.get("confidence", 0.5)
        lidar_boost = 0.15 if lidar.get("movement_detected") else 0.0
        audio_boost = 0.1 if audio.get("alert") == "LOUD_NOISE" else 0.0
        fused_confidence = min(1.0, video_conf + lidar_boost + audio_boost)

        # Compute fused priority
        priority = "LOW"
        if fused_confidence > 0.85 or audio.get("severity") == "HIGH":
            priority = "CRITICAL"
        elif fused_confidence > 0.70 or lidar.get("movement_detected"):
            priority = "HIGH"
        elif fused_confidence > 0.50:
            priority = "MEDIUM"

        # Additional threat indicators
        threat_indicators = []
        if telemetry.get("door_status") == "OPEN":
            threat_indicators.append("DOOR_OPEN")
        if lidar.get("nearest_object_m", 10) < 1.5:
            threat_indicators.append("CLOSE_PROXIMITY")
        if audio.get("alert") == "LOUD_NOISE":
            threat_indicators.append("LOUD_NOISE")
        if lidar.get("occupied_zones"):
            threat_indicators.extend([f"ZONE:{z}" for z in lidar["occupied_zones"]])

        fused_event = {
            "event_id": str(uuid.uuid4()),
            "source": "multimodal_fusion",
            "timestamp": datetime.utcnow().isoformat(),
            "fused_confidence": round(fused_confidence, 4),
            "priority": priority,
            "threat_indicators": threat_indicators,
            "modalities": {
                "video": video_event,
                "audio": audio,
                "telemetry": telemetry,
                "lidar": lidar
            }
        }

        # Store in Redis for correlation engine
        redis_client.setex(
            f"event:{fused_event['event_id']}",
            3600,  # 1 hour TTL
            json.dumps(fused_event)
        )
        return fused_event

    def run_consumer(self):
        """Consume video detection events from Kafka and fuse them."""
        consumer = KafkaConsumer(
            os.getenv("KAFKA_TOPIC_TRACKING", "tracking_events"),
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda v: json.loads(v.decode()),
            group_id="fusion-engine",
            auto_offset_reset="latest"
        )
        log.info("Fusion engine consuming from Kafka...")
        for message in consumer:
            try:
                video_event = message.value
                fused = self.fuse(video_event)
                producer.send(
                    os.getenv("KAFKA_TOPIC_FUSED", "fused_events"),
                    fused
                )
                if fused["priority"] in ("CRITICAL", "HIGH"):
                    log.warning(f"[{fused['priority']}] Fused event: "
                                f"{fused['modalities']['video'].get('label')} | "
                                f"conf={fused['fused_confidence']:.2f} | "
                                f"indicators={fused['threat_indicators']}")
            except Exception as e:
                log.error(f"Fusion error: {e}")

if __name__ == "__main__":
    engine = FusionEngine()
    engine.run_consumer()