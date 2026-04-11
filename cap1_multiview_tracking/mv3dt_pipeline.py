"""
CAP 1: Multi-View 3D Tracking Pipeline (DeepStream 8.0)
Supports: webcam, RTSP, HTTP/HTTPS, local file, uploaded video
Publishes: tracking events to Kafka mission grid
"""
import os, sys, json, time, uuid, logging
from datetime import datetime
import gi
gi.require_version('Gst', '1.0')
gi.require_version('GstRtspServer', '1.0')
from gi.repository import Gst, GLib
import pyds
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("MV3DT")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
VIDEO_SOURCES_RAW = os.getenv("VIDEO_SOURCES", "webcam")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TRACKING", "tracking_events")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode(),
    retries=5
)

# ── Source URI normalizer ─────────────────────────────────────────────
def normalize_uri(src: str) -> tuple[str, int]:
    """Returns (gst_uri, source_type) where type: 1=camera, 3=uri, 4=rtsp"""
    src = src.strip()
    if src == "webcam" or src.startswith("/dev/video"):
        dev = 0 if src == "webcam" else int(src.replace("/dev/video", ""))
        return f"v4l2src device=/dev/video{dev}", 1
    elif src.startswith("rtsp://"):
        return src, 4
    elif src.startswith("http://") or src.startswith("https://"):
        return src, 3
    elif src.startswith("/uploads/") or os.path.exists(src):
        return f"file://{os.path.abspath(src)}", 3
    else:
        raise ValueError(f"Unknown source: {src}")

# ── DeepStream Pipeline Builder ───────────────────────────────────────
def build_pipeline(sources: list[str]) -> Gst.Pipeline:
    Gst.init(None)
    pipeline = Gst.Pipeline()
    streammux = Gst.ElementFactory.make("nvstreammux", "streammux")
    streammux.set_property("width", 1920)
    streammux.set_property("height", 1080)
    streammux.set_property("batch-size", len(sources))
    streammux.set_property("batched-push-timeout", 4000000)
    pipeline.add(streammux)

    for i, src in enumerate(sources):
        uri, src_type = normalize_uri(src)
        log.info(f"[Source {i}] {src} → {uri} (type {src_type})")

        if src_type == 1:  # webcam
            source = Gst.ElementFactory.make("v4l2src", f"source{i}")
            source.set_property("device", "/dev/video0")
            caps = Gst.ElementFactory.make("capsfilter", f"caps{i}")
            caps.set_property("caps", Gst.Caps.from_string(
                "video/x-raw,width=1280,height=720,framerate=30/1"))
            nvvidconv = Gst.ElementFactory.make("nvvideoconvert", f"conv{i}")
            pipeline.add(source); pipeline.add(caps); pipeline.add(nvvidconv)
            source.link(caps); caps.link(nvvidconv)
            src_pad = nvvidconv.get_static_pad("src")
        else:
            source = Gst.ElementFactory.make("uridecodebin", f"source{i}")
            source.set_property("uri", uri)
            pipeline.add(source)
            # Async pad linking for uridecodebin
            source.connect("pad-added", on_pad_added, streammux, i)
            continue  # skip manual linking below

        sink_pad = streammux.get_request_pad(f"sink_{i}")
        src_pad.link(sink_pad)

    # Primary inference (object detection)
    pgie = Gst.ElementFactory.make("nvinfer", "primary-inference")
    pgie.set_property("config-file-path", "/workspace/deepstream_config.txt")

    # Multi-camera 3D tracker (MV3DT)
    tracker = Gst.ElementFactory.make("nvtracker", "mv3dt-tracker")
    tracker.set_property("ll-lib-file", "/opt/nvidia/deepstream/deepstream/lib/libnvds_nvmultiobjecttracker.so")
    tracker.set_property("ll-config-file", "/workspace/tracker_config.yml")
    tracker.set_property("tracker-width", 640)
    tracker.set_property("tracker-height", 384)

    # Tiler for multi-view display
    tiler = Gst.ElementFactory.make("nvmultistreamtiler", "tiler")
    cols = max(1, (len(sources) + 1) // 2)
    tiler.set_property("rows", 2); tiler.set_property("columns", cols)
    tiler.set_property("width", 1920); tiler.set_property("height", 1080)

    nvvidconv = Gst.ElementFactory.make("nvvideoconvert", "vidconv-out")
    osd = Gst.ElementFactory.make("nvdsosd", "osd")
    sink = Gst.ElementFactory.make("nveglglessink", "sink")
    sink.set_property("sync", False)

    for el in [pgie, tracker, tiler, nvvidconv, osd, sink]:
        pipeline.add(el)

    streammux.link(pgie)
    pgie.link(tracker)
    tracker.link(tiler)
    tiler.link(nvvidconv)
    nvvidconv.link(osd)
    osd.link(sink)

    # Attach probe to extract metadata → Kafka
    tracker_src_pad = tracker.get_static_pad("src")
    tracker_src_pad.add_probe(Gst.PadProbeType.BUFFER, tracking_probe_callback, 0)

    return pipeline

def on_pad_added(src, new_pad, mux, cam_id):
    sink_pad = mux.get_request_pad(f"sink_{cam_id}")
    if not sink_pad.is_linked():
        new_pad.link(sink_pad)

# ── Tracking Probe — Extract Metadata → Kafka ─────────────────────────
def tracking_probe_callback(pad, info, u_data):
    gst_buffer = info.get_buffer()
    if not gst_buffer:
        return Gst.PadProbeReturn.OK

    batch_meta = pyds.gst_buffer_get_nvds_batch_meta(hash(gst_buffer))
    l_frame = batch_meta.frame_meta_list

    while l_frame is not None:
        try:
            frame_meta = pyds.NvDsFrameMeta.cast(l_frame.data)
            camera_id = frame_meta.source_id
            frame_num = frame_meta.frame_num
            timestamp = datetime.utcnow().isoformat()

            l_obj = frame_meta.obj_meta_list
            while l_obj is not None:
                try:
                    obj = pyds.NvDsObjectMeta.cast(l_obj.data)
                    event = {
                        "event_id": str(uuid.uuid4()),
                        "camera_id": camera_id,
                        "global_id": obj.object_id,
                        "frame_num": frame_num,
                        "timestamp": timestamp,
                        "label": obj.obj_label,
                        "confidence": round(float(obj.confidence), 4),
                        "bbox": {
                            "left": obj.rect_params.left,
                            "top": obj.rect_params.top,
                            "width": obj.rect_params.width,
                            "height": obj.rect_params.height
                        },
                        "tracker_confidence": round(float(obj.tracker_confidence), 4),
                        "source": "deepstream_mv3dt"
                    }
                    producer.send(KAFKA_TOPIC, event)
                except StopIteration:
                    break
                try:
                    l_obj = l_obj.next
                except StopIteration:
                    break
        except StopIteration:
            break
        try:
            l_frame = l_frame.next
        except StopIteration:
            break

    return Gst.PadProbeReturn.OK

# ── Main ──────────────────────────────────────────────────────────────
def main():
    sources = [s.strip() for s in VIDEO_SOURCES_RAW.split(",") if s.strip()]
    log.info(f"Starting MV3DT pipeline with {len(sources)} source(s): {sources}")

    pipeline = build_pipeline(sources)
    loop = GLib.MainLoop()
    bus = pipeline.get_bus()
    bus.add_signal_watch()

    def on_message(bus, message):
        if message.type == Gst.MessageType.EOS:
            log.info("End of stream")
            loop.quit()
        elif message.type == Gst.MessageType.ERROR:
            err, debug = message.parse_error()
            log.error(f"GST Error: {err} | {debug}")
            loop.quit()

    bus.connect("message", on_message)
    pipeline.set_state(Gst.State.PLAYING)
    log.info("Pipeline running. Press Ctrl+C to stop.")
    try:
        loop.run()
    except KeyboardInterrupt:
        pass
    finally:
        pipeline.set_state(Gst.State.NULL)
        producer.flush()
        log.info("Pipeline stopped.")

if __name__ == "__main__":
    main()