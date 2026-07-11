"""
slave.py — Slave node (fixed: all slaves now process frames in parallel).

Bugs fixed
----------
1. Race condition — enroll message missed by slave 2
   CAUSE:  control consumer used auto_offset_reset="latest", so if slave 2
           started AFTER the master published the enroll message, it never
           saw it. The message was already consumed and gone.
   FIX:    auto_offset_reset="earliest" + unique group_id per slave on the
           control topic. Each slave reads ALL control messages from the
           beginning so it always gets the enroll message regardless of
           startup timing. A local seen-set prevents re-processing.

2. Drop instead of buffer on missing state
   CAUSE:  When state was missing the slave waited only 4 s (20 × 0.2 s)
           then dropped the entire batch permanently. On a loaded system
           the enroll message can take longer than 4 s.
   FIX:    Buffer dropped batches in a per-job deque. After state arrives
           (via control consumer callback), reprocess buffered batches
           immediately. Buffer is capped at 500 batches to avoid OOM.

3. Partition assignment — only 1 slave gets frames
   CAUSE:  The load balancer returns f"slave:{slave_id}".encode() as the
           Kafka partition key. Kafka hashes this key to ONE partition.
           If both slaves are assigned the same partition (because the
           cluster has 4 partitions but both slaves ended up owning
           partition 0 via round-robin), one slave gets nothing.
   FIX:    Use the slave's ASSIGNED partition index as the explicit
           partition argument in producer.send(), not just a key. The
           load balancer now returns a (partition_int, key_bytes) tuple.
           The producer uses partition= directly, bypassing key hashing.
           This guarantees each frame batch lands on the exact partition
           the target slave owns.
"""

import os, sys, json, base64, time, logging, threading, traceback
from pathlib import Path
from collections import defaultdict, deque
import socket
import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
try:
    import psutil; _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False

sys.path.insert(0, str(Path(__file__).parent))
import config_distributed as cfg

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [SLAVE] %(levelname)s: %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("slave")

os.environ["ORT_LOGGING_LEVEL"]     = "3"
os.environ["INSIGHTFACE_LOG_LEVEL"] = "ERROR"

# Unique ID for this slave instance
SLAVE_ID = f"{socket.gethostname()}_{os.getpid()}"
logger.info(f"Slave ID: {SLAVE_ID}")

# ── Model loading (once at startup) ───────────────────────────────────────────
logger.info("Loading YOLO model…")
from ultralytics import YOLO
import torch

_device     = "0" if (cfg.USE_GPU and torch.cuda.is_available()) else "cpu"
_yolo_model = YOLO(cfg.YOLO_MODEL)
logger.info(f"YOLO ready on {'GPU' if _device=='0' else 'CPU'}.")

logger.info("Loading InsightFace buffalo_sc…")
from insightface.app import FaceAnalysis
_face_app = FaceAnalysis(name="buffalo_sc", providers=["CPUExecutionProvider"])
_face_app.prepare(ctx_id=-1, det_size=(320, 320))
logger.info("InsightFace ready.")


# ══════════════════════════════════════════════════════════════════════════════
#  PER-JOB STATE
# ══════════════════════════════════════════════════════════════════════════════

class JobState:
    def __init__(self, job_id: str, enrolled: list[dict], settings: dict):
        self.job_id    = job_id
        self.settings  = settings
        self.threshold = float(settings.get("face_threshold",
                                            cfg.FACE_DISTANCE_THRESHOLD))
        self.embeddings: list[np.ndarray] = []
        self.names:      list[str]        = []
        for e in enrolled:
            raw  = base64.b64decode(e["embedding_b64"])
            emb  = np.frombuffer(raw, dtype=np.float32).reshape(
                       tuple(e["embedding_shape"])).copy()
            emb /= (np.linalg.norm(emb) + 1e-9)
            self.embeddings.append(emb)
            self.names.append(e["name"])

        self._cache: dict[str, tuple] = {}
        self._frame_counter = 0

    def box_key(self, x1, y1, x2, y2) -> str:
        g = 25
        return f"{x1//g},{y1//g},{x2//g},{y2//g}"

    def prune_cache(self):
        cutoff = self._frame_counter - cfg.FACE_CHECK_EVERY_N_FRAMES * 4
        self._cache = {k: v for k, v in self._cache.items() if v[0] > cutoff}

    def identify(self, frame_bgr, x1, y1, x2, y2) -> list[dict]:
        bk      = self.box_key(x1, y1, x2, y2)
        frame_n = self._frame_counter
        h, w    = frame_bgr.shape[:2]
        x1c, y1c = max(0, x1), max(0, y1)
        x2c, y2c = min(w, x2), min(h, y2)

        if (x2c-x1c) < cfg.MIN_FACE_SIZE or (y2c-y1c) < cfg.MIN_FACE_SIZE:
            return [{"person_idx":i,"name":n,"distance":1.0,
                     "found":False,"uncertain":False}
                    for i,n in enumerate(self.names)]

        cached = self._cache.get(bk)
        if cached and (frame_n - cached[0]) < cfg.FACE_CHECK_EVERY_N_FRAMES:
            return cached[1]

        embs    = self._get_crop_embeddings(frame_bgr, x1c, y1c, x2c, y2c)
        results = []
        for pi, tgt in enumerate(self.embeddings):
            best = min((float(1.0 - np.dot(tgt, e)) for e in embs), default=1.0)
            found     = best < self.threshold
            uncertain = not found and best < self.threshold * 1.10
            results.append({"person_idx":pi,"name":self.names[pi],
                             "distance":round(best,4),"found":found,"uncertain":uncertain})
        self._cache[bk] = (frame_n, results)
        return results

    def _get_crop_embeddings(self, frame_bgr, x1, y1, x2, y2):
        embs = []
        bh = y2 - y1
        for cy1, cy2 in [(y1, y2),
                         (y1, y1 + max(cfg.MIN_FACE_SIZE, int(bh * 0.45)))]:
            cy2 = min(frame_bgr.shape[0], cy2)
            if cy2 <= cy1: continue
            crop = cv2.cvtColor(frame_bgr[cy1:cy2, x1:x2], cv2.COLOR_BGR2RGB)
            ch, cw = crop.shape[:2]
            if ch < cfg.MIN_FACE_SIZE or cw < cfg.MIN_FACE_SIZE: continue
            if ch < 80 or cw < 80:
                scale = max(80/cw, 80/ch)
                crop  = cv2.resize(crop, (int(cw*scale), int(ch*scale)),
                                   interpolation=cv2.INTER_LINEAR)
            for face in _face_app.get(crop):
                emb = face.normed_embedding.astype(np.float32)
                embs.append(emb / (np.linalg.norm(emb) + 1e-9))
        return embs


# ── Job state registry ─────────────────────────────────────────────────────────
_job_states:      dict[str, JobState]        = {}
_job_states_lock: threading.Lock             = threading.Lock()

# FIX 2: per-job batch buffer — stores batches that arrived before state was ready
_batch_buffer:    dict[str, deque]           = defaultdict(lambda: deque(maxlen=500))
_buffer_lock:     threading.Lock             = threading.Lock()


def register_state(job_id: str, enrolled: list[dict], settings: dict) -> JobState:
    """Create JobState and immediately drain any buffered batches for this job."""
    with _job_states_lock:
        if job_id not in _job_states:
            _job_states[job_id] = JobState(job_id, enrolled, settings)
        state = _job_states[job_id]

    # Drain buffered batches that arrived before enrollment
    with _buffer_lock:
        buffered = list(_batch_buffer.pop(job_id, deque()))

    if buffered:
        logger.info(f"[{job_id}] Draining {len(buffered)} buffered batches.")
        for buf_data in buffered:
            _process_batch(buf_data, state)

    return state


def get_state(job_id: str) -> JobState | None:
    with _job_states_lock:
        return _job_states.get(job_id)


# ══════════════════════════════════════════════════════════════════════════════
#  FRAME PROCESSING
# ══════════════════════════════════════════════════════════════════════════════

def process_frame(frame_bgr, frame_idx, timestamp, state: JobState) -> dict:
    state._frame_counter = frame_idx
    if frame_idx % 100 == 0:
        state.prune_cache()

    yolo_res = _yolo_model.predict(
        frame_bgr,
        classes=[cfg.PERSON_CLASS_ID],
        conf=float(state.settings.get("yolo_conf", cfg.YOLO_CONF)),
        iou=cfg.YOLO_IOU, imgsz=cfg.YOLO_IMGSZ,
        device=_device, verbose=False,
    )

    detections   = []
    any_target   = False
    person_dets  = [{} for _ in state.embeddings]
    annotated    = frame_bgr.copy()

    for r in yolo_res:
        if r.boxes is None: continue
        for box in r.boxes:
            x1, y1, x2, y2 = map(int, box.xyxy[0])
            conf            = float(box.conf[0])
            matches = state.identify(frame_bgr, x1, y1, x2, y2)
            is_tgt  = any(m["found"]     for m in matches)
            is_unc  = any(m["uncertain"] for m in matches)
            best_d  = min((m["distance"] for m in matches), default=1.0)

            if is_tgt:
                any_target = True
                for m in matches:
                    if m["found"]:
                        person_dets[m["person_idx"]] = \
                            {"found": True, "distance": m["distance"]}

            detections.append({"coords":[x1,y1,x2,y2],"conf":round(conf,3),
                                "is_target":is_tgt,"is_uncertain":is_unc,
                                "face_dist":round(best_d,4)})

            color = cfg.COLOR_TARGET if is_tgt else \
                    cfg.COLOR_UNCERTAIN if is_unc else cfg.COLOR_PERSON
            label = f"{'TARGET' if is_tgt else 'POSSIBLE' if is_unc else 'Person'} {conf:.0%}"
            cv2.rectangle(annotated,(x1,y1),(x2,y2),color,cfg.BOX_THICKNESS)
            (lw,lh),_ = cv2.getTextSize(label,cv2.FONT_HERSHEY_SIMPLEX,0.50,1)
            cv2.rectangle(annotated,(x1,y1-lh-6),(x1+lw+4,y1),color,-1)
            cv2.putText(annotated,label,(x1+2,y1-4),
                        cv2.FONT_HERSHEY_SIMPLEX,0.50,(255,255,255),1,cv2.LINE_AA)

    cnt = len(detections)
    for line, yoff in [(f"Crowd: {cnt}",30),
                       ("TARGET DETECTED" if any_target else "",66)]:
        if not line: continue
        col = cfg.COLOR_TARGET if "TARGET" in line else (255,255,255)
        cv2.putText(annotated,line,(10,yoff),
                    cv2.FONT_HERSHEY_SIMPLEX,0.9,(0,0,0),4,cv2.LINE_AA)
        cv2.putText(annotated,line,(10,yoff),
                    cv2.FONT_HERSHEY_SIMPLEX,0.9,col,2,cv2.LINE_AA)

    if any_target and cfg.FLASH_ON_TARGET:
        h,w = annotated.shape[:2]
        cv2.rectangle(annotated,(0,0),(w-1,h-1),cfg.COLOR_TARGET,6)

    _,buf    = cv2.imencode(".jpg", annotated, [cv2.IMWRITE_JPEG_QUALITY,88])
    ann_b64  = base64.b64encode(buf.tobytes()).decode()

    return {
        "frame_idx":frame_idx,"timestamp":timestamp,
        "person_count":cnt,"any_target":any_target,
        "detections":detections,"person_detections":person_dets,
        "annotated_b64":ann_b64,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  BATCH PROCESSOR  (shared by main loop + buffer drain)
# ══════════════════════════════════════════════════════════════════════════════

# Shared producer and progress tracker (set in main())
_producer:      KafkaProducer | None = None
_job_progress:  dict[str, dict]      = defaultdict(lambda: {"done":0,"last_report":0})
_frames_processed = 0
_queue_depth      = 0
_fps_counter      = {"count":0,"last_time":time.time(),"fps":0.0}
_frame_times_ms   = []          # rolling list of per-frame processing times (ms)
_MAX_FRAME_TIMES  = 100         # keep last 100 samples for avg / history


def _update_fps():
    _fps_counter["count"] += 1
    now     = time.time()
    elapsed = now - _fps_counter["last_time"]
    if elapsed >= 3.0:
        _fps_counter["fps"]       = _fps_counter["count"] / elapsed
        _fps_counter["count"]     = 0
        _fps_counter["last_time"] = now


def _process_batch(data: dict, state: JobState):
    """Process one frame batch and send results to TOPIC_RESULTS."""
    global _frames_processed, _queue_depth

    job_id         = data["job_id"]
    frames_in_batch = data.get("frames", [])
    frame_results   = []

    for fdata in frames_in_batch:
        try:
            raw   = base64.b64decode(fdata["frame_b64"])
            frame = cv2.imdecode(np.frombuffer(raw, np.uint8), cv2.IMREAD_COLOR)
            if frame is None: continue
            _t_start = time.time()
            result   = process_frame(frame, fdata["frame_idx"], fdata["timestamp"], state)
            _frame_ms = round((time.time() - _t_start) * 1000, 1)
            _frame_times_ms.append(_frame_ms)
            if len(_frame_times_ms) > _MAX_FRAME_TIMES:
                _frame_times_ms.pop(0)
            frame_results.append(result)
            _job_progress[job_id]["done"] += 1
            _update_fps()
        except Exception as e:
            logger.error(f"[{job_id}] Frame {fdata.get('frame_idx')} error: {e}")

    if frame_results and _producer:
        _frames_processed += len(frame_results)
        _queue_depth = max(0, _queue_depth - len(frames_in_batch))
        _producer.send(cfg.TOPIC_RESULTS, value={
            "job_id":        job_id,
            "video_idx":     data.get("video_idx", 0),
            "slave_id":      SLAVE_ID,
            "frame_results": frame_results,
        }, key=job_id.encode())

    done = _job_progress[job_id]["done"]
    if _producer and done - _job_progress[job_id]["last_report"] >= 30:
        _job_progress[job_id]["last_report"] = done
        _producer.send(cfg.TOPIC_PROGRESS, value={
            "job_id":      job_id,
            "video_idx":   data.get("video_idx", 0),
            "slave_id":    SLAVE_ID,
            "message":     f"Slave {SLAVE_ID} processed {done} frames…",
            "done_frames": done,
            "progress":    min(80, int(done / max(done+10,1) * 80)),
        })
        logger.info(f"[{job_id}] Processed {done} frames.")


# ══════════════════════════════════════════════════════════════════════════════
#  CONTROL CONSUMER
#  FIX 1: earliest offset + slave-unique group_id so every slave reads ALL
#          historical enroll messages regardless of when it started.
# ══════════════════════════════════════════════════════════════════════════════

def control_consumer_thread():
    # Unique group_id per slave instance → each slave gets its own offset pointer
    # so it reads from offset 0 every time and never misses an enroll message.
    consumer = KafkaConsumer(
        cfg.TOPIC_CONTROL,
        bootstrap_servers=[cfg.KAFKA_BROKER],
            api_version=cfg.KAFKA_API_VERSION,
        group_id=f"slave_ctrl_{SLAVE_ID}",   # FIX: unique per slave
        auto_offset_reset="earliest",         # FIX: read from beginning
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode()),
        consumer_timeout_ms=60_000,
    )
    logger.info("Control consumer ready (reads from earliest offset).")
    seen_jobs: set[str] = set()   # prevent re-processing old enroll messages

    try:
        for msg in consumer:
            data = msg.value
            if data.get("type") == "enroll":
                jid = data["job_id"]
                if jid in seen_jobs:
                    continue   # already enrolled for this job
                seen_jobs.add(jid)
                enrolled = data["enrolled"]
                settings = data.get("settings", {})
                logger.info(f"[{jid}] Enroll message received — loading {len(enrolled)} person(s).")
                register_state(jid, enrolled, settings)   # also drains buffer
                logger.info(f"[{jid}] Enrollment complete. Ready to process frames.")

            elif data.get("type") == "cancel":
                jid = data.get("job_id")
                with _job_states_lock:
                    _job_states.pop(jid, None)
                with _buffer_lock:
                    _batch_buffer.pop(jid, None)
                seen_jobs.discard(jid)
                logger.info(f"[{jid}] Job cancelled.")
    except Exception as e:
        logger.error(f"Control consumer error: {e}")
    finally:
        consumer.close()
        # Restart control consumer if it dies
        logger.warning("Control consumer ended — restarting in 3s…")
        time.sleep(3)
        threading.Thread(target=control_consumer_thread, daemon=True).start()


# ══════════════════════════════════════════════════════════════════════════════
#  HEARTBEAT
# ══════════════════════════════════════════════════════════════════════════════

def heartbeat_thread(hb_producer: KafkaProducer):
    import subprocess
    logger.info("Heartbeat thread started.")
    while True:
        try:
            cpu = psutil.cpu_percent(interval=None) if _HAS_PSUTIL else 0.0
            gpu = 0.0
            try:
                out = subprocess.check_output(
                    ["nvidia-smi","--query-gpu=utilization.gpu",
                     "--format=csv,noheader,nounits"],
                    timeout=2, stderr=subprocess.DEVNULL)
                gpu = float(out.decode().strip().split("\n")[0])
            except Exception:
                pass
            avg_ms   = round(sum(_frame_times_ms)/max(len(_frame_times_ms),1),1) if _frame_times_ms else 0
            last_ms  = _frame_times_ms[-1] if _frame_times_ms else 0
            hist_ms  = list(_frame_times_ms[-30:])   # last 30 samples for graph
            hb_producer.send(cfg.TOPIC_HEARTBEAT, value={
                "slave_id":       SLAVE_ID,
                "queue_depth":    _queue_depth,
                "fps":            round(_fps_counter["fps"],1),
                "cpu_pct":        cpu,
                "gpu_pct":        gpu,
                "total_frames":   _frames_processed,
                "alive":          True,
                "avg_frame_ms":   avg_ms,
                "last_frame_ms":  last_ms,
                "frame_time_hist": hist_ms,
            })
        except Exception as e:
            logger.debug(f"Heartbeat error: {e}")
        time.sleep(cfg.HEARTBEAT_INTERVAL)


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    global _producer, _queue_depth

    # 1. Start control consumer FIRST — must be running before frames arrive
    ctrl_thread = threading.Thread(target=control_consumer_thread, daemon=True)
    ctrl_thread.start()

    # 2. Give control consumer time to connect and fetch historical messages
    logger.info("Waiting 3s for control consumer to connect and fetch enroll messages…")
    time.sleep(3)

    # 3. Shared Kafka producer
    _producer = KafkaProducer(
        bootstrap_servers=[cfg.KAFKA_BROKER],
            api_version=cfg.KAFKA_API_VERSION,
        max_request_size=cfg.KAFKA_MAX_MSG_BYTES,
        compression_type="gzip",
        value_serializer=lambda v: json.dumps(v).encode(),
        retries=5,
    )

    # 4. Heartbeat producer (separate instance to avoid thread contention)
    hb_prod = KafkaProducer(
        bootstrap_servers=[cfg.KAFKA_BROKER],
            api_version=cfg.KAFKA_API_VERSION,
        max_request_size=cfg.KAFKA_MAX_MSG_BYTES,
        value_serializer=lambda v: json.dumps(v).encode(),
        retries=3,
    )
    threading.Thread(target=heartbeat_thread, args=(hb_prod,), daemon=True).start()

    # 5. Frame consumer — all slaves share "slave_workers" group
    #    Kafka auto-assigns each slave a unique subset of partitions
    consumer = KafkaConsumer(
        cfg.TOPIC_FRAMES,
        bootstrap_servers=[cfg.KAFKA_BROKER],
            api_version=cfg.KAFKA_API_VERSION,
        group_id="slave_workers",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        max_partition_fetch_bytes=cfg.KAFKA_MAX_MSG_BYTES,
        value_deserializer=lambda m: json.loads(m.decode()),
        fetch_max_wait_ms=200,
        # Shorter session timeout so rebalance happens fast when a slave dies
        session_timeout_ms=10_000,
        heartbeat_interval_ms=3_000,
        max_poll_interval_ms=300_000,
    )

    logger.info(f"Slave {SLAVE_ID} frame consumer ready. Waiting for frames…")

    for msg in consumer:
        data   = msg.value
        job_id = data.get("job_id")
        if not job_id:
            continue

        # EOS sentinel
        if data.get("eos"):
            total_sent = data.get("total_sent", 0)
            _producer.send(cfg.TOPIC_PROGRESS, value={
                "job_id":      job_id,
                "video_idx":   data.get("video_idx", 0),
                "slave_id":    SLAVE_ID,
                "message":     f"Slave {SLAVE_ID} finished.",
                "done_frames": _job_progress[job_id]["done"],
                "progress":    85,
                "total_sent":  total_sent,
            })
            logger.info(f"[{job_id}] EOS — processed {_job_progress[job_id]['done']} frames.")
            continue

        _queue_depth += len(data.get("frames", []))

        state = get_state(job_id)
        if state is None:
            # FIX 2: buffer instead of drop — control message may be in flight
            with _buffer_lock:
                _batch_buffer[job_id].append(data)
            logger.warning(
                f"[{job_id}] State not yet ready — buffered batch "
                f"(buffer size: {len(_batch_buffer[job_id])}). "
                f"Will process when enroll message arrives."
            )
            _queue_depth = max(0, _queue_depth - len(data.get("frames",[])))
            continue

        _process_batch(data, state)


if __name__ == "__main__":
    main()
