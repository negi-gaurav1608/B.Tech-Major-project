"""
config_distributed.py — Distributed system configuration.
Edit KAFKA_BROKER, MASTER_IP, and SHARED_DIR before running.
"""
from pathlib import Path

# ── Network ────────────────────────────────────────────────────────────────────
KAFKA_BROKER = "10.165.135.138:9092"
MASTER_IP    = "10.165.135.138"
WEB_PORT     = 5000
SHARED_DIR   = Path("/srv/crowdlens")

# ── Kafka topics ───────────────────────────────────────────────────────────────
TOPIC_FRAMES    = "video_frames"
TOPIC_RESULTS   = "frame_results"
TOPIC_PROGRESS  = "job_progress"
TOPIC_CONTROL   = "job_control"
TOPIC_HEARTBEAT = "slave_heartbeat"   # slaves report health/load to master

KAFKA_PARTITIONS    = 4
KAFKA_MAX_MSG_BYTES = 10_485_760      # 10 MB

# ── Frame batching ─────────────────────────────────────────────────────────────
FRAMES_PER_BATCH    = 4
KAFKA_FRAME_QUALITY = 85

# ── Load balancer ──────────────────────────────────────────────────────────────
HEARTBEAT_INTERVAL = 3    # seconds between slave heartbeats
SLAVE_TIMEOUT_S    = 15   # dead if no heartbeat for this long
SLAVE_QUEUE_LIMIT  = 50   # redirect if slave has more than this many queued frames

# ── Detection (applied on slave) ──────────────────────────────────────────────
YOLO_MODEL                = "yolov8s.pt"
YOLO_CONF                 = 0.40
YOLO_IOU                  = 0.35
YOLO_IMGSZ                = 1280
PERSON_CLASS_ID           = 0
INSIGHTFACE_MODEL         = "buffalo_sc"
FACE_DISTANCE_THRESHOLD   = 0.45
MIN_FACE_SIZE             = 40
FACE_CHECK_EVERY_N_FRAMES = 5
USE_GPU                   = True

# ── Paths ──────────────────────────────────────────────────────────────────────
def jobs_dir()    -> Path: return SHARED_DIR / "jobs"
def uploads_dir() -> Path: return SHARED_DIR / "uploads"
def logs_dir()    -> Path: return SHARED_DIR / "logs"

# ── Annotation colours (BGR) ───────────────────────────────────────────────────
COLOR_PERSON    = (0, 200, 0)
COLOR_TARGET    = (0, 0, 220)
COLOR_UNCERTAIN = (0, 165, 255)
BOX_THICKNESS   = 2
FLASH_ON_TARGET = True
