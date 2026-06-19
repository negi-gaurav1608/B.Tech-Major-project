"""
master.py — Master node with clean job folder structure.

Each job now creates the following directory tree under /srv/crowdlens/jobs/<job_id>/:

    <job_id>/
    ├── input/
    │   ├── video_0.mp4          ← uploaded video file(s)
    │   ├── video_1.mkv
    │   ├── target_0_0.jpg       ← enrollment photos
    │   └── target_1_0.jpg
    ├── output/
    │   ├── annotated/
    │   │   ├── video_0_annotated.mp4   ← fully annotated output video
    │   │   └── video_1_annotated.mp4
    │   ├── clips/
    │   │   ├── video_0_Alice.mp4       ← per-person target clips
    │   │   └── video_0_Bob.mp4
    │   └── result.json                 ← complete job result summary
    ├── meta/
    │   ├── meta_v0.json         ← video metadata (fps, size, frame count)
    │   └── meta_v1.json
    ├── raw/
    │   ├── v0/                  ← raw (unannotated) skipped frames
    │   └── v1/
    └── result_v0.json           ← per-video result (used by assembler internally)
"""

import os, sys, uuid, json, time, base64, threading, logging, traceback, subprocess
from pathlib import Path
from queue import Queue, Empty
from collections import defaultdict
import cv2, numpy as np
from flask import (Flask, request, jsonify, Response,
                   send_file, send_from_directory, stream_with_context)
from werkzeug.utils import secure_filename
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

sys.path.insert(0, str(Path(__file__).parent))
import config_distributed as cfg
from load_balancer import get_load_balancer

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [MASTER] %(levelname)s: %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("master")

for d in [cfg.jobs_dir(), cfg.uploads_dir(), cfg.logs_dir()]:
    d.mkdir(parents=True, exist_ok=True)

BASE_DIR = Path(__file__).parent
app = Flask(__name__, static_url_path="/static",
            static_folder=str(BASE_DIR / "static"))
app.config["MAX_CONTENT_LENGTH"] = 4 * 1024 * 1024 * 1024
ALLOWED_VIDEO = {".mp4", ".avi", ".mkv", ".mov", ".wmv", ".m4v"}
ALLOWED_IMAGE = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}

# ── In-memory job store ────────────────────────────────────────────────────────
_jobs: dict[str, dict] = {}
_lock = threading.Lock()


def _jget(jid):
    with _lock:
        return _jobs.get(jid)


def _jset(jid, **kw):
    with _lock:
        job = _jobs.get(jid)
        if not job:
            return
        job.update(kw)
        payload = {k: v for k, v in job.items()
                   if k not in ("_sse_q", "_frame_bufs", "_person_frames")}
        try:
            job["_sse_q"].put_nowait(payload)
        except Exception:
            pass


# ── Job directory helpers ──────────────────────────────────────────────────────
def job_dir(jid: str) -> Path:
    return cfg.jobs_dir() / jid

def input_dir(jid: str) -> Path:
    return job_dir(jid) / "input"

def output_dir(jid: str) -> Path:
    return job_dir(jid) / "output"

def annotated_dir(jid: str) -> Path:
    return output_dir(jid) / "annotated"

def clips_dir(jid: str) -> Path:
    return output_dir(jid) / "clips"

def meta_dir(jid: str) -> Path:
    return job_dir(jid) / "meta"

def raw_dir(jid: str, video_idx: int) -> Path:
    return job_dir(jid) / "raw" / f"v{video_idx}"

def result_json_path(jid: str) -> Path:
    return output_dir(jid) / "result.json"


def setup_job_dirs(jid: str):
    """Create the full directory tree for a new job."""
    for d in [
        input_dir(jid),
        annotated_dir(jid),
        clips_dir(jid),
        meta_dir(jid),
    ]:
        d.mkdir(parents=True, exist_ok=True)
    logger.info(f"[{jid}] Job directory created: {job_dir(jid)}")


def reencode_for_browser(src: Path) -> Path:
    """
    Re-encode mp4v video to browser-compatible H.264 using ffmpeg.
    H.264 + faststart moov = plays natively in Chrome/Firefox/Safari.
    Returns path to re-encoded file; falls back to original on failure.
    """
    # Use a temp file, then atomically replace src so the filename never changes
    tmp = src.with_name(src.stem + "_tmp_h264.mp4")
    try:
        result = subprocess.run(
            ["ffmpeg", "-y", "-i", str(src),
             "-vcodec", "libx264", "-crf", "23", "-preset", "fast",
             "-pix_fmt", "yuv420p", "-movflags", "+faststart",
             "-acodec", "copy", str(tmp)],
            capture_output=True, text=True, timeout=600,
        )
        if result.returncode == 0 and tmp.exists() and tmp.stat().st_size > 0:
            src.unlink(missing_ok=True)   # remove original mp4v
            tmp.rename(src)               # rename tmp → original name
            logger.info(f"Re-encoded for browser (H264): {src.name}")
            return src
        logger.warning(f"ffmpeg failed (code {result.returncode}): {result.stderr[-300:]}")
        tmp.unlink(missing_ok=True)
    except Exception as e:
        logger.warning(f"ffmpeg unavailable ({e}); video may not play in browser")
        tmp.unlink(missing_ok=True)
    return src


# ── Job factory ────────────────────────────────────────────────────────────────
def _new_job(jid, video_paths, person_names, target_paths, settings):
    job = dict(
        id=jid,
        status="queued",
        video_paths=video_paths,
        person_names=person_names,
        target_paths=target_paths,
        settings=settings,
        videos=[{
            "path": vp, "name": Path(vp).name,
            "status": "queued", "progress": 0,
            "total_frames": 0, "done_frames": 0,
            "person_count_avg": 0, "person_count_max": 0,
            "target_detections": 0, "message": "Queued",
        } for vp in video_paths],
        progress=0, total_frames=0, done_frames=0,
        message="Queued", error=None, result=None,
        created_at=time.time(),
        _sse_q=Queue(),
        _frame_bufs=[{} for _ in video_paths],
        _person_frames=[defaultdict(list) for _ in video_paths],
    )
    with _lock:
        _jobs[jid] = job
    return job


# ══════════════════════════════════════════════════════════════════════════════
#  FACE ENROLLMENT
# ══════════════════════════════════════════════════════════════════════════════

def enroll_persons(job_id, target_paths, person_names):
    os.environ["ORT_LOGGING_LEVEL"] = "3"
    from insightface.app import FaceAnalysis
    fa = FaceAnalysis(name="buffalo_sc", providers=["CPUExecutionProvider"])
    fa.prepare(ctx_id=-1, det_size=(320, 320))

    enrolled = []
    for idx, paths in enumerate(target_paths):
        name = person_names[idx] if idx < len(person_names) else f"Person {idx+1}"
        embs = []
        for p in paths[:5]:
            img = cv2.imread(p)
            if img is None:
                continue
            faces = fa.get(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))
            if not faces:
                continue
            lg = max(faces, key=lambda f: (f.bbox[2]-f.bbox[0]) * (f.bbox[3]-f.bbox[1]))
            emb = lg.normed_embedding.astype(np.float32)
            embs.append(emb / np.linalg.norm(emb))
            logger.info(f"  [{job_id}] Enrolled {name}: {Path(p).name}")
        if not embs:
            raise ValueError(f"No face detected in images for '{name}'.")
        avg = np.mean(embs, axis=0)
        avg /= np.linalg.norm(avg)
        enrolled.append({
            "name": name,
            "embedding_b64":   base64.b64encode(avg.tobytes()).decode(),
            "embedding_shape": list(avg.shape),
        })
        _jset(job_id, message=f"Enrolled: {name}")
    return enrolled


# ══════════════════════════════════════════════════════════════════════════════
#  PRODUCER THREAD  (one per video)
# ══════════════════════════════════════════════════════════════════════════════

def producer_thread(job_id, video_idx, video_path, lb, producer):
    """
    Read one video file, JPEG-encode each frame, batch into Kafka messages.
    Skipped frames are saved as raw JPEGs in raw/<v{idx}>/ for the assembler.
    Video metadata (fps, width, height, total_frames) is written to meta/.
    """
    vid_key    = f"{job_id}:v{video_idx}"
    frame_skip = int(_jget(job_id)["settings"].get("frame_skip", 1))

    try:
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            raise RuntimeError(f"Cannot open video: {video_path}")

        total  = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps    = cap.get(cv2.CAP_PROP_FPS) or 25.0
        width  = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

        # Write metadata to meta/
        mp = meta_dir(job_id) / f"meta_v{video_idx}.json"
        mp.write_text(json.dumps({
            "fps": fps, "width": width, "height": height,
            "total_frames": total,
            "video_path": video_path,
            "video_name": Path(video_path).name,
        }))

        with _lock:
            job = _jobs[job_id]
            job["videos"][video_idx].update(
                total_frames=total, status="processing",
                message=f"Streaming {total} frames")
            job["total_frames"] = sum(v["total_frames"] for v in job["videos"])

        # Wait a moment after broadcasting embeddings so slaves have time to
        # register job state before the first frame batch arrives
        time.sleep(2)

        # Ensure raw dir exists for this video
        rdir = raw_dir(job_id, video_idx)
        rdir.mkdir(parents=True, exist_ok=True)

        batch = []
        frame_idx = 0
        sent_count = 0

        while True:
            ret, frame = cap.read()
            if not ret:
                break
            frame_idx += 1

            # Save skipped frames as raw JPEGs
            if frame_skip > 1 and (frame_idx % frame_skip != 0):
                p = rdir / f"{frame_idx:07d}.jpg"
                cv2.imwrite(str(p), frame, [cv2.IMWRITE_JPEG_QUALITY, 90])
                continue

            ts = frame_idx / fps
            _, buf = cv2.imencode(".jpg", frame,
                                  [cv2.IMWRITE_JPEG_QUALITY, cfg.KAFKA_FRAME_QUALITY])
            batch.append({
                "frame_idx": frame_idx,
                "timestamp": round(ts, 4),
                "frame_b64": base64.b64encode(buf.tobytes()).decode(),
            })

            if len(batch) >= cfg.FRAMES_PER_BATCH:
                # FIX 3: use explicit partition index — bypasses key hashing
                part, pk = lb.best_partition(vid_key)
                producer.send(cfg.TOPIC_FRAMES,
                               value={"job_id": job_id, "video_idx": video_idx,
                                      "vid_key": vid_key, "frames": batch},
                               key=pk, partition=part)
                sent_count += len(batch)
                batch = []

        if batch:
            part, pk = lb.best_partition(vid_key)
            producer.send(cfg.TOPIC_FRAMES,
                           value={"job_id": job_id, "video_idx": video_idx,
                                  "vid_key": vid_key, "frames": batch},
                           key=pk, partition=part)
            sent_count += len(batch)

        # EOS sentinel — broadcast to all partitions so every slave sees it
        for p in range(cfg.KAFKA_PARTITIONS):
            producer.send(cfg.TOPIC_FRAMES,
                           value={"job_id": job_id, "video_idx": video_idx,
                                  "vid_key": vid_key, "eos": True,
                                  "total_sent": sent_count},
                           key=vid_key.encode(), partition=p)
        producer.flush()
        cap.release()
        logger.info(f"[{job_id}] v{video_idx} producer done — {sent_count} frames sent.")

    except Exception as e:
        logger.error(f"[{job_id}] v{video_idx} producer error:\n{traceback.format_exc()}")
        with _lock:
            job = _jobs.get(job_id, {})
            if "videos" in job:
                job["videos"][video_idx].update(status="error", message=str(e))


# ══════════════════════════════════════════════════════════════════════════════
#  ASSEMBLER THREAD  (one per video)
# ══════════════════════════════════════════════════════════════════════════════

def assembler_thread(job_id, video_idx, enrolled):
    """
    Consume frame results from Kafka, reassemble in frame order,
    write to output/annotated/<name>_annotated.mp4,
    extract per-person clips to output/clips/<name>_<person>.mp4.
    """
    # Wait for metadata written by producer
    mp = meta_dir(job_id) / f"meta_v{video_idx}.json"
    for _ in range(60):
        if mp.exists():
            break
        time.sleep(0.5)

    meta   = json.loads(mp.read_text())
    fps    = meta["fps"]
    width  = meta["width"]
    height = meta["height"]
    total  = meta["total_frames"]
    vname  = Path(meta["video_name"]).stem   # stem without extension

    frame_skip = int(_jget(job_id)["settings"].get("frame_skip", 1))
    fourcc     = cv2.VideoWriter_fourcc(*"mp4v")

    # Output: output/annotated/<stem>_annotated.mp4
    ann_path = annotated_dir(job_id) / f"{vname}_annotated.mp4"
    writer   = cv2.VideoWriter(str(ann_path), fourcc, fps, (width, height))

    rdir = raw_dir(job_id, video_idx)

    result_buf: dict[int, dict]    = {}
    person_frames: dict[int, list] = defaultdict(list)
    received   = 0
    total_sent = None
    counts     = []
    lb         = get_load_balancer()

    # Kafka consumer
    consumer = KafkaConsumer(
        cfg.TOPIC_RESULTS, cfg.TOPIC_PROGRESS,
        bootstrap_servers=[cfg.KAFKA_BROKER],
        group_id=f"asm_{job_id}_v{video_idx}",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        max_partition_fetch_bytes=cfg.KAFKA_MAX_MSG_BYTES,
        value_deserializer=lambda m: json.loads(m.decode()),
        consumer_timeout_ms=30_000,
    )

    try:
        for msg in consumer:
            data = msg.value
            if data.get("job_id") != job_id:
                continue
            vi = data.get("video_idx")
            if vi is not None and vi != video_idx:
                continue

            # Progress heartbeat
            if msg.topic == cfg.TOPIC_PROGRESS:
                done = data.get("done_frames", 0)
                pct  = min(85, int(done / max(total, 1) * 85))
                with _lock:
                    job = _jobs.get(job_id, {})
                    if "videos" in job:
                        job["videos"][video_idx].update(
                            done_frames=done, progress=pct,
                            message=data.get("message", ""))
                        job["done_frames"] = sum(v["done_frames"] for v in job["videos"])
                        job["progress"]    = min(85, int(
                            job["done_frames"] / max(job["total_frames"], 1) * 85))
                try:
                    j = _jget(job_id)
                    if j:
                        j["_sse_q"].put_nowait({
                            k: v for k, v in j.items()
                            if k not in ("_sse_q", "_frame_bufs", "_person_frames")
                        })
                except Exception:
                    pass
                if data.get("total_sent"):
                    total_sent = data["total_sent"]
                continue

            # Frame results
            if msg.topic == cfg.TOPIC_RESULTS:
                slave_id = data.get("slave_id", "")
                for fr in data.get("frame_results", []):
                    result_buf[fr["frame_idx"]] = fr
                    received += 1
                    lb.acknowledge_result(slave_id)
                if data.get("total_sent"):
                    total_sent = data["total_sent"]

            if total_sent and received >= total_sent:
                break

    except Exception as e:
        logger.warning(f"[{job_id}] v{video_idx} assembler consumer ended: {e}")
    finally:
        consumer.close()

    logger.info(f"[{job_id}] v{video_idx}: {received} frame results received.")
    _jset(job_id, message=f"Building annotated video {video_idx + 1}…")

    # ── Write annotated video in frame order ───────────────────────────────────
    for fidx in range(1, total + 1):
        # Skipped frame — write raw unannotated version
        if frame_skip > 1 and (fidx % frame_skip != 0):
            raw = rdir / f"{fidx:07d}.jpg"
            if raw.exists():
                f = cv2.imread(str(raw))
                if f is not None:
                    writer.write(f)
            continue

        res = result_buf.get(fidx)
        if res is None:
            continue

        ann = cv2.imdecode(
            np.frombuffer(base64.b64decode(res["annotated_b64"]), np.uint8),
            cv2.IMREAD_COLOR
        )
        if ann is not None:
            writer.write(ann)

        counts.append(res.get("person_count", 0))

        for pi, pdet in enumerate(res.get("person_detections", [])):
            if isinstance(pdet, dict) and pdet.get("found"):
                person_frames[pi].append((fidx, res["timestamp"], ann))

    writer.release()
    # Re-encode to H.264 so the browser can play it natively
    ann_path = reencode_for_browser(ann_path)
    logger.info(f"[{job_id}] v{video_idx} annotated video: {ann_path}")

    # ── Extract per-person clips → output/clips/ ───────────────────────────────
    person_results = []
    for pi, enr in enumerate(enrolled):
        name   = enr["name"]
        flist  = person_frames.get(pi, [])
        clip_path = None

        if flist:
            safe_name = secure_filename(name)
            clip_path = clips_dir(job_id) / f"{vname}_{safe_name}.mp4"
            cw = cv2.VideoWriter(str(clip_path), fourcc, fps, (width, height))
            for (_, _, frm) in flist:
                if frm is not None:
                    cw.write(frm)
            cw.release()
            clip_path = reencode_for_browser(clip_path)
            logger.info(f"[{job_id}] v{video_idx} clip for '{name}': {clip_path}")

        timestamps = [round(ts, 2) for (_, ts, _) in flist]
        # Use final clip_path.name (may be _h264.mp4 after re-encode)
        person_results.append({
            "name":        name,
            "match_count": len(flist),
            "clip_file":   clip_path.name if clip_path else None,
            "timestamps":  timestamps[:200],
            "found":       len(flist) > 0,
        })

    # ── Per-video summary (internal) ───────────────────────────────────────────
    crowd_timeline = [
        counts[i] for i in range(0, len(counts), max(1, len(counts) // 100))
    ]
    vs = {
        "video_name":       Path(meta["video_path"]).name,
        "duration_s":       round(total / fps, 2),
        "total_frames":     total,
        "frames_processed": received,
        "source_fps":       round(fps, 2),
        "resolution":       f"{width}x{height}",
        "avg_persons":      round(sum(counts) / max(len(counts), 1), 2),
        "max_persons":      max(counts) if counts else 0,
        "persons":          person_results,
        "annotated_file":   ann_path.name,   # actual filename after optional reencode
        "crowd_timeline":   crowd_timeline,
    }
    (job_dir(job_id) / f"result_v{video_idx}.json").write_text(
        json.dumps(vs, indent=2)
    )

    with _lock:
        job = _jobs.get(job_id, {})
        if "videos" in job:
            job["videos"][video_idx].update(
                status="done", progress=100,
                person_count_avg=vs["avg_persons"],
                person_count_max=vs["max_persons"],
                target_detections=sum(p["match_count"] for p in person_results),
                message="Done",
            )

    logger.info(f"[{job_id}] v{video_idx} assembly complete.")
    return vs


# ══════════════════════════════════════════════════════════════════════════════
#  JOB ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

def run_job(job_id):
    """Top-level job thread: enroll → broadcast → produce + assemble in parallel."""
    job = _jget(job_id)
    setup_job_dirs(job_id)

    try:
        # 1. Enroll
        _jset(job_id, status="enrolling", message="Enrolling target persons…")
        enrolled = enroll_persons(job_id, job["target_paths"], job["person_names"])

        # 2. Broadcast embeddings to slaves
        ctrl = KafkaProducer(
            bootstrap_servers=[cfg.KAFKA_BROKER],
            max_request_size=cfg.KAFKA_MAX_MSG_BYTES,
            value_serializer=lambda v: json.dumps(v).encode(),
            retries=5,
        )
        ctrl.send(cfg.TOPIC_CONTROL, value={
            "type": "enroll", "job_id": job_id,
            "enrolled": enrolled, "settings": job["settings"],
        })
        ctrl.flush()
        ctrl.close()
        logger.info(f"[{job_id}] Embeddings broadcast to slaves.")

        # 3. Start assemblers BEFORE producers so no results are missed
        _jset(job_id, status="processing", message="Processing videos…")
        lb = get_load_balancer()
        producer = KafkaProducer(
            bootstrap_servers=[cfg.KAFKA_BROKER],
            max_request_size=cfg.KAFKA_MAX_MSG_BYTES,
            compression_type="gzip",
            value_serializer=lambda v: json.dumps(v).encode(),
            retries=5, linger_ms=20,
        )

        asm_results = [None] * len(job["video_paths"])

        def _asm(vi, vp):
            asm_results[vi] = assembler_thread(job_id, vi, enrolled)

        asm_threads = []
        for vi, vp in enumerate(job["video_paths"]):
            t = threading.Thread(target=_asm, args=(vi, vp), daemon=True)
            t.start()
            asm_threads.append(t)

        # 4. Start producers
        for vi, vp in enumerate(job["video_paths"]):
            threading.Thread(
                target=producer_thread,
                args=(job_id, vi, vp, lb, producer),
                daemon=True,
            ).start()

        for t in asm_threads:
            t.join()

        producer.flush()
        producer.close()

        # 5. Build aggregate result.json → output/result.json
        _jset(job_id, message="Writing final result…", progress=97)
        all_summaries = [r for r in asm_results if r]
        all_persons = {}
        for vs in all_summaries:
            for p in vs["persons"]:
                nm = p["name"]
                if nm not in all_persons:
                    all_persons[nm] = {
                        "name": nm, "total_matches": 0,
                        "videos_found": [], "clips": [],
                    }
                if p["found"]:
                    all_persons[nm]["total_matches"] += p["match_count"]
                    all_persons[nm]["videos_found"].append(vs["video_name"])
                    if p.get("clip_file"):
                        all_persons[nm]["clips"].append(p["clip_file"])

        summary = {
            "job_id":       job_id,
            "job_dir":      str(job_dir(job_id)),
            "output_dir":   str(output_dir(job_id)),
            "videos":       all_summaries,
            "persons":      list(all_persons.values()),
            "total_videos": len(all_summaries),
        }

        # Write to output/result.json (the canonical output location)
        result_json_path(job_id).write_text(json.dumps(summary, indent=2))
        logger.info(f"[{job_id}] result.json → {result_json_path(job_id)}")

        _jset(job_id, status="done", progress=100, message="Done!", result=summary)
        logger.info(f"[{job_id}] Complete. Output: {output_dir(job_id)}")

    except Exception as e:
        logger.error(f"[{job_id}] Job failed:\n{traceback.format_exc()}")
        _jset(job_id, status="error", error=str(e), message=f"Error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  FLASK ROUTES
# ══════════════════════════════════════════════════════════════════════════════

def ensure_topics():
    try:
        admin = KafkaAdminClient(bootstrap_servers=[cfg.KAFKA_BROKER])
        topics = [
            NewTopic(t, num_partitions=cfg.KAFKA_PARTITIONS,
                     replication_factor=1,
                     topic_configs={"max.message.bytes": str(cfg.KAFKA_MAX_MSG_BYTES)})
            for t in [cfg.TOPIC_FRAMES, cfg.TOPIC_RESULTS,
                      cfg.TOPIC_PROGRESS, cfg.TOPIC_CONTROL, cfg.TOPIC_HEARTBEAT]
        ]
        admin.create_topics(topics)
        logger.info("Kafka topics created.")
    except TopicAlreadyExistsError:
        logger.info("Kafka topics already exist.")
    except Exception as e:
        logger.warning(f"Topic setup: {e}")


# ── Video streaming with Range support ───────────────────────────────────────
def stream_video(path: Path):
    """
    Serve a video file with proper HTTP Range request support.
    Browsers REQUIRE Range responses to:
      • Seek within a video (clicking on the timeline)
      • Start playback mid-file on slow connections
      • Resume interrupted downloads
    Flask's send_file with conditional=True handles ETags/If-None-Match
    but does NOT split responses on Range headers — we do it here.
    """
    file_size = path.stat().st_size
    range_header = request.headers.get("Range", None)

    if not range_header:
        # Full file response — browser will follow up with a Range request
        resp = Response(
            open(str(path), "rb"),
            200,
            mimetype="video/mp4",
            direct_passthrough=True,
        )
        resp.headers["Content-Length"]  = file_size
        resp.headers["Accept-Ranges"]   = "bytes"
        resp.headers["Cache-Control"]   = "no-cache"
        return resp

    # Parse "bytes=start-end"
    byte_range = range_header.replace("bytes=", "")
    parts      = byte_range.split("-")
    start      = int(parts[0]) if parts[0] else 0
    end        = int(parts[1]) if len(parts) > 1 and parts[1] else file_size - 1
    end        = min(end, file_size - 1)
    length     = end - start + 1

    def generate_chunk():
        with open(str(path), "rb") as f:
            f.seek(start)
            remaining = length
            chunk_size = 64 * 1024   # 64 KB chunks
            while remaining > 0:
                chunk = f.read(min(chunk_size, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                yield chunk

    resp = Response(
        generate_chunk(),
        206,   # Partial Content
        mimetype="video/mp4",
        direct_passthrough=True,
    )
    resp.headers["Content-Range"]  = f"bytes {start}-{end}/{file_size}"
    resp.headers["Content-Length"] = length
    resp.headers["Accept-Ranges"]  = "bytes"
    resp.headers["Cache-Control"]  = "no-cache"
    return resp


@app.route("/")
def index():
    return send_from_directory(str(BASE_DIR), "dashboard.html")


@app.route("/api/analyze", methods=["POST"])
def analyze():
    video_keys = [k for k in sorted(request.files.keys())
                  if k == "video" or k.startswith("video_")]
    if not video_keys:
        return jsonify({"error": "No video files provided"}), 400

    job_id = str(uuid.uuid4())[:8]
    setup_job_dirs(job_id)

    # Save uploaded videos → input/
    video_paths = []
    for key in video_keys:
        vf  = request.files[key]
        ext = Path(vf.filename).suffix.lower()
        if ext in ALLOWED_VIDEO:
            vp = input_dir(job_id) / f"video_{len(video_paths)}{ext}"
            vf.save(str(vp))
            video_paths.append(str(vp))

    if not video_paths:
        return jsonify({"error": "No valid video files"}), 400

    # Save enrollment images → input/
    person_names, target_paths = [], []
    n = 0
    while True:
        if (f"target_{n}_name" not in request.form and
                f"target_{n}_img0" not in request.files):
            break
        name = request.form.get(f"target_{n}_name", f"Person {n+1}")
        imgs = []
        m = 0
        while f"target_{n}_img{m}" in request.files:
            imgf = request.files[f"target_{n}_img{m}"]
            ie   = Path(imgf.filename).suffix.lower()
            if ie in ALLOWED_IMAGE:
                ip = input_dir(job_id) / f"target_{n}_{m}{ie}"
                imgf.save(str(ip))
                imgs.append(str(ip))
            m += 1
        if imgs:
            person_names.append(name)
            target_paths.append(imgs)
        n += 1

    if not target_paths:
        return jsonify({"error": "No target images provided"}), 400

    settings = {
        k: request.form.get(k, v)
        for k, v in {
            "yolo_model":     "yolov8s.pt",
            "yolo_conf":      "0.40",
            "face_threshold": "0.45",
            "frame_skip":     "1",
        }.items()
    }

    _new_job(job_id, video_paths, person_names, target_paths, settings)
    threading.Thread(target=run_job, args=(job_id,), daemon=True).start()

    logger.info(f"Started job {job_id}: "
                f"{len(video_paths)} video(s), {len(target_paths)} person(s)")
    return jsonify({
        "job_id":       job_id,
        "job_dir":      str(job_dir(job_id)),
        "video_count":  len(video_paths),
        "person_count": len(target_paths),
    })


@app.route("/api/progress/<jid>")
def progress(jid):
    def gen():
        job = _jget(jid)
        if not job:
            yield f"data: {json.dumps({'error': 'Not found'})}\n\n"
            return
        q = job["_sse_q"]
        while True:
            try:
                ev = q.get(timeout=25)
                yield f"data: {json.dumps(ev)}\n\n"
                if ev.get("status") in ("done", "error"):
                    break
            except Empty:
                yield ": ping\n\n"

    return Response(
        stream_with_context(gen()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/result/<jid>")
def result(jid):
    job = _jget(jid)
    if not job:
        return jsonify({"error": "Not found"}), 404
    if job["status"] != "done":
        return jsonify({"status": job["status"], "message": job["message"]}), 202
    return jsonify(job["result"])


@app.route("/api/status/<jid>")
def status(jid):
    job = _jget(jid)
    if not job:
        return jsonify({"error": "Not found"}), 404
    return jsonify({k: job[k] for k in (
        "status", "progress", "done_frames",
        "total_frames", "message", "error", "videos",
    )})


@app.route("/api/files/<jid>")
def list_files(jid):
    """List all output files for a completed job."""
    jdir = job_dir(jid)
    if not jdir.exists():
        return jsonify({"error": "Job not found"}), 404

    odir = output_dir(jid)
    files = {
        "job_dir":    str(jdir),
        "input":      [f.name for f in input_dir(jid).iterdir() if f.is_file()],
        "annotated":  [f.name for f in annotated_dir(jid).iterdir() if f.is_file()]
                       if annotated_dir(jid).exists() else [],
        "clips":      [f.name for f in clips_dir(jid).iterdir() if f.is_file()]
                       if clips_dir(jid).exists() else [],
        "result_json": str(result_json_path(jid))
                        if result_json_path(jid).exists() else None,
    }
    return jsonify(files)


@app.route("/api/video/<jid>/annotated/<int:vidx>")
def video_annotated(jid, vidx):
    """Serve annotated video for video index vidx."""
    adir = annotated_dir(jid)
    if not adir.exists():
        return jsonify({"error": "Not ready"}), 404

    # Find annotated video — glob by video stem so filename change never breaks lookup
    mp = meta_dir(jid) / f"meta_v{vidx}.json"
    p  = None
    if mp.exists():
        meta  = json.loads(mp.read_text())
        vname = Path(meta["video_name"]).stem
        # Try exact name first, then any mp4 that starts with the stem
        exact = adir / f"{vname}_annotated.mp4"
        if exact.exists():
            p = exact
        else:
            candidates = sorted(adir.glob(f"{vname}*.mp4"))
            if candidates:
                p = candidates[0]
    if p is None:
        # Final fallback: nth mp4 in annotated dir
        all_mp4 = sorted(adir.glob("*.mp4"))
        p = all_mp4[vidx] if vidx < len(all_mp4) else None
    if p is None or not p.exists():
        return jsonify({"error": "Not ready — video still processing"}), 404
    return stream_video(p)


@app.route("/api/video/<jid>/clip/<int:vidx>/<int:pidx>")
def video_clip(jid, vidx, pidx):
    """Serve target person clip for video vidx, person pidx."""
    rp = job_dir(jid) / f"result_v{vidx}.json"
    if not rp.exists():
        return jsonify({"error": "Not ready"}), 404

    vs = json.loads(rp.read_text())
    persons = vs.get("persons", [])
    if pidx >= len(persons):
        return jsonify({"error": "Person index out of range"}), 404

    cf = persons[pidx].get("clip_file")
    if not cf:
        return jsonify({"error": "No clip for this person"}), 404

    p = clips_dir(jid) / cf
    if not p.exists():
        return jsonify({"error": "Clip file not found"}), 404
    return stream_video(p)


@app.route("/api/download/<jid>/result")
def download_result(jid):
    """Download the complete result.json for a job."""
    rp = result_json_path(jid)
    if not rp.exists():
        return jsonify({"error": "Result not ready"}), 404
    return send_file(str(rp), mimetype="application/json",
                     as_attachment=True,
                     download_name=f"crowdlens_{jid}_result.json")


@app.route("/api/dashboard")
def dashboard():
    lb = get_load_balancer()
    with _lock:
        jobs_summary = [{
            "id":          j["id"],
            "status":      j["status"],
            "progress":    j["progress"],
            "message":     j["message"],
            "videos":      j.get("videos", []),
            "created_at":  j["created_at"],
            "total_frames":j.get("total_frames", 0),
            "done_frames": j.get("done_frames", 0),
            "job_dir":     str(job_dir(j["id"])),
            "output_dir":  str(output_dir(j["id"])),
        } for j in _jobs.values()]

    return jsonify({
        "jobs":      sorted(jobs_summary, key=lambda j: j["created_at"], reverse=True),
        "slaves":    lb.get_status(),
        "timestamp": time.time(),
    })


@app.route("/api/jobs")
def list_jobs():
    with _lock:
        return jsonify([{
            "id":       j["id"],
            "status":   j["status"],
            "progress": j["progress"],
            "videos":   len(j.get("video_paths", [])),
            "created":  j["created_at"],
            "job_dir":  str(job_dir(j["id"])),
        } for j in sorted(_jobs.values(),
                           key=lambda x: x["created_at"], reverse=True)])


@app.route("/api/slaves")
def slaves():
    return jsonify(get_load_balancer().get_status())


@app.route("/api/cancel/<jid>", methods=["POST"])
def cancel_job(jid):
    job = _jget(jid)
    if not job:
        return jsonify({"error": "Not found"}), 404
    _jset(jid, status="cancelled", message="Cancelled by user.")
    try:
        p = KafkaProducer(
            bootstrap_servers=[cfg.KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        p.send(cfg.TOPIC_CONTROL, value={"type": "cancel", "job_id": jid})
        p.flush()
        p.close()
    except Exception:
        pass
    return jsonify({"ok": True})


@app.route("/api/reencode/<jid>", methods=["POST"])
def reencode_job(jid):
    """Re-encode all mp4v videos for an existing job to H.264."""
    jdir = job_dir(jid)
    if not jdir.exists():
        return jsonify({"error": "Job not found"}), 404
    done, failed = [], []
    for mp4 in list(annotated_dir(jid).glob("*.mp4")) + list(clips_dir(jid).glob("*.mp4")):
        probe = subprocess.run(
            ["ffprobe","-v","quiet","-select_streams","v:0",
             "-show_entries","stream=codec_name",
             "-of","default=noprint_wrappers=1:nokey=1",str(mp4)],
            capture_output=True, text=True)
        if probe.stdout.strip() == "h264":
            done.append(mp4.name + " (already H.264)"); continue
        reencode_for_browser(mp4)
        done.append(mp4.name)
    return jsonify({"reencoded": done, "failed": failed})


def migrate_existing_jobs():
    """On startup: scan all job dirs for mp4v files and re-encode to H.264."""
    def _run():
        jobs_root = cfg.jobs_dir()
        if not jobs_root.exists(): return
        for jdir in jobs_root.iterdir():
            if not jdir.is_dir(): continue
            for folder in [jdir/"output"/"annotated", jdir/"output"/"clips"]:
                if not folder.exists(): continue
                for mp4 in folder.glob("*.mp4"):
                    probe = subprocess.run(
                        ["ffprobe","-v","quiet","-select_streams","v:0",
                         "-show_entries","stream=codec_name",
                         "-of","default=noprint_wrappers=1:nokey=1",str(mp4)],
                        capture_output=True, text=True)
                    if probe.stdout.strip() != "h264":
                        logger.info(f"Migrating {mp4.name} to H.264")
                        reencode_for_browser(mp4)
    threading.Thread(target=_run, daemon=True).start()


if __name__ == "__main__":
    ensure_topics()
    get_load_balancer()
    migrate_existing_jobs()
    logger.info(f"Master on http://0.0.0.0:{cfg.WEB_PORT}")
    app.run(host="0.0.0.0", port=cfg.WEB_PORT, debug=False, threaded=True)
