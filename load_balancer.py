"""
load_balancer.py — Weighted load balancer for frame distribution across slaves.

How it works
------------
Each slave publishes a heartbeat every HEARTBEAT_INTERVAL seconds on
TOPIC_HEARTBEAT containing:
  { slave_id, queue_depth, fps, cpu_pct, gpu_pct, alive: true }

The LoadBalancer maintains a registry of live slaves and their current load.
When the master producer asks which Kafka partition to send a frame batch to,
the load balancer returns the partition key of the least-loaded live slave.

Weight formula
--------------
  weight = 1 / (queue_depth + 1)   ← lower queue = higher weight

Partitions are mapped to slave IDs via round-robin assignment, refreshed
whenever slaves join or leave.

Failure handling
----------------
  • Slave missing for > SLAVE_TIMEOUT_S → marked dead, excluded from routing
  • If ALL slaves are dead → route to partition 0 (best-effort, Kafka buffers it)
  • When a dead slave recovers and sends a heartbeat → re-added automatically
"""

import time
import json
import logging
import threading
from collections import defaultdict
from kafka import KafkaConsumer
import config_distributed as cfg

logger = logging.getLogger("load_balancer")


class SlaveInfo:
    def __init__(self, slave_id: str, partition: int):
        self.slave_id    = slave_id
        self.partition   = partition        # Kafka partition this slave owns
        self.queue_depth = 0               # frames currently queued
        self.fps         = 0.0             # frames processed per second
        self.cpu_pct     = 0.0
        self.gpu_pct     = 0.0
        self.last_seen   = time.time()
        self.alive       = True
        self.total_frames    = 0           # lifetime frames processed
        self.avg_frame_ms    = 0.0         # average per-frame processing time (ms)
        self.last_frame_ms   = 0.0         # most recent frame time (ms)
        self.frame_time_hist = []          # rolling history for graph

    @property
    def weight(self) -> float:
        """Higher weight = preferred for next batch. Lower queue = higher weight."""
        if not self.alive:
            return 0.0
        return 1.0 / (self.queue_depth + 1)

    @property
    def is_overloaded(self) -> bool:
        return self.queue_depth > cfg.SLAVE_QUEUE_LIMIT

    def to_dict(self) -> dict:
        return {
            "slave_id":    self.slave_id,
            "partition":   self.partition,
            "queue_depth": self.queue_depth,
            "fps":         round(self.fps, 1),
            "cpu_pct":     round(self.cpu_pct, 1),
            "gpu_pct":     round(self.gpu_pct, 1),
            "alive":       self.alive,
            "total_frames":     self.total_frames,
            "avg_frame_ms":     round(self.avg_frame_ms, 1),
            "last_frame_ms":    round(self.last_frame_ms, 1),
            "frame_time_hist":  list(self.frame_time_hist[-30:]),
            "last_seen_s": round(time.time() - self.last_seen, 1),
        }


class LoadBalancer:
    """
    Tracks slave health and routes frame batches to the least-loaded slave.
    Runs its own background thread consuming TOPIC_HEARTBEAT.
    """

    def __init__(self):
        self._slaves: dict[str, SlaveInfo] = {}   # slave_id → SlaveInfo
        self._lock   = threading.Lock()
        self._next_partition = 0   # for new slave assignment

        # Start heartbeat consumer thread
        t = threading.Thread(target=self._heartbeat_consumer, daemon=True)
        t.start()

        # Start staleness checker
        t2 = threading.Thread(target=self._staleness_checker, daemon=True)
        t2.start()

        logger.info("LoadBalancer started.")

    # ── Heartbeat consumer ─────────────────────────────────────────────────────
    def _heartbeat_consumer(self):
        try:
            consumer = KafkaConsumer(
                cfg.TOPIC_HEARTBEAT,
                bootstrap_servers=[cfg.KAFKA_BROKER],
            api_version=cfg.KAFKA_API_VERSION,
                group_id="load_balancer",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode()),
                consumer_timeout_ms=5000,
            )
        except Exception as e:
            logger.warning(f"LoadBalancer consumer failed to start: {e}")
            return

        while True:
            try:
                for msg in consumer:
                    self._process_heartbeat(msg.value)
            except Exception as e:
                logger.debug(f"Heartbeat consumer error: {e}")
                time.sleep(2)

    def _process_heartbeat(self, data: dict):
        slave_id = data.get("slave_id")
        if not slave_id:
            return

        with self._lock:
            if slave_id not in self._slaves:
                # New slave — assign next available partition
                partition = self._next_partition % cfg.KAFKA_PARTITIONS
                self._next_partition += 1
                self._slaves[slave_id] = SlaveInfo(slave_id, partition)
                logger.info(f"New slave registered: {slave_id} → partition {partition}")

            s = self._slaves[slave_id]
            s.queue_depth  = data.get("queue_depth", 0)
            s.fps          = data.get("fps", 0.0)
            s.cpu_pct      = data.get("cpu_pct", 0.0)
            s.gpu_pct      = data.get("gpu_pct", 0.0)
            s.total_frames     = data.get("total_frames",    s.total_frames)
            s.avg_frame_ms     = data.get("avg_frame_ms",    s.avg_frame_ms)
            s.last_frame_ms    = data.get("last_frame_ms",   s.last_frame_ms)
            new_hist = data.get("frame_time_hist", [])
            if new_hist:
                s.frame_time_hist = (s.frame_time_hist + new_hist)[-100:]
            s.last_seen    = time.time()
            if not s.alive:
                logger.info(f"Slave recovered: {slave_id}")
            s.alive = True

    def _staleness_checker(self):
        """Mark slaves as dead if they haven't sent a heartbeat recently."""
        while True:
            time.sleep(cfg.HEARTBEAT_INTERVAL)
            now = time.time()
            with self._lock:
                for s in self._slaves.values():
                    if s.alive and (now - s.last_seen) > cfg.SLAVE_TIMEOUT_S:
                        logger.warning(f"Slave timed out: {s.slave_id}")
                        s.alive = False

    # ── Routing ────────────────────────────────────────────────────────────────
    def best_partition(self, job_id: str) -> tuple[int | None, bytes]:
        """
        FIX 3: Return (partition_int, key_bytes) so the master producer can
        use partition= explicitly. This guarantees frames land on the exact
        partition owned by the target slave, bypassing Kafka key hashing
        which could hash two different slave keys to the same partition.

        Returns:
          (partition_index, key_bytes) when live slaves are known
          (None, job_id.encode())      when no slaves registered yet
              → Kafka round-robins across all partitions (fair default)
        """
        with self._lock:
            live = [s for s in self._slaves.values() if s.alive]

        if not live:
            return None, job_id.encode()

        best = max(live, key=lambda s: s.weight)
        with self._lock:
            best.queue_depth += cfg.FRAMES_PER_BATCH

        return best.partition, f"slave:{best.slave_id}".encode()

    # Keep old name as alias for backwards compatibility
    def best_partition_key(self, job_id: str) -> bytes:
        _, key = self.best_partition(job_id)
        return key

    def acknowledge_result(self, slave_id: str, n_frames: int = 1):
        """Called when a result arrives — decrements that slave's queue estimate."""
        with self._lock:
            s = self._slaves.get(slave_id)
            if s:
                s.queue_depth = max(0, s.queue_depth - n_frames)

    # ── Dashboard data ─────────────────────────────────────────────────────────
    def get_all_frame_time_history(self) -> dict:
        """Return combined frame-time history keyed by slave_id, for dashboard graphs."""
        with self._lock:
            return {
                sid: {
                    "history": list(s.frame_time_hist),
                    "total_frames": s.total_frames,
                    "avg_ms": round(s.avg_frame_ms, 1),
                }
                for sid, s in self._slaves.items()
            }

    def get_status(self) -> dict:
        with self._lock:
            slaves = [s.to_dict() for s in self._slaves.values()]
        live  = sum(1 for s in slaves if s["alive"])
        total_fps = sum(s["fps"] for s in slaves if s["alive"])
        return {
            "slaves":        slaves,
            "live_count":    live,
            "total_count":   len(slaves),
            "total_fps":     round(total_fps, 1),
        }


# Singleton instance used by master
_lb_instance: LoadBalancer | None = None

def get_load_balancer() -> LoadBalancer:
    global _lb_instance
    if _lb_instance is None:
        _lb_instance = LoadBalancer()
    return _lb_instance
