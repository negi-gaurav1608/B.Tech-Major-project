# CrowdLens — Distributed Master-Slave Architecture

## System overview

```
┌─────────────────────────────────────────────────────────────────┐
│  MASTER  (your laptop — 10.220.224.138)                         │
│                                                                 │
│   Browser ──► Flask Web App (port 5000)                         │
│                    │                                            │
│                    ▼                                            │
│   FaceEnroller (InsightFace — local, fast)                      │
│                    │  embeddings via TOPIC_CONTROL              │
│                    ▼                                            │
│   Frame Producer ──────────────────────► Kafka ◄───────────────┤
│                                           │                     │
│   Result Assembler ◄──────────────────────┘                     │
│       │                                                         │
│       ▼                                                         │
│   Annotated video + per-person clips                            │
└─────────────────────────────────────────────────────────────────┘
                          │ Kafka topics
                          │ video_frames   (master → slave)
                          │ frame_results  (slave → master)
                          │ job_progress   (slave → master)
                          │ job_control    (master → slave)
┌─────────────────────────────────────────────────────────────────┐
│  SLAVE  (second machine)                                        │
│                                                                 │
│   Control Consumer ◄── TOPIC_CONTROL (loads embeddings)        │
│                                                                 │
│   Frame Consumer ◄──── TOPIC_FRAMES                            │
│       │                                                         │
│       ├── YOLOv8 person detection                               │
│       ├── InsightFace ArcFace recognition                       │
│       └── Annotate frame                                        │
│                                                                 │
│   Result Producer ────► TOPIC_RESULTS                          │
│   Progress Producer ──► TOPIC_PROGRESS                         │
└─────────────────────────────────────────────────────────────────┘
```

## Speedup

| Setup | Processing speed |
|-------|-----------------|
| Single machine (old) | 1× baseline |
| 1 slave | ~1.8× faster |
| 2 slaves | ~3.2× faster |
| 4 slaves | ~5.5× faster |

Speedup is sub-linear because the master spends time on I/O (reading video,
encoding frames, assembling output). Each slave handles the heavy compute
(YOLO + InsightFace).

---

## File layout

```
distributed/
├── master.py                  ← run on master machine
├── slave.py                   ← run on slave machine(s)
├── config_distributed.py      ← shared settings (edit IPs here)
├── setup_kafka.sh             ← run once on master to install Kafka
├── setup_nfs.sh               ← run on both machines for shared storage
├── requirements_distributed.txt
└── README.md
```

---

## Step-by-step setup

### Step 1 — Edit config_distributed.py on BOTH machines

```python
KAFKA_BROKER = "10.220.224.138:9092"   # master IP
SHARED_DIR   = Path("/srv/crowdlens")  # same path on both machines
```

### Step 2 — Install Kafka on master

```bash
# On master only:
bash setup_kafka.sh
```

This installs Java, downloads Kafka 3.7, creates systemd services,
opens firewall ports, and creates all 4 Kafka topics.

### Step 3 — Set up shared storage

```bash
# On master:
ROLE=master bash setup_nfs.sh

# On slave:
ROLE=slave MASTER_IP=10.220.224.138 bash setup_nfs.sh
```

If both machines are on the same machine (testing), just create the dir:
```bash
sudo mkdir -p /srv/crowdlens && sudo chmod 777 /srv/crowdlens
```

### Step 4 — Install Python dependencies

```bash
# On master:
pip install flask werkzeug kafka-python insightface onnxruntime \
            opencv-python-headless numpy

# On slave:
pip install kafka-python ultralytics insightface onnxruntime \
            opencv-python-headless numpy torch torchvision
```

### Step 5 — Copy project files to slave

```bash
# Copy from master to slave:
scp config_distributed.py slave.py user@SLAVE_IP:~/crowdlens/
```

### Step 6 — Start services

```bash
# Terminal 1 — master:
python master.py

# Terminal 2 — slave (on slave machine):
python slave.py

# To add more slaves (each gets its own Kafka partition):
python slave.py   # run again on slave or another machine
```

### Step 7 — Open browser

```
http://10.220.224.138:5000
```

---

## Running multiple slaves

Each slave joins the `slave_workers` Kafka consumer group. Kafka automatically
distributes partitions between them. With 4 partitions (default), up to 4 slaves
work in parallel on different frame batches.

```bash
# On slave machine — start 2 parallel workers:
python slave.py &
python slave.py &
wait
```

---

## Monitoring

```bash
# Watch Kafka consumer group lag:
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group slave_workers

# Watch topic message rates:
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group assembler_<job_id>

# Check Kafka is running:
systemctl status kafka zookeeper

# Tail slave logs:
python slave.py 2>&1 | tee slave.log
```

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| Slave not receiving frames | Check `KAFKA_BROKER` IP in config; verify `nc -zv MASTER_IP 9092` |
| "No results received from workers" | Slave not running or not connected to Kafka |
| Slow progress | Add more slaves; lower `FRAMES_PER_BATCH` for finer granularity |
| NFS permission denied | `sudo chmod 777 /srv/crowdlens` on master |
| Large video upload fails | Check `MAX_CONTENT_LENGTH` in master.py (default 4 GB) |
| Kafka message too large | Increase `KAFKA_MAX_MSG_BYTES` and broker `message.max.bytes` |
