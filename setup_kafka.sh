#!/bin/bash
# setup_kafka.sh — Install and configure Kafka on the MASTER machine.
# Run once on the master: bash setup_kafka.sh
#
# After this script completes:
#   Master: python master.py
#   Slave : python slave.py   (on the slave machine)

set -e

KAFKA_VERSION="3.9.1"
KAFKA_DIR="/opt/kafka"
MASTER_IP="10.83.253.138"   # ← change to your master machine IP

echo "============================================"
echo "  CrowdLens Kafka Setup"
echo "============================================"

# ── 1. Java ────────────────────────────────────────────────────────────────────
echo "[1/6] Installing Java 21…"
sudo apt-get update -qq
sudo apt-get install -y openjdk-21-jdk curl wget unzip
java -version

# ── 2. Kafka download ──────────────────────────────────────────────────────────
echo "[2/6] Downloading Kafka ${KAFKA_VERSION}…"
if [ ! -d "$KAFKA_DIR" ]; then
  wget "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz" \
     -O /tmp/kafka.tgz
  sudo tar -xzf /tmp/kafka.tgz -C /opt/
  sudo mv "/opt/kafka_2.13-${KAFKA_VERSION}" "$KAFKA_DIR"
fi
echo "export PATH=\$PATH:${KAFKA_DIR}/bin" >> ~/.bashrc
export PATH="$PATH:${KAFKA_DIR}/bin"

# ── 3. Kafka config ─────────────────────────────────────────────────────────────
echo "[3/6] Configuring Kafka…"

sudo tee "${KAFKA_DIR}/config/server.properties" > /dev/null << EOF
broker.id=0
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://${MASTER_IP}:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT
num.network.threads=4
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/tmp/kafka-logs
num.partitions=4
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=2
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
# Allow large messages (10 MB) for video frames
message.max.bytes=10485760
replica.fetch.max.bytes=10485760
EOF

# ── 4. systemd services ────────────────────────────────────────────────────────
echo "[4/6] Creating systemd services…"

sudo tee /etc/systemd/system/zookeeper.service > /dev/null << EOF
[Unit]
Description=Apache ZooKeeper
After=network.target

[Service]
User=$USER
ExecStart=${KAFKA_DIR}/bin/zookeeper-server-start.sh ${KAFKA_DIR}/config/zookeeper.properties
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/kafka.service > /dev/null << EOF
[Unit]
Description=Apache Kafka
After=zookeeper.service

[Service]
User=$USER
ExecStart=${KAFKA_DIR}/bin/kafka-server-start.sh ${KAFKA_DIR}/config/server.properties
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable zookeeper kafka
sudo systemctl start zookeeper
sleep 5
sudo systemctl start kafka
sleep 3

# ── 5. Firewall ────────────────────────────────────────────────────────────────
echo "[5/6] Opening firewall ports…"
sudo ufw allow 9092/tcp comment "Kafka"
sudo ufw allow 2181/tcp comment "ZooKeeper"
sudo ufw allow 5000/tcp comment "CrowdLens Web"
sudo ufw reload || true

# ── 6. Create topics ────────────────────────────────────────────────────────────
echo "[6/6] Creating Kafka topics…"
sleep 3   # wait for broker to be fully ready

for TOPIC in video_frames frame_results job_progress job_control; do
  ${KAFKA_DIR}/bin/kafka-topics.sh \
    --create --if-not-exists \
    --topic "$TOPIC" \
    --bootstrap-server "localhost:9092" \
    --partitions 4 \
    --replication-factor 1 \
    --config max.message.bytes=10485760 \
    --config retention.ms=7200000
  echo "  Created topic: $TOPIC"
done

echo ""
echo "============================================"
echo "  Setup complete!"
echo "============================================"
echo ""
echo "  Kafka broker : ${MASTER_IP}:9092"
echo ""
echo "  Next steps:"
echo "  1. Install NFS (optional, for two machines):"
echo "     bash setup_nfs.sh"
echo ""
echo "  2. Install Python deps on BOTH machines:"
echo "     pip install kafka-python ultralytics insightface onnxruntime"
echo "     pip install flask werkzeug opencv-python-headless numpy"
echo ""
echo "  3. Start master:  python master.py"
echo "  4. Start slave:   python slave.py   (on slave machine)"
echo "  5. Open browser:  http://${MASTER_IP}:5000"
