#!/bin/bash
# setup_nfs.sh — Set up shared NFS storage between master and slave.
#
# Run on MASTER:  ROLE=master bash setup_nfs.sh
# Run on SLAVE :  ROLE=slave  MASTER_IP=10.220.224.138 bash setup_nfs.sh

set -e

SHARED_DIR="/srv/crowdlens"
MASTER_IP="${MASTER_IP:-10.220.224.138}"

if [ "$ROLE" = "master" ]; then
    echo "Setting up NFS SERVER (master)…"
    sudo apt-get install -y nfs-kernel-server

    sudo mkdir -p "$SHARED_DIR"
    sudo chmod 777 "$SHARED_DIR"

    # Export to entire subnet
    SUBNET=$(echo "$MASTER_IP" | cut -d. -f1-3).0/24
    echo "${SHARED_DIR} ${SUBNET}(rw,sync,no_subtree_check,no_root_squash)" \
        | sudo tee -a /etc/exports

    sudo exportfs -ra
    sudo systemctl enable nfs-kernel-server
    sudo systemctl restart nfs-kernel-server
    sudo ufw allow from "$SUBNET" to any port nfs
    sudo ufw reload || true

    echo "NFS server ready. Shared dir: $SHARED_DIR"

elif [ "$ROLE" = "slave" ]; then
    echo "Setting up NFS CLIENT (slave)…"
    sudo apt-get install -y nfs-common

    sudo mkdir -p "$SHARED_DIR"

    # Mount
    echo "${MASTER_IP}:${SHARED_DIR}  ${SHARED_DIR}  nfs  defaults,_netdev  0  0" \
        | sudo tee -a /etc/fstab

    sudo mount -a
    echo "NFS client mounted: $MASTER_IP:$SHARED_DIR → $SHARED_DIR"

else
    echo "Usage: ROLE=master bash setup_nfs.sh"
    echo "       ROLE=slave MASTER_IP=<ip> bash setup_nfs.sh"
fi
