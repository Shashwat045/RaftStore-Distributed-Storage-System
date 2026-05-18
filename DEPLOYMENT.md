# RaftStore Deployment Guide

RaftStore is a distributed, fault-tolerant file storage system. This guide explains how to deploy a cluster across multiple nodes.

## Cluster Configuration

A standard cluster consists of at least three nodes. Each node must be configured with a unique `NODE_ID`, a local `PORT`, and a list of `PEERS`.

### Environment Variables
- `NODE_ID`: Unique name for the node (e.g., `node1`).
- `HOST_IP`: The IP address of the machine (e.g., `192.168.1.10`).
- `PORT`: The port the server will bind to (e.g., `8001`).
- `PEERS`: Comma-separated list of peer URLs (e.g., `http://192.168.1.11:8001,http://192.168.1.12:8001`).
- `STORAGE_DIR`: Local directory for file storage and Raft state.

## Deployment Options

### 1. Multi-Node Startup (Local Machine)
For testing on a single machine, use the automated script:
```powershell
./run_nodes.ps1
```

### 2. Manual Deployment (Individual Machines)
To deploy nodes across different physical machines, use the `deploy.ps1` script:

**On Machine 1 (192.168.1.10):**
```powershell
.\deploy.ps1 -NodeID "node1" -HostIP "192.168.1.10" -Port 8001 -Peers "http://192.168.1.11:8001,http://192.168.1.12:8001"
```

**On Machine 2 (192.168.1.11):**
```powershell
.\deploy.ps1 -NodeID "node2" -HostIP "192.168.1.11" -Port 8001 -Peers "http://192.168.1.10:8001,http://192.168.1.12:8001"
```

**On Machine 3 (192.168.1.12):**
```powershell
.\deploy.ps1 -NodeID "node3" -HostIP "192.168.1.12" -Port 8001 -Peers "http://192.168.1.10:8001,http://192.168.1.11:8001"
```

## Management
- **Stop Cluster**: Run `./stop_nodes.ps1` to terminate all local processes.
- **Data Cleanup**: Delete the `./data` directory to reset the cluster state.
