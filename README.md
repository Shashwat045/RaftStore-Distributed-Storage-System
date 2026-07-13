# RaftStore — Distributed Fault-Tolerant Storage System

A decentralized, fault-tolerant distributed storage system implementing the **Raft consensus algorithm** for leader election, log replication, and real-time synchronization across multiple nodes.

## Features

- 🗳️ Leader election and log replication via Raft consensus
- 🔄 Real-time synchronization across distributed nodes
- 📁 Distributed file upload, deletion, rollback, and recovery
- 🔗 Replicated metadata consistency with automatic node sync
- 📊 Monitoring dashboard with real-time cluster event tracking
- 🔐 JWT-based authentication
- 🖥️ Multi-node deployment support (localhost)

## Tech Stack

- **Backend:** Python, FastAPI, Uvicorn
- **Consensus:** Raft Algorithm (custom implementation)
- **Auth:** JWT
- **Frontend:** HTML, CSS, JavaScript

## Architecture

RaftStore runs multiple nodes that elect a leader using the Raft protocol. The leader coordinates writes and replicates logs to follower nodes, ensuring consistency even if nodes fail or disconnect. See `DEPLOYMENT.md` for full setup details.

## Running Locally

```bash
git clone https://github.com/Shashwat045/RaftStore-Distributed-Storage-System.git
cd RaftStore-Distributed-Storage-System
pip install -r requirements.txt
uvicorn main:app --reload
```

See `DEPLOYMENT.md` for multi-node cluster setup instructions.

## Author

**Shashwat Yadav** — [GitHub](https://github.com/Shashwat045)
