# RaftStore: Distributed Fault-Tolerant Storage

A high-performance, distributed file storage system built on the Raft consensus algorithm. RaftStore provides a unified view of your files while replicating data across multiple independent nodes to ensure high availability and data integrity.

## Features
- **Strong Consistency**: Uses the Raft consensus algorithm for all metadata operations.
- **Data Replication**: Automatically replicates files across the cluster.
- **Fault Tolerance**: Maintains availability even if nodes fail.
- **Version Control**: Built-in support for file versioning and rollbacks.
- **Distributed Architecture**: True peer-to-peer system with no single point of failure.

## Quick Start

1. **Install Dependencies**:
   ```bash
   pip install fastapi uvicorn httpx python-multipart pyjwt
   ```

2. **Start the Cluster**:
   ```powershell
   ./run_nodes.ps1
   ```

3. **Access the Dashboard**:
   Open [http://localhost:8001](http://localhost:8001) in your browser.

## Architecture
RaftStore nodes communicate via a custom implementation of the Raft protocol. Each node maintains a local state machine that is kept in sync with the rest of the cluster. When a file is uploaded, the leader node proposes a consensus entry, and the file is replicated to followers once a majority of nodes have acknowledged the request.

For detailed deployment instructions, see [DEPLOYMENT.md](DEPLOYMENT.md).
