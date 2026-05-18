# run_nodes.ps1
Write-Host "Starting RaftStore Cluster (Original Distributed Mode)..." -ForegroundColor Cyan

# Ensure data directories exist
if (!(Test-Path "./data")) { New-Item -ItemType Directory -Path "./data" }

# Start Node 1
Start-Process powershell -ArgumentList "-NoExit", "-Command", "`$env:NODE_ID='node1'; `$env:PORT='8001'; `$env:PEERS='http://localhost:8002,http://localhost:8003'; uvicorn server:app --host 0.0.0.0 --port 8001"
Write-Host "-> Started Node 1 on port 8001"

# Start Node 2
Start-Process powershell -ArgumentList "-NoExit", "-Command", "`$env:NODE_ID='node2'; `$env:PORT='8002'; `$env:PEERS='http://localhost:8001,http://localhost:8003'; uvicorn server:app --host 0.0.0.0 --port 8002"
Write-Host "-> Started Node 2 on port 8002"

# Start Node 3
Start-Process powershell -ArgumentList "-NoExit", "-Command", "`$env:NODE_ID='node3'; `$env:PORT='8003'; `$env:PEERS='http://localhost:8001,http://localhost:8002'; uvicorn server:app --host 0.0.0.0 --port 8003"
Write-Host "-> Started Node 3 on port 8003"

Write-Host "`nNodes are running independently. Access any node directly (e.g., http://localhost:8001)" -ForegroundColor Green
