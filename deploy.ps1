# deploy.ps1
param (
    [Parameter(Mandatory=$true)] [string]$NodeID,
    [Parameter(Mandatory=$true)] [string]$HostIP,
    [Parameter(Mandatory=$true)] [int]$Port,
    [Parameter(Mandatory=$true)] [string]$Peers,
    [string]$StorageDir = "./data/"
)

$actualStorageDir = "$StorageDir$NodeID"
Write-Host "Deploying RaftStore Node: $NodeID" -ForegroundColor Cyan
Write-Host "Address: http://${HostIP}:${Port}"
Write-Host "Peers: $Peers"
Write-Host "Storage: $actualStorageDir"

$env:NODE_ID = $NodeID
$env:HOST_IP = $HostIP
$env:PORT = $Port
$env:PEERS = $Peers
$env:STORAGE_DIR = $actualStorageDir

uvicorn server:app --host 0.0.0.0 --port $Port
