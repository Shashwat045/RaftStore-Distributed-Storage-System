# stop_nodes.ps1
Write-Host "Stopping all RaftStore nodes..." -ForegroundColor Yellow

taskkill /f /im uvicorn.exe /t
taskkill /f /im python.exe /t

Write-Host "All nodes stopped." -ForegroundColor Red
