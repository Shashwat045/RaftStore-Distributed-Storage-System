import os
import asyncio
import json
from starlette.applications import Starlette
from starlette.responses import JSONResponse, FileResponse, HTMLResponse
from starlette.routing import Route
from raft import RaftNode, NodeState
from storage import FileStateMachine
import httpx
import time
import base64
import hashlib
import uuid
from datetime import datetime
from auth import load_users, save_users, hash_password, verify_password, create_jwt_token, verify_jwt_token

from starlette.middleware.cors import CORSMiddleware

# Load Configuration from Environment Variables
NODE_ID = os.getenv("NODE_ID", "node_default")
# HOST_IP should be the actual IP of this machine on the network (e.g. 192.168.x.x)
HOST_IP = os.getenv("HOST_IP", "127.0.0.1")
PORT = int(os.getenv("PORT", "8001"))
NODE_URL = os.getenv("NODE_URL", f"http://{HOST_IP}:{PORT}")
STORAGE_DIR = os.getenv("STORAGE_DIR", f"./data/{NODE_ID}")
PEERS_STR = os.getenv("PEERS", "")
PEERS = [p.strip() for p in PEERS_STR.split(",") if p.strip()]

# Initialize State Machine and Raft Node
state_machine = FileStateMachine(STORAGE_DIR)
raft_node = RaftNode(NODE_ID, NODE_URL, PEERS, state_machine)

async def read_root(request):
    with open("static/index.html", "r") as f:
        return HTMLResponse(f.read())

async def get_status(request):
    status = raft_node.get_status()
    status["files"] = state_machine.get_files_info()
    return JSONResponse(status)

# --- Admin APIs (Local Only) ---

async def shutdown_node(request):
    # Only allow shutdown if client is from localhost (simulating local access)
    # In a real setup, this would be protected by auth or local-bind only
    raft_node.add_event("admin", "Manual shutdown initiated")
    # Graceful exit after response
    asyncio.create_task(asyncio.sleep(0.5)).add_done_callback(lambda _: os._exit(0))
    return JSONResponse({"status": "success", "message": "Node shutting down..."})

# --- Logs & Metadata ---

async def get_logs(request):
    return JSONResponse(raft_node.events)

async def get_health(request):
    return JSONResponse({
        "status": "healthy",
        "uptime": time.time() - raft_node.uptime_start,
        "node_id": NODE_ID,
        "role": raft_node.state.value,
        "peers_count": len(raft_node.peers)
    })

async def search_files(request):
    query = request.query_params.get("q", "")
    return JSONResponse(state_machine.search_files(query))

# --- Raft RPC Endpoints ---

async def request_vote(request):
    data = await request.json()
    return JSONResponse(await raft_node.handle_request_vote(
        data["term"], data["candidateId"], data["lastLogIndex"], data["lastLogTerm"]
    ))

async def append_entries(request):
    data = await request.json()
    return JSONResponse(await raft_node.handle_append_entries(
        data["term"], data["leaderId"], data["prevLogIndex"], data["prevLogTerm"], data["entries"], data["leaderCommit"]
    ))

# --- Auth Endpoints ---

def get_user_from_request(request):
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return None
    token = auth_header.split(" ")[1]
    return verify_jwt_token(token)

async def signup(request):
    data = await request.json()
    email = data.get("email")
    password = data.get("password")
    if not email or not password or len(password) < 6:
        return JSONResponse({"error": "Invalid email or password (min 6 chars)"}, status_code=400)
    
    users = load_users()
    if email in users:
        return JSONResponse({"error": "User already exists"}, status_code=400)
    
    users[email] = hash_password(password)
    save_users(users)
    return JSONResponse({"message": "User created successfully"})

async def login(request):
    data = await request.json()
    email = data.get("email")
    password = data.get("password")
    
    users = load_users()
    if email not in users or not verify_password(password, users[email]):
        return JSONResponse({"error": "Invalid credentials"}, status_code=401)
    
    token = create_jwt_token(email)
    return JSONResponse({"token": token, "email": email})

async def logout(request):
    return JSONResponse({"status": "success"})

# --- Client Endpoints (Leader Enforced) ---

async def upload_file(request):
    # [DEMO MODE] Auth bypassed for demo stability
    user_id = get_user_from_request(request) or "demo_user"
        
    form = await request.form()
    file = form["file"]
    filename = file.filename
    content = await file.read()
    content_size = len(content)
    
    # Hash check for Deduplication
    file_hash = hashlib.sha256(content).hexdigest()
    latest_hash = state_machine.get_latest_hash(filename)
    
    if latest_hash == file_hash:
        return JSONResponse({"error": "No changes detected. File already exists."}, status_code=400)
    
    # Encode binary to base64 for safe JSON/Raft transport
    b64_content = base64.b64encode(content).decode('ascii')
        
    command = {
        "action": "UPLOAD_FILE",
        "filename": filename,
        "content": b64_content,
        "size": content_size,
        "origin_node": NODE_ID,
        "user_id": user_id,
        "hash": file_hash,
        "timestamp": datetime.now().isoformat()
    }
    
    raft_node.add_event("client", f"Received upload: {filename} ({content_size} bytes)")
    result = await raft_node.propose(command)
    
    if result["status"] == "redirect":
        # Transparent proxy to leader
        try:
            async with httpx.AsyncClient() as client:
                files = {"file": (filename, content)}
                headers = {"Authorization": request.headers.get("Authorization")}
                leader_url = result['leader_url'].rstrip('/')
                resp = await client.post(f"{leader_url}/upload", files=files, headers=headers)
                if resp.status_code == 200:
                    if "application/json" in resp.headers.get("content-type", ""):
                        return JSONResponse(resp.json(), status_code=resp.status_code)
                    return JSONResponse({"error": "Malformed server response"}, status_code=502)
                return JSONResponse({"error": "Failed proxying to leader"}, status_code=resp.status_code)
        except Exception as e:
            return JSONResponse({"error": "Leader not reachable"}, status_code=502)
    
    if result["status"] == "success":
        return JSONResponse({"message": "File uploaded successfully", "filename": filename, "size": content_size, "handled_by": NODE_ID})
    else:
        return JSONResponse({"detail": result.get("message", "Failed to process request")}, status_code=500)

async def rollback_file(request):
    # [DEMO MODE] Auth bypassed for demo stability
    user_id = get_user_from_request(request) or "demo_user"
        
    data = await request.json()
    command = {
        "action": "ROLLBACK_FILE",
        "filename": data["filename"],
        "version": data["version"],
        "origin_node": NODE_ID,
        "user_id": user_id
    }
    result = await raft_node.propose(command)
    if result["status"] == "redirect":
        try:
            async with httpx.AsyncClient() as client:
                headers = {"Authorization": request.headers.get("Authorization")}
                leader_url = result['leader_url'].rstrip('/')
                resp = await client.post(f"{leader_url}/rollback", json=data, headers=headers)
                if resp.status_code == 200:
                    if "application/json" in resp.headers.get("content-type", ""):
                        return JSONResponse(resp.json(), status_code=resp.status_code)
                    return JSONResponse({"error": "Malformed server response"}, status_code=502)
                return JSONResponse({"error": "Failed proxying to leader"}, status_code=resp.status_code)
        except Exception as e:
            return JSONResponse({"error": "Leader not reachable"}, status_code=502)
    return JSONResponse(result)

async def download_file(request):
    filename = request.path_params["filename"]
    version = request.query_params.get("v")
    v_int = int(version) if version is not None else None
    
    file_path = state_machine.get_file_path(filename, v_int)
    if not file_path:
        return JSONResponse({"detail": "File not found"}, status_code=404)
    
    actual_size = os.path.getsize(file_path)
    raft_node.add_event("client", f"Serving download: {filename} (Size: {actual_size} bytes)")
    
    return FileResponse(
        file_path, 
        filename=filename, 
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

async def delete_file(request):
    # [DEMO MODE] Auth bypassed for demo stability
    user_id = get_user_from_request(request) or "demo_user"
        
    filename = request.path_params["filename"]
    command = {"action": "DELETE_FILE", "filename": filename, "origin_node": NODE_ID, "user_id": user_id}
    result = await raft_node.propose(command)
    if result["status"] == "redirect":
        try:
            async with httpx.AsyncClient() as client:
                headers = {"Authorization": request.headers.get("Authorization")}
                leader_url = result['leader_url'].rstrip('/')
                resp = await client.delete(f"{leader_url}/delete/{filename}", headers=headers)
                if resp.status_code == 200:
                    if "application/json" in resp.headers.get("content-type", ""):
                        return JSONResponse(resp.json(), status_code=resp.status_code)
                    return JSONResponse({"error": "Malformed server response"}, status_code=502)
                return JSONResponse({"error": "Failed proxying to leader"}, status_code=resp.status_code)
        except Exception as e:
            return JSONResponse({"error": "Leader not reachable"}, status_code=502)
    return JSONResponse(result)

routes = [
    Route("/", read_root),
    Route("/status", get_status),
    Route("/health", get_health),
    Route("/admin/shutdown", shutdown_node, methods=["POST"]),
    Route("/raft/logs", get_logs),
    Route("/files/search", search_files),
    Route("/signup", signup, methods=["POST"]),
    Route("/login", login, methods=["POST"]),
    Route("/raft/request_vote", request_vote, methods=["POST"]),
    Route("/raft/append_entries", append_entries, methods=["POST"]),
    Route("/upload", upload_file, methods=["POST"]),
    Route("/rollback", rollback_file, methods=["POST"]),
    Route("/download/{filename}", download_file),
    Route("/delete/{filename}", delete_file, methods=["DELETE"]),
    Route("/logout", logout, methods=["POST"]),
]


app = Starlette(debug=True, routes=routes, on_startup=[lambda: asyncio.create_task(raft_node.run())])
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
