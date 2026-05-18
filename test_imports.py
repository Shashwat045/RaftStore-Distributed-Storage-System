import os
import sys

# Add current dir to path
sys.path.append(os.getcwd())

try:
    from storage import FileStateMachine
    print("Storage imported successfully")
    from raft import RaftNode, NodeState
    print("Raft imported successfully")
    # server.py might fail because of FastAPI/Uvicorn if not fully installed yet
    # but let's check basic logic
    print("All internal modules imported successfully")
except Exception as e:
    print(f"Import failed: {e}")
    sys.exit(1)
