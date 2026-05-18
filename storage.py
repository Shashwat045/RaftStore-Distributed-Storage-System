import os
import shutil
import json
import base64
from datetime import datetime

class FileStateMachine:
    def __init__(self, storage_dir):
        self.storage_dir = storage_dir
        self.metadata_path = os.path.join(self.storage_dir, "metadata.json")
        os.makedirs(self.storage_dir, exist_ok=True)
        self.metadata = self._load_metadata()

    def _load_metadata(self):
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, "r") as f:
                data = json.load(f)
                if "files" not in data:
                    data["files"] = {}
                if "users" not in data:
                    data["users"] = {}
                return data
        return {"files": {}, "users": {}}

    def _save_metadata(self):
        with open(self.metadata_path, "w") as f:
            json.dump(self.metadata, f, indent=4)

    def load_raft_state(self):
        path = os.path.join(self.storage_dir, "raft_state.json")
        if os.path.exists(path):
            with open(path, "r") as f:
                return json.load(f)
        return {"current_term": 0, "voted_for": None, "log": [], "commit_index": 0}

    def save_raft_state(self, state):
        path = os.path.join(self.storage_dir, "raft_state.json")
        with open(path, "w") as f:
            json.dump(state, f, indent=4)

    def apply(self, command):
        action = command.get("action")
        cmd_type = command.get("action")
        payload = command.get("payload", {})
        filename = command.get("filename")
        origin_node = command.get("origin_node", "unknown")
        user_id = command.get("user_id", "anonymous")
        file_hash = command.get("hash", "")
        timestamp = command.get("timestamp", datetime.now().isoformat())
        
        if cmd_type == "UPLOAD_FILE":
            b64_content = command.get("content")
            try:
                file_data = base64.b64decode(b64_content)
            except Exception as e:
                print(f"[STORAGE ERROR] Base64 decode failed for {filename}: {e}")
                return {"status": "error", "message": "Corruption during transmission (B64 Decode Failed)"}

            expected_size = command.get("size", 0)
            if expected_size > 0 and len(file_data) != expected_size:
                print(f"[STORAGE WARNING] Size mismatch for {filename}: Expected {expected_size}, Got {len(file_data)}")
            
            if filename not in self.metadata["files"]:
                self.metadata["files"][filename] = {"versions": [], "current_index": -1}
            
            version_index = len(self.metadata["files"][filename]["versions"])
            version_filename = f"{filename}.v{version_index}"
            file_path = os.path.join(self.storage_dir, version_filename)
            
            with open(file_path, "wb") as f:
                f.write(file_data)
            
            print(f"[STORAGE] Committed {filename} (v{version_index}) - {len(file_data)} bytes")
                
            self.metadata["files"][filename]["versions"].append({
                "path": version_filename,
                "timestamp": timestamp,
                "origin_node": origin_node,
                "user_id": user_id,
                "hash": file_hash
            })
            self.metadata["files"][filename]["current_index"] = version_index
            self._save_metadata()
            return {"status": "success", "message": f"File {filename} version {version_index} uploaded"}

        if cmd_type == "DELETE_FILE" or action == "delete":
            if filename in self.metadata["files"]:
                for v_info in self.metadata["files"][filename]["versions"]:
                    p = os.path.join(self.storage_dir, v_info["path"])
                    if os.path.exists(p): os.remove(p)
                del self.metadata["files"][filename]
                self._save_metadata()
                return {"status": "success", "message": f"File {filename} and all versions deleted"}
            return {"status": "error", "message": f"File {filename} not found"}

        if cmd_type == "ROLLBACK_FILE" or action == "rollback":
            target_version = payload.get("version") if payload else command.get("version")
            if filename in self.metadata["files"]:
                if 0 <= target_version < len(self.metadata["files"][filename]["versions"]):
                    self.metadata["files"][filename]["current_index"] = target_version
                    self._save_metadata()
                    return {"status": "success", "message": f"Rolled back {filename} to version {target_version}"}
            return {"status": "error", "message": "Invalid version or file not found"}
        
        return {"status": "error", "message": f"Unknown action: {cmd_type or action}"}


    def get_files_info(self):
        """Returns detailed info for all files."""
        info = []
        for name, data in self.metadata["files"].items():
            curr_idx = data["current_index"]
            curr_v_info = data["versions"][curr_idx]
            file_path = os.path.join(self.storage_dir, curr_v_info["path"])
            size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            
            info.append({
                "filename": name,
                "version": curr_idx,
                "size": size,
                "timestamp": curr_v_info["timestamp"],
                "origin_node": curr_v_info["origin_node"],
                "user_id": curr_v_info.get("user_id", "anonymous"),
                "hash": curr_v_info.get("hash", ""),
                "all_versions": data["versions"],
                "current_index": curr_idx
            })
        return info

    def search_files(self, query):
        """Search files by name or metadata."""
        all_info = self.get_files_info()
        query = query.lower()
        return [f for f in all_info if query in f["filename"].lower() or query in f["origin_node"].lower()]

    def get_file_path(self, filename, version=None):
        if filename not in self.metadata["files"]:
            return None
        
        data = self.metadata["files"][filename]
        if version is None:
            version = data["current_index"]
            
        if 0 <= version < len(data["versions"]):
            v_info = data["versions"][version]
            return os.path.join(self.storage_dir, v_info["path"])
        return None
    
    def get_latest_hash(self, filename):
        files = self.metadata["files"]
        if filename in files:
            curr_idx = files[filename]["current_index"]
            if 0 <= curr_idx < len(files[filename]["versions"]):
                return files[filename]["versions"][curr_idx].get("hash", "")
        return None

