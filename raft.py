import asyncio
import random
import time
import httpx
import os
import json
from enum import Enum
from datetime import datetime

class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

class RaftNode:
    def __init__(self, node_id, node_url, peers, state_machine):
        self.node_id = node_id
        self.node_url = node_url
        self.peers = list(peers)  # List of peer URLs
        self.state_machine = state_machine
        
        self.state = NodeState.FOLLOWER
        
        saved_state = self.state_machine.load_raft_state()
        self.current_term = saved_state.get("current_term", 0)
        self.voted_for = saved_state.get("voted_for", None)
        self.log = saved_state.get("log", [])
        
        self.commit_index = saved_state.get("commit_index", 0)
        self.last_applied = self.commit_index
        
        # Leader specific state
        self.next_index = {peer: 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        
        self.leader_id = None
        self.last_heartbeat = time.time()
        # Randomized election timeout between 3 and 5 seconds for stability
        self.election_timeout = random.uniform(3.0, 5.0)
        
        # Logging & Stats
        self.events = [] # List of {time, type, message}
        self.max_events = 100
        self.uptime_start = time.time()
        
        self.lock = asyncio.Lock()
        self.add_event("system", f"Node {node_id} initialized at {node_url}")

    def _save_state(self):
        self.state_machine.save_raft_state({
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": self.log,
            "commit_index": self.commit_index
        })

    def add_event(self, event_type, message):
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        self.events.append({"time": timestamp, "type": event_type, "message": message})
        if len(self.events) > self.max_events:
            self.events.pop(0)

    async def run(self):
        """Main loop for the Raft node."""
        while True:
            try:
                if self.state == NodeState.FOLLOWER:
                    await self._follower_loop()
                elif self.state == NodeState.CANDIDATE:
                    await self._candidate_loop()
                elif self.state == NodeState.LEADER:
                    await self._leader_loop()
            except Exception as e:
                self.add_event("error", f"Main loop error: {str(e)}")
            await asyncio.sleep(0.1)

    async def _follower_loop(self):
        if time.time() - self.last_heartbeat > self.election_timeout:
            self.add_event("election", f"Heartbeat missed! Timeout ({self.election_timeout:.1f}s) reached. Transitioning to CANDIDATE")
            async with self.lock:
                self.state = NodeState.CANDIDATE
                self.leader_id = None

    async def _candidate_loop(self):
        async with self.lock:
            self.current_term += 1
            self.voted_for = self.node_id
            self._save_state()
            self.last_heartbeat = time.time()
            self.election_timeout = random.uniform(3.0, 5.0)
            
        votes = 1  # Vote for self
        self.add_event("election", f"Starting election for term {self.current_term}")
        
        last_log_index = len(self.log)
        last_log_term = self.log[-1]["term"] if self.log else 0
        
        async def request_vote(peer):
            try:
                async with httpx.AsyncClient() as client:
                    resp = await client.post(f"{peer}/raft/request_vote", json={
                        "term": self.current_term,
                        "candidateId": self.node_url,
                        "lastLogIndex": last_log_index,
                        "lastLogTerm": last_log_term
                    }, timeout=2.0)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data["voteGranted"]:
                            self.add_event("election", f"Vote GRANTED by {peer}")
                            return True
                        elif data["term"] > self.current_term:
                            self.add_event("election", f"Higher term {data['term']} found from {peer}. Reverting to FOLLOWER")
                            async with self.lock:
                                self.current_term = data["term"]
                                self.state = NodeState.FOLLOWER
                                self.voted_for = None
                                self._save_state()
                        else:
                            self.add_event("election", f"Vote DENIED by {peer} (Reason: {data.get('reason', 'Unknown')})")
            except Exception as e:
                pass
            return False

        results = await asyncio.gather(*(request_vote(peer) for peer in self.peers))
        votes += sum(results)
        
        if self.state == NodeState.CANDIDATE and votes > (len(self.peers) + 1) / 2:
            self.add_event("election", f"Won election with {votes} votes! Becoming LEADER for term {self.current_term}")
            async with self.lock:
                self.state = NodeState.LEADER
                self.leader_id = self.node_url
                self.next_index = {peer: len(self.log) + 1 for peer in self.peers}
                self.match_index = {peer: 0 for peer in self.peers}
        else:
            await asyncio.sleep(random.uniform(0.5, 1.0))

    async def _leader_loop(self):
        # Send heartbeats / AppendEntries
        async def send_append(peer):
            prev_log_index = self.next_index.get(peer, 1) - 1
            prev_log_term = self.log[prev_log_index-1]["term"] if prev_log_index > 0 else 0
            entries = self.log[prev_log_index:]
            
            try:
                async with httpx.AsyncClient() as client:
                    resp = await client.post(f"{peer}/raft/append_entries", json={
                        "term": self.current_term,
                        "leaderId": self.node_url,
                        "prevLogIndex": prev_log_index,
                        "prevLogTerm": prev_log_term,
                        "entries": entries,
                        "leaderCommit": self.commit_index
                    }, timeout=2.0)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data["success"]:
                            async with self.lock:
                                self.next_index[peer] = prev_log_index + len(entries) + 1
                                self.match_index[peer] = prev_log_index + len(entries)
                        elif data["term"] > self.current_term:
                            self.add_event("system", f"Higher term {data['term']} found. Stepping down as LEADER")
                            async with self.lock:
                                self.current_term = data["term"]
                                self.state = NodeState.FOLLOWER
                                self.voted_for = None
                                self._save_state()
                        else:
                            # Log inconsistency, decrement next_index and retry
                            async with self.lock:
                                self.next_index[peer] = max(1, self.next_index[peer] - 1)
            except Exception:
                pass

        await asyncio.gather(*(send_append(peer) for peer in self.peers))
        
        async with self.lock:
            if self.state != NodeState.LEADER: return

            for n in range(self.commit_index + 1, len(self.log) + 1):
                if self.log[n-1]["term"] == self.current_term:
                    match_count = 1
                    for peer in self.peers:
                        if self.match_index.get(peer, 0) >= n:
                            match_count += 1
                    if match_count > (len(self.peers) + 1) / 2:
                        self.add_event("consensus", f"Index {n} committed by majority ({match_count})")
                        self.commit_index = n
                        self._save_state()
            
            await self._apply_to_state_machine()

        await asyncio.sleep(0.5)

    async def _apply_to_state_machine(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            command = entry["command"]
            if command.get("action") == "upload":
                content_size = len(command.get("content", ""))
                self.add_event("replication", f"COMMITTED: {command['filename']} ({content_size} chars b64)")
            self.state_machine.apply(command)

    async def handle_request_vote(self, term, candidate_id, last_log_index, last_log_term):
        async with self.lock:
            if term > self.current_term:
                self.add_event("system", f"Term update: {self.current_term} -> {term} (from {candidate_id})")
                self.current_term = term
                self.state = NodeState.FOLLOWER
                self.voted_for = None
                self._save_state()
            
            # 1. Term check
            if term < self.current_term:
                return {"term": self.current_term, "voteGranted": False, "reason": "Higher term already exists"}
            
            # 2. Voted check
            if self.voted_for and self.voted_for != candidate_id:
                return {"term": self.current_term, "voteGranted": False, "reason": f"Already voted for {self.voted_for}"}
            
            # 3. Log safety check
            my_last_log_index = len(self.log)
            my_last_log_term = self.log[-1]["term"] if self.log else 0
            
            log_up_to_date = (last_log_term > my_last_log_term) or \
                             (last_log_term == my_last_log_term and last_log_index >= my_last_log_index)
            
            if log_up_to_date:
                self.voted_for = candidate_id
                self._save_state()
                self.last_heartbeat = time.time()
                self.add_event("election", f"Vote GRANTED to {candidate_id} for term {term}")
                return {"term": self.current_term, "voteGranted": True}
            else:
                return {"term": self.current_term, "voteGranted": False, "reason": "Your log is not up-to-date"}

    async def handle_append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        async with self.lock:
            if term > self.current_term:
                self.current_term = term
                self.state = NodeState.FOLLOWER
                self.voted_for = None
                self._save_state()
            
            if term < self.current_term:
                return {"term": self.current_term, "success": False}
            
            # Reset heartbeat timer
            self.state = NodeState.FOLLOWER
            self.leader_id = leader_id
            self.last_heartbeat = time.time()
            
            # Consistency check
            if prev_log_index > 0:
                if prev_log_index > len(self.log) or self.log[prev_log_index-1]["term"] != prev_log_term:
                    return {"term": self.current_term, "success": False}
            
            if entries:
                self.log = self.log[:prev_log_index]
                self.log.extend(entries)
                self._save_state()
                self.add_event("replication", f"Appended {len(entries)} entries from {leader_id}")
            
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log))
                self._save_state()
                await self._apply_to_state_machine()
            
            return {"term": self.current_term, "success": True}

    async def propose(self, command):
        if self.state != NodeState.LEADER:
            if self.leader_id:
                return {"status": "redirect", "leader_url": self.leader_id}
            return {"status": "error", "message": "No leader elected"}
        
        async with self.lock:
            entry = {"term": self.current_term, "command": command}
            self.log.append(entry)
            self._save_state()
            index = len(self.log)
            self.add_event("client", f"Proposed new command at index {index}")
            
        # Wait for commit
        start_wait = time.time()
        while self.commit_index < index:
            if self.state != NodeState.LEADER:
                return {"status": "error", "message": "Lost leadership during proposal"}
            if time.time() - start_wait > 10.0:
                return {"status": "error", "message": "Commit timeout (majority not reached)"}
            await asyncio.sleep(0.1)
            
        return {"status": "success", "index": index}

    def get_status(self):
        return {
            "node_id": self.node_id,
            "role": self.state.value,
            "term": self.current_term,
            "leader": self.leader_id,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "is_syncing": self.last_applied < self.commit_index,
            "uptime": time.time() - self.uptime_start,
            "peers": self.peers
        }

    async def add_peer(self, peer_url):
        async with self.lock:
            if peer_url not in self.peers:
                self.peers.append(peer_url)
                self.next_index[peer_url] = len(self.log) + 1
                self.match_index[peer_url] = 0
                self.add_event("system", f"Added peer: {peer_url}")

    async def remove_peer(self, peer_url):
        async with self.lock:
            if peer_url in self.peers:
                self.peers.remove(peer_url)
                self.next_index.pop(peer_url, None)
                self.match_index.pop(peer_url, None)
                self.add_event("system", f"Removed peer: {peer_url}")
