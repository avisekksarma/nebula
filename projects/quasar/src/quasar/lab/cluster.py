"""Control real Quasar processes. No Raft logic — spawn, pause, observe."""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

NODE_IDS = ("A", "B", "C")
HOST = "127.0.0.1"
BASE_PORT = 8001
DATA_ROOT = Path(tempfile.gettempdir()) / "quasar-lab"


def lab_port() -> int:
    return int(os.environ.get("QUASAR_LAB_PORT", "9000"))


def node_port(node_id: str) -> int:
    return BASE_PORT + NODE_IDS.index(node_id)


def node_url(node_id: str) -> str:
    return f"http://{HOST}:{node_port(node_id)}"


def peers_arg() -> str:
    port = lab_port()
    return ",".join(f"{nid}=http://{HOST}:{port}/p/{nid}" for nid in NODE_IDS)


def data_dir(node_id: str) -> Path:
    return DATA_ROOT / node_id


def http_json(
    method: str,
    url: str,
    payload: dict | None = None,
    timeout: float = 2.0,
) -> tuple[int, dict]:
    data = None if payload is None else json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"} if data is not None else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode()
            body = json.loads(raw) if raw else {}
            return int(resp.status), body
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        try:
            body = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            body = {"detail": raw}
        return int(exc.code), body


def apply_entry(store: dict[str, str], entry: dict) -> None:
    op = str(entry.get("operation", ""))
    key = str(entry.get("key", ""))
    if op == "PUT":
        store[key] = str(entry.get("value", ""))
    elif op == "DELETE":
        store.pop(key, None)


def store_from_disk_and_log(node_id: str, log_body: dict) -> dict[str, str]:
    """Rebuild the applied map from files and GET /log. Quasar already applied it."""
    store: dict[str, str] = {}
    snap_path = data_dir(node_id) / "snapshot.json"
    if snap_path.is_file():
        try:
            snap = json.loads(snap_path.read_text())
            raw = snap.get("store")
            if isinstance(raw, dict):
                store = {str(k): str(v) for k, v in raw.items()}
        except (OSError, json.JSONDecodeError, TypeError):
            store = {}
    last_applied = int(log_body.get("last_applied") or 0)
    for entry in log_body.get("entries") or []:
        if not isinstance(entry, dict):
            continue
        try:
            idx = int(entry.get("index", 0))
        except (TypeError, ValueError):
            continue
        if 0 < idx <= last_applied:
            apply_entry(store, entry)
    return store


@dataclass
class Node:
    node_id: str
    process: subprocess.Popen[bytes] | None = None
    paused: bool = False
    last_state: dict[str, object] | None = None
    last_error: str | None = None


class Cluster:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.nodes: dict[str, Node] = {nid: Node(node_id=nid) for nid in NODE_IDS}

    def spawn(self, node_id: str) -> None:
        node = self.nodes[node_id]
        dest = data_dir(node_id)
        dest.mkdir(parents=True, exist_ok=True)
        cmd = [
            sys.executable,
            "-m",
            "quasar",
            "--node-id",
            node_id,
            "--host",
            HOST,
            "--port",
            str(node_port(node_id)),
            "--peers",
            peers_arg(),
            "--data-dir",
            str(dest),
        ]
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        node.process = subprocess.Popen(cmd, env=env)
        node.paused = False
        node.last_error = None

    def wait_ready(self, node_id: str, timeout: float = 5.0) -> bool:
        node = self.nodes[node_id]
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if node.process is not None and node.process.poll() is not None:
                node.last_error = f"exited {node.process.returncode}"
                return False
            try:
                status, body = http_json(
                    "GET", f"{node_url(node_id)}/health", timeout=0.3
                )
                if status == 200 and body.get("node") == node_id:
                    return True
            except (urllib.error.URLError, TimeoutError, OSError):
                pass
            time.sleep(0.1)
        node.last_error = "did not become ready"
        return False

    def _cont(self, node: Node) -> None:
        if (
            node.paused
            and node.process is not None
            and node.process.poll() is None
            and node.process.pid is not None
        ):
            os.kill(node.process.pid, signal.SIGCONT)
            node.paused = False

    def stop(self, node_id: str) -> None:
        node = self.nodes[node_id]
        proc = node.process
        if proc is None:
            return
        self._cont(node)
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=1)
        node.process = None
        node.paused = False

    def start_all(self) -> None:
        DATA_ROOT.mkdir(parents=True, exist_ok=True)
        for nid in NODE_IDS:
            self.spawn(nid)
        failed: list[str] = []
        for nid in NODE_IDS:
            if not self.wait_ready(nid):
                failed.append(f"{nid} ({self.nodes[nid].last_error})")
        if failed:
            self.stop_all()
            raise RuntimeError("nodes failed to start: " + ", ".join(failed))

    def stop_all(self) -> None:
        for nid in NODE_IDS:
            self.stop(nid)

    def wipe_data(self) -> None:
        if DATA_ROOT.exists():
            shutil.rmtree(DATA_ROOT, ignore_errors=True)

    def reset(self) -> None:
        self.stop_all()
        time.sleep(0.2)
        self.wipe_data()
        with self._lock:
            for node in self.nodes.values():
                node.last_state = None
                node.last_error = None
        self.start_all()

    def pause(self, node_id: str) -> None:
        if not hasattr(signal, "SIGSTOP"):
            raise RuntimeError("pause requires SIGSTOP (macOS/Linux)")
        node = self.nodes[node_id]
        if node.paused:
            return
        if node.process is None or node.process.poll() is not None:
            raise RuntimeError(f"{node_id} is not running")
        if node.process.pid is None:
            raise RuntimeError(f"{node_id} has no pid")
        os.kill(node.process.pid, signal.SIGSTOP)
        node.paused = True

    def resume(self, node_id: str) -> None:
        node = self.nodes[node_id]
        if not node.paused:
            return
        if node.process is None or node.process.poll() is not None:
            node.paused = False
            raise RuntimeError(f"{node_id} is not running")
        self._cont(node)

    def restart(self, node_id: str) -> None:
        self.stop(node_id)
        self.nodes[node_id].last_state = None
        self.spawn(node_id)
        if not self.wait_ready(node_id):
            raise RuntimeError(
                f"{node_id} failed to restart ({self.nodes[node_id].last_error})"
            )

    def is_paused(self, node_id: str) -> bool:
        return self.nodes[node_id].paused

    def is_running(self, node_id: str) -> bool:
        node = self.nodes[node_id]
        return node.process is not None and node.process.poll() is None

    def observe(self, node_id: str) -> dict[str, object]:
        node = self.nodes[node_id]
        base: dict[str, object] = {
            "node": node_id,
            "port": node_port(node_id),
            "paused": node.paused,
            "reachable": False,
            "status": "down",
        }
        if node.paused:
            out = dict(node.last_state) if node.last_state else {}
            out.update(base)
            out["paused"] = True
            out["reachable"] = False
            out["status"] = "paused"
            return out
        if not self.is_running(node_id):
            out = dict(node.last_state) if node.last_state else {}
            out.update(base)
            out["error"] = node.last_error or "not running"
            out["status"] = "down"
            return out
        try:
            h_status, health = http_json(
                "GET", f"{node_url(node_id)}/health", timeout=0.4
            )
            l_status, log_body = http_json(
                "GET", f"{node_url(node_id)}/log", timeout=0.4
            )
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            out = dict(node.last_state) if node.last_state else {}
            out.update(base)
            out["error"] = str(exc)
            return out
        if h_status != 200 or l_status != 200:
            out = dict(node.last_state) if node.last_state else {}
            out.update(base)
            out["error"] = f"http {h_status}/{l_status}"
            return out
        entries = list(log_body.get("entries") or [])
        commit_index = int(health.get("commit_index") or 0)
        last_applied = int(log_body.get("last_applied") or 0)
        snapshot_index = int(log_body.get("snapshot_index") or 0)
        indexes = [int(e["index"]) for e in entries if isinstance(e, dict) and "index" in e]
        log_lo = min(indexes) if indexes else None
        log_hi = max(indexes) if indexes else None
        merged: dict[str, object] = {
            **health,
            **base,
            "reachable": True,
            "paused": False,
            "status": "alive",
            "log": entries,
            "commit_index": commit_index,
            "last_applied": last_applied,
            "snapshot_index": snapshot_index,
            "log_from": log_lo,
            "log_to": log_hi,
            "store": store_from_disk_and_log(node_id, log_body),
        }
        node.last_state = merged
        node.last_error = None
        return merged

    def snapshot(self) -> dict[str, object]:
        nodes_out: dict[str, object] = {}
        for nid in NODE_IDS:
            nodes_out[nid] = self.observe(nid)
        leader = None
        for nid in NODE_IDS:
            state = nodes_out[nid]
            if (
                isinstance(state, dict)
                and state.get("reachable")
                and state.get("role") == "leader"
            ):
                leader = nid
                break
        return {"leader": leader, "nodes": nodes_out}

    def find_leader(self) -> str | None:
        for nid in NODE_IDS:
            if self.nodes[nid].paused or not self.is_running(nid):
                continue
            try:
                status, body = http_json(
                    "GET", f"{node_url(nid)}/health", timeout=0.4
                )
            except (urllib.error.URLError, TimeoutError, OSError):
                continue
            if status == 200 and body.get("role") == "leader":
                return nid
        return None
