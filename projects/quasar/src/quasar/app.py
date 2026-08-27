import json
import os
import random
import threading
import time
import urllib.error
import urllib.request
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Step 2 of election: on timeout, become candidate. Still --leader for writes. No votes yet.
_HEARTBEAT_INTERVAL = 0.25
_ELECTION_TIMEOUT_MIN = 0.8
_ELECTION_TIMEOUT_MAX = 1.6

_store: dict[str, str] = {}
_lock = threading.Lock()
_stop = threading.Event()
_role = "follower"
_last_heard = 0.0
_leader_alive = True
_election_timeout = 1.0


@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _role
    _role = "leader" if _is_leader() else "follower"
    _reset_election_timeout()
    threading.Thread(target=_heartbeat_loop, daemon=True).start()
    yield
    _stop.set()


app = FastAPI(
    title="Quasar",
    description="A distributed, replicated key-value store.",
    version="0.1.0",
    lifespan=_lifespan,
)


def _node_id() -> str:
    return os.environ.get("QUASAR_NODE_ID", "A")


def _leader_id() -> str:
    return os.environ.get("QUASAR_LEADER", "A")


def _is_leader() -> bool:
    return _node_id() == _leader_id()


def _require_leader() -> None:
    if _is_leader():
        return
    leader = _leader_id()
    raise HTTPException(
        status_code=409,
        detail={
            "error": "not the leader",
            "leader": leader,
            "leader_url": _peers().get(leader),
        },
    )


def _peers() -> dict[str, str]:
    raw = os.environ.get("QUASAR_PEERS", "")
    peers: dict[str, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part or "=" not in part:
            continue
        node_id, addr = part.split("=", 1)
        peers[node_id.strip()] = addr.strip().rstrip("/")
    return peers


def _other_peers() -> dict[str, str]:
    me = _node_id()
    return {nid: addr for nid, addr in _peers().items() if nid != me}


def _apply_put(key: str, value: str) -> None:
    _store[key] = value


def _apply_delete(key: str) -> None:
    _store.pop(key, None)


def _http_json(
    method: str, url: str, payload: dict | None = None, timeout: float = 5.0
) -> dict:
    data = None if payload is None else json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"} if data is not None else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode()
        return json.loads(raw) if raw else {}


def _fanout(method: str, path: str, payload: dict | None = None) -> list[str]:
    failed: list[str] = []
    for node_id, addr in _other_peers().items():
        try:
            _http_json(method, f"{addr}{path}", payload)
        except urllib.error.URLError as exc:
            print(f"replication to {node_id} failed: {exc}", flush=True)
            failed.append(node_id)
    return failed


def _reset_election_timeout() -> None:
    global _election_timeout, _last_heard
    _election_timeout = random.uniform(_ELECTION_TIMEOUT_MIN, _ELECTION_TIMEOUT_MAX)
    _last_heard = time.monotonic()


def _send_heartbeats() -> None:
    payload = {"leader_id": _node_id()}
    for node_id, addr in _other_peers().items():
        try:
            _http_json("POST", f"{addr}/internal/heartbeat", payload, timeout=1.0)
        except urllib.error.URLError:
            continue


def _heartbeat_loop() -> None:
    global _role, _leader_alive
    while not _stop.wait(_HEARTBEAT_INTERVAL):
        if _is_leader():
            _send_heartbeats()
            continue
        with _lock:
            timed_out = (time.monotonic() - _last_heard) >= _election_timeout
            if timed_out and _role == "follower":
                _role = "candidate"
                _leader_alive = False
                print(
                    f"{_node_id()}: candidate (leader {_leader_id()} heartbeat lost)",
                    flush=True,
                )


class PutBody(BaseModel):
    value: str


class InternalMessage(BaseModel):
    message: str


class HeartbeatBody(BaseModel):
    leader_id: str


@app.get("/health")
def health() -> dict[str, str | bool]:
    with _lock:
        return {
            "status": "ok",
            "service": "quasar",
            "node": _node_id(),
            "role": _role,
            "leader": _leader_id(),
            "leader_alive": True if _is_leader() else _leader_alive,
        }


@app.get("/")
def root() -> dict[str, str | int | bool]:
    with _lock:
        return {
            "name": "quasar",
            "node": _node_id(),
            "role": _role,
            "leader": _leader_id(),
            "leader_alive": True if _is_leader() else _leader_alive,
            "keys": len(_store),
        }


@app.put("/kv/{key}")
def put(key: str, body: PutBody) -> dict[str, object]:
    _require_leader()
    _apply_put(key, body.value)
    failed = _fanout("PUT", f"/internal/kv/{key}", {"value": body.value})
    return {"key": key, "value": body.value, "replication_failed": failed}


@app.get("/kv/{key}")
def get(key: str) -> dict[str, str]:
    if key not in _store:
        raise HTTPException(status_code=404, detail="key not found")
    return {"key": key, "value": _store[key]}


@app.delete("/kv/{key}")
def delete(key: str) -> dict[str, object]:
    _require_leader()
    if key not in _store:
        raise HTTPException(status_code=404, detail="key not found")
    _apply_delete(key)
    failed = _fanout("DELETE", f"/internal/kv/{key}")
    return {"deleted": key, "replication_failed": failed}


@app.put("/internal/kv/{key}")
def replicate_put(key: str, body: PutBody) -> dict[str, str]:
    _apply_put(key, body.value)
    return {"applied": "put", "key": key, "node": _node_id()}


@app.delete("/internal/kv/{key}")
def replicate_delete(key: str) -> dict[str, str]:
    _apply_delete(key)
    return {"applied": "delete", "key": key, "node": _node_id()}


@app.post("/internal/heartbeat")
def receive_heartbeat(body: HeartbeatBody) -> dict[str, str]:
    global _role, _last_heard, _leader_alive
    with _lock:
        _last_heard = time.monotonic()
        _leader_alive = True
        if _role == "candidate":
            _role = "follower"
            print(f"{_node_id()}: follower again (heartbeat from {body.leader_id})", flush=True)
        _reset_election_timeout()
    return {"received_by": _node_id(), "from": body.leader_id}


@app.post("/internal/message")
def receive_internal(body: InternalMessage) -> dict[str, str]:
    return {"received_by": _node_id(), "message": body.message}


@app.post("/internal/send/{target}")
def send_internal(target: str, body: InternalMessage) -> dict[str, object]:
    addr = _peers().get(target)
    if addr is None:
        raise HTTPException(status_code=404, detail=f"unknown peer {target}")

    try:
        peer_reply = _http_json(
            "POST", f"{addr}/internal/message", {"message": body.message}
        )
    except urllib.error.URLError as exc:
        raise HTTPException(status_code=502, detail=f"peer unreachable: {exc}") from exc

    return {"from": _node_id(), "to": target, "response": peer_reply}
