import json
import os
import random
import threading
import time
import urllib.error
import urllib.request
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

_HEARTBEAT_INTERVAL = 0.25
_ELECTION_TIMEOUT_MIN = 0.8
_ELECTION_TIMEOUT_MAX = 1.6

_store: dict[str, str] = {}
_log: list[dict[str, str | int]] = []
_commit_index = 0
_last_applied = 0
_lock = threading.Lock()
_stop = threading.Event()
_role = "follower"
_term = 0
_voted_for: str | None = None
_leader: str | None = None
_last_heard = 0.0
_leader_alive = False
_election_timeout = 1.0
_next_index: dict[str, int] = {}


@asynccontextmanager
async def _lifespan(app: FastAPI):
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


def _require_leader() -> None:
    with _lock:
        if _role == "leader":
            return
        leader = _leader
    raise HTTPException(
        status_code=409,
        detail={
            "error": "not the leader",
            "leader": leader,
            "leader_url": _peers().get(leader) if leader else None,
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


def _cluster_size() -> int:
    peers = _peers()
    n = len(peers)
    if _node_id() not in peers:
        n += 1
    return max(1, n)


def _majority() -> int:
    return _cluster_size() // 2 + 1


def _apply_entry(entry: dict[str, str | int]) -> None:
    op = str(entry["operation"])
    key = str(entry["key"])
    if op == "PUT":
        _store[key] = str(entry["value"])
    elif op == "DELETE":
        _store.pop(key, None)


def _apply_committed() -> None:
    global _last_applied
    while _last_applied < _commit_index:
        _last_applied += 1
        entry = _log[_last_applied - 1]
        _apply_entry(entry)
        print(f"{_node_id()}: apply {entry} (commit_index={_commit_index})", flush=True)


def _last_log_meta() -> tuple[int, int]:
    if not _log:
        return 0, 0
    return len(_log), int(_log[-1]["term"])


def _term_at(index: int) -> int:
    if index <= 0 or index > len(_log):
        return 0
    return int(_log[index - 1]["term"])


def _prefix_matches(prev_log_index: int, prev_log_term: int) -> bool:
    if prev_log_index == 0:
        return True
    if prev_log_index > len(_log):
        return False
    return int(_log[prev_log_index - 1]["term"]) == prev_log_term


def _advance_commit_from_leader(leader_commit: int) -> None:
    global _commit_index
    new = min(leader_commit, len(_log))
    if new > _commit_index:
        _commit_index = new
    _apply_committed()


def _append_log(operation: str, key: str, value: str | None = None) -> dict[str, str | int]:
    with _lock:
        entry: dict[str, str | int] = {
            "index": len(_log) + 1,
            "term": _term,
            "operation": operation,
            "key": key,
        }
        if value is not None:
            entry["value"] = value
        _log.append(entry)
        print(f"{_node_id()}: log append {entry}", flush=True)
        return entry


def _send_append_entries(node_id: str, addr: str) -> bool:
    while True:
        with _lock:
            if _role != "leader":
                return False
            last = len(_log)
            ni = min(_next_index.get(node_id, last + 1), last + 1)
            _next_index[node_id] = ni
            prev_index = ni - 1
            prev_term = _term_at(prev_index)
            entries = [dict(e) for e in _log[prev_index:]]
            payload = {
                "term": _term,
                "leader_id": _node_id(),
                "prev_log_index": prev_index,
                "prev_log_term": prev_term,
                "entries": entries,
                "leader_commit": _commit_index,
            }
        try:
            reply = _http_json(
                "POST", f"{addr}/internal/append", payload, timeout=1.0
            )
        except urllib.error.URLError as exc:
            print(f"log replication to {node_id} failed: {exc}", flush=True)
            return False
        with _lock:
            their_term = int(reply.get("term", 0))
            if their_term > _term:
                _become_follower(their_term)
                return False
            if _role != "leader":
                return False
            if reply.get("success"):
                _next_index[node_id] = ni + len(entries)
                return True
            if ni <= 1:
                return False
            _next_index[node_id] = ni - 1
            print(
                f"{_node_id()}: append to {node_id} rejected, next_index={ni - 1}",
                flush=True,
            )


def _replicate_log() -> list[str]:
    failed: list[str] = []
    for node_id, addr in _other_peers().items():
        if not _send_append_entries(node_id, addr):
            failed.append(node_id)
    return failed


def _push_commit_index() -> None:
    for node_id, addr in _other_peers().items():
        _send_append_entries(node_id, addr)


def _maybe_commit(index: int, acks: int) -> None:
    global _commit_index
    if acks < _majority():
        return
    with _lock:
        if index > _commit_index:
            _commit_index = index
            print(
                f"{_node_id()}: commit_index={_commit_index} (acks={acks}/{_majority()})",
                flush=True,
            )
        _apply_committed()
    _push_commit_index()


def _http_json(
    method: str, url: str, payload: dict | None = None, timeout: float = 5.0
) -> dict:
    data = None if payload is None else json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"} if data is not None else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode()
        return json.loads(raw) if raw else {}


def _reset_election_timeout() -> None:
    global _election_timeout, _last_heard
    _election_timeout = random.uniform(_ELECTION_TIMEOUT_MIN, _ELECTION_TIMEOUT_MAX)
    _last_heard = time.monotonic()


def _become_follower(new_term: int | None = None) -> None:
    global _role, _term, _voted_for, _leader, _leader_alive
    if new_term is not None and new_term > _term:
        if _role in ("leader", "candidate"):
            print(
                f"{_node_id()}: stepping down {_role} → follower (term {_term} → {new_term})",
                flush=True,
            )
        _term = new_term
        _voted_for = None
        _leader = None
        _leader_alive = False
    _role = "follower"
    _reset_election_timeout()


def _send_heartbeats() -> None:
    for node_id, addr in _other_peers().items():
        with _lock:
            if _role != "leader":
                return
        _send_append_entries(node_id, addr)


def _start_election() -> None:
    global _role, _term, _voted_for, _leader, _leader_alive, _next_index
    with _lock:
        if _role == "leader":
            return
        _role = "candidate"
        _term += 1
        me = _node_id()
        _voted_for = me
        _leader = None
        _leader_alive = False
        term = _term
        last_log_index, last_log_term = _last_log_meta()
        _reset_election_timeout()
        print(f"{me}: candidate term={term} (requesting votes)", flush=True)

    votes = 1
    majority = _majority()
    for node_id, addr in _other_peers().items():
        try:
            reply = _http_json(
                "POST",
                f"{addr}/internal/vote",
                {
                    "term": term,
                    "candidate_id": me,
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term,
                },
                timeout=1.0,
            )
        except urllib.error.URLError as exc:
            print(f"{me}: vote request to {node_id} failed: {exc}", flush=True)
            continue
        with _lock:
            their_term = int(reply.get("term", 0))
            if their_term > _term:
                _become_follower(their_term)
                return
            if _role != "candidate" or _term != term:
                return
            if reply.get("vote_granted"):
                votes += 1
                print(f"{me}: got vote from {node_id} ({votes}/{majority})", flush=True)
                if votes >= majority:
                    _role = "leader"
                    _leader = me
                    _leader_alive = True
                    _next_index = {nid: len(_log) + 1 for nid in _other_peers()}
                    print(f"{me}: leader term={term}", flush=True)
                    break

    with _lock:
        won = _role == "leader"
    if won:
        _send_heartbeats()


def _heartbeat_loop() -> None:
    while not _stop.wait(_HEARTBEAT_INTERVAL):
        with _lock:
            role = _role
            timed_out = (time.monotonic() - _last_heard) >= _election_timeout
        if role == "leader":
            _send_heartbeats()
        elif timed_out:
            _start_election()


class PutBody(BaseModel):
    value: str


class HeartbeatBody(BaseModel):
    term: int
    leader_id: str
    leader_commit: int = 0


class VoteBody(BaseModel):
    term: int
    candidate_id: str
    last_log_index: int = 0
    last_log_term: int = 0


class AppendBody(BaseModel):
    term: int = 0
    leader_id: str = ""
    prev_log_index: int = 0
    prev_log_term: int = 0
    entries: list[dict[str, str | int]] = Field(default_factory=list)
    leader_commit: int = 0


@app.get("/health")
def health() -> dict[str, str | int | bool | None]:
    with _lock:
        return {
            "status": "ok",
            "service": "quasar",
            "node": _node_id(),
            "role": _role,
            "term": _term,
            "leader": _leader,
            "leader_alive": True if _role == "leader" else _leader_alive,
            "commit_index": _commit_index,
        }


@app.get("/log")
def get_log() -> dict[str, object]:
    with _lock:
        return {
            "node": _node_id(),
            "role": _role,
            "term": _term,
            "commit_index": _commit_index,
            "entries": list(_log),
        }


@app.put("/kv/{key}")
def put(key: str, body: PutBody) -> dict[str, object]:
    _require_leader()
    entry = _append_log("PUT", key, body.value)
    log_failed = _replicate_log()
    acks = 1 + len(_other_peers()) - len(log_failed)
    _maybe_commit(int(entry["index"]), acks)
    with _lock:
        commit_index = _commit_index
    return {
        "key": key,
        "value": body.value,
        "log": entry,
        "commit_index": commit_index,
        "log_replication_failed": log_failed,
    }


@app.get("/kv/{key}")
def get(key: str) -> dict[str, str]:
    with _lock:
        if key not in _store:
            raise HTTPException(status_code=404, detail="key not found")
        return {"key": key, "value": _store[key]}


@app.delete("/kv/{key}")
def delete(key: str) -> dict[str, object]:
    _require_leader()
    with _lock:
        if key not in _store:
            raise HTTPException(status_code=404, detail="key not found")
    entry = _append_log("DELETE", key)
    log_failed = _replicate_log()
    acks = 1 + len(_other_peers()) - len(log_failed)
    _maybe_commit(int(entry["index"]), acks)
    with _lock:
        commit_index = _commit_index
    return {
        "deleted": key,
        "log": entry,
        "commit_index": commit_index,
        "log_replication_failed": log_failed,
    }


@app.post("/internal/append")
def receive_append(body: AppendBody) -> dict[str, object]:
    global _role, _term, _leader, _leader_alive
    with _lock:
        if body.term < _term:
            return {
                "term": _term,
                "success": False,
                "last_log_index": len(_log),
            }
        if body.term > _term or _role in ("leader", "candidate"):
            _become_follower(body.term)
        _term = body.term
        _role = "follower"
        _leader = body.leader_id
        _leader_alive = True
        _reset_election_timeout()

        if not _prefix_matches(body.prev_log_index, body.prev_log_term):
            print(
                f"{_node_id()}: append reject prev_index={body.prev_log_index} "
                f"prev_term={body.prev_log_term}",
                flush=True,
            )
            return {
                "term": _term,
                "success": False,
                "last_log_index": len(_log),
            }

        for entry in body.entries:
            idx = int(entry["index"])
            if idx <= len(_log):
                existing_term = int(_log[idx - 1]["term"])
                incoming_term = int(entry["term"])
                if existing_term == incoming_term:
                    continue
                if idx <= _commit_index:
                    print(
                        f"{_node_id()}: refuse truncate committed index={idx}",
                        flush=True,
                    )
                    return {
                        "term": _term,
                        "success": False,
                        "last_log_index": len(_log),
                    }
                print(
                    f"{_node_id()}: conflict at index={idx} "
                    f"(had term {existing_term}, got {incoming_term}); truncated",
                    flush=True,
                )
                del _log[idx - 1 :]
                _log.append(dict(entry))
                print(f"{_node_id()}: log append {entry}", flush=True)
            elif idx == len(_log) + 1:
                _log.append(dict(entry))
                print(f"{_node_id()}: log append {entry}", flush=True)
            else:
                print(
                    f"{_node_id()}: skip gap index={idx} expected={len(_log) + 1}",
                    flush=True,
                )
                return {
                    "term": _term,
                    "success": False,
                    "last_log_index": len(_log),
                }

        _advance_commit_from_leader(body.leader_commit)
        return {
            "term": _term,
            "success": True,
            "last_log_index": len(_log),
        }


@app.post("/internal/heartbeat")
def receive_heartbeat(body: HeartbeatBody) -> dict[str, object]:
    global _role, _term, _leader, _leader_alive
    with _lock:
        if body.term < _term:
            return {
                "term": _term,
                "success": False,
                "last_log_index": len(_log),
            }
        if body.term > _term or _role in ("leader", "candidate"):
            _become_follower(body.term)
        _term = body.term
        _role = "follower"
        _leader = body.leader_id
        _leader_alive = True
        _reset_election_timeout()
        _advance_commit_from_leader(body.leader_commit)
        return {
            "term": _term,
            "success": True,
            "last_log_index": len(_log),
        }


@app.post("/internal/vote")
def receive_vote(body: VoteBody) -> dict[str, object]:
    global _role, _term, _voted_for
    with _lock:
        if body.term < _term:
            return {"term": _term, "vote_granted": False}
        if body.term > _term:
            _become_follower(body.term)
        my_index, my_term = _last_log_meta()
        if body.last_log_term != my_term:
            log_ok = body.last_log_term > my_term
        else:
            log_ok = body.last_log_index >= my_index
        granted = (_voted_for is None or _voted_for == body.candidate_id) and log_ok
        if granted:
            _voted_for = body.candidate_id
            _reset_election_timeout()
            print(
                f"{_node_id()}: granted vote to {body.candidate_id} term={_term}",
                flush=True,
            )
        return {"term": _term, "vote_granted": granted}
