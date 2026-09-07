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
_snapshot_index = 0  # last Raft index covered by a local snapshot; 0 = none
_snapshot_term = 0


@asynccontextmanager
async def _lifespan(app: FastAPI):
    _load_meta()  # term/vote before we talk to anyone
    _load_wal()  # rebuild _log from disk before election/heartbeats
    # _store stays empty; do not replay the WAL (it may have uncommitted tail)
    print(
        f"{_node_id()}: store empty until leader_commit (log={len(_log)})",
        flush=True,
    )
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
    """Copy committed log entries into the KV map (_store).

    _commit_index is 'the cluster agreed up to here'. _last_applied is 'already
    in the map'. After a snapshot, entries 1.._snapshot_index are no longer in
    _log but their effect is already in _store, so we skip those indexes.
    """
    global _last_applied
    while _last_applied < _commit_index:
        nxt = _last_applied + 1
        if nxt <= _snapshot_index:
            _last_applied = nxt
            continue
        entry = _entry_at(nxt)
        if entry is None:
            break
        _last_applied = nxt
        _apply_entry(entry)
        print(f"{_node_id()}: apply {entry} (commit_index={_commit_index})", flush=True)


def _last_index() -> int:
    """Raft index of the newest entry this node still knows about.

    Usually that is _log[-1]['index']. If /snapshot compacted the whole log,
    _log is empty and we return _snapshot_index instead (the last included
    snapshot entry). Not the same as len(_log), which is only how many
    leftover lines sit in the list.
    """
    if _log:
        return int(_log[-1]["index"])
    return _snapshot_index


def _last_log_meta() -> tuple[int, int]:
    """Last log index and last log term, for RequestVote.

    Followers compare this with their own log to decide if the candidate is
    at least as up-to-date. If the leftover WAL is empty after compaction,
    the snapshot's last_included index/term is that last log position.
    """
    if _log:
        return int(_log[-1]["index"]), int(_log[-1]["term"])
    return _snapshot_index, _snapshot_term


def _log_pos(index: int) -> int | None:
    """Turn a Raft log index into a Python list offset in _log.

    Raft indexes are on the entry itself (1, 2, 3, …) and never change.
    After compaction _log[0] might be Raft index 101, so you cannot use
    _log[index - 1]. Offset is: this index minus the first leftover index.
    Returns None if that index is not in the leftover list.
    """
    if not _log:
        return None
    pos = index - int(_log[0]["index"])
    if pos < 0 or pos >= len(_log):
        return None
    return pos


def _entry_at(index: int) -> dict[str, str | int] | None:
    """Return the leftover log entry with this Raft index.

    None means we do not have it in _log: either it was dropped by a snapshot
    or it has not been replicated to this node yet.
    """
    pos = _log_pos(index)
    if pos is None:
        return None
    return _log[pos]


def _term_at(index: int) -> int:
    """Term stored on the log entry at this Raft index.

    Needed for AppendEntries prev_log_term. If the entry was compacted, use
    the snapshot's last_included_term when index == _snapshot_index.
    Index 0 (empty log prefix) is term 0.
    """
    if index <= 0:
        return 0
    if index == _snapshot_index:
        return _snapshot_term
    entry = _entry_at(index)
    return 0 if entry is None else int(entry["term"])


def _prefix_matches(prev_log_index: int, prev_log_term: int) -> bool:
    """Does our log match the leader's prefix up to prev_log_index?

    The leader sends (prev_index, prev_term) with each AppendEntries. We
    accept only if we have that same index with that same term. prev_index 0
    means 'before the first entry'. If prev_index is exactly the snapshot
    boundary, compare against _snapshot_term because that entry is gone from _log.
    """
    if prev_log_index == 0:
        return True
    if prev_log_index == _snapshot_index:
        return prev_log_term == _snapshot_term
    entry = _entry_at(prev_log_index)
    if entry is None:
        return False
    return int(entry["term"]) == prev_log_term


def _advance_commit_from_leader(leader_commit: int) -> None:
    """Learn commit_index from the leader, then apply newly committed entries.

    We never guess commit_index (it is not on disk). Cap it at our last log
    index so we do not apply entries we do not have.
    """
    global _commit_index
    new = min(leader_commit, _last_index())  # leader tells us; we do not guess
    if new > _commit_index:
        _commit_index = new
    _apply_committed()  # same apply path after restart as during a live commit


def _data_dir() -> str:
    return os.environ.get("QUASAR_DATA_DIR") or os.path.join("data", _node_id())


def _wal_path() -> str:
    return os.path.join(_data_dir(), "log.jsonl")


def _meta_path() -> str:
    return os.path.join(_data_dir(), "raft.json")


def _snapshot_path() -> str:
    return os.path.join(_data_dir(), "snapshot.json")


def _write_snapshot() -> dict[str, object] | None:
    """Write snapshot.json from the applied KV map. Does not delete WAL lines.

    The file is last_included_index (last applied log index), that entry's
    term, and a copy of _store. fsync before returning. None if nothing
    has been applied yet.
    """
    if _last_applied == 0:
        return None
    last_index = _last_applied
    payload = {
        "last_included_index": last_index,
        "last_included_term": _term_at(last_index),
        "store": dict(_store),
    }
    path = _snapshot_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        fh.write(json.dumps(payload))
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    return payload


def _compact_log(up_to: int) -> None:
    """Remove log entries with index <= up_to from the WAL and from _log.

    Those entries are already in snapshot.json, so keeping them would be
    redundant. Must run only after the snapshot is fsynced, or a crash
    could lose both the old WAL prefix and the snapshot.
    """
    remaining = [e for e in _log if int(e["index"]) > up_to]
    _wal_rewrite(remaining)  # drop prefix only after snapshot is on disk
    _log[:] = remaining


def _persist_meta(term: int, voted_for: str | None) -> None:
    """Save currentTerm and votedFor to raft.json.

    Must finish (including fsync) before we grant a vote or start an
    election, so a crash cannot make us vote twice in the same term.
    """
    path = _meta_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        fh.write(json.dumps({"current_term": term, "voted_for": voted_for}))
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def _load_meta() -> None:
    """On startup, restore term and votedFor from raft.json.

    If the file is missing or not valid JSON, leave the in-memory defaults
    (term 0, voted_for None) so the node can still start.
    """
    global _term, _voted_for
    path = _meta_path()
    if not os.path.isfile(path):
        return
    try:
        with open(path) as fh:
            data = json.load(fh)
        _term = int(data["current_term"])
        voted = data.get("voted_for")
        _voted_for = None if voted is None else str(voted)
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
        print(f"{_node_id()}: meta ignore malformed file", flush=True)
        return
    print(
        f"{_node_id()}: meta loaded term={_term} voted_for={_voted_for}",
        flush=True,
    )


def _wal_write_lines(fh, entries: list[dict[str, str | int]]) -> None:
    for entry in entries:
        fh.write(json.dumps(entry, separators=(",", ":")) + "\n")
    fh.flush()
    os.fsync(fh.fileno())  # durable before we touch _log


def _wal_append(entry: dict[str, str | int]) -> None:
    """Append one log entry as a JSON line and fsync before _log.append."""
    path = _wal_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a") as fh:
        _wal_write_lines(fh, [entry])


def _wal_rewrite(entries: list[dict[str, str | int]]) -> None:
    """Replace log.jsonl with exactly these entries (tmp file, fsync, rename).

    Used when we compact after a snapshot or when a conflict truncates the
    uncommitted tail. Cannot edit a single JSONL line in the middle.
    """
    path = _wal_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        _wal_write_lines(fh, entries)
    os.replace(tmp, path)  # swap in the new file only after it is fsynced


def _load_wal() -> None:
    """On startup, rebuild _log by reading log.jsonl one JSON object per line.

    If the process crashed while writing the last line, that line is incomplete
    JSON: skip it and rewrite the file without it. Does not apply entries to
    _store (uncommitted tail must not go into the map).
    """
    path = _wal_path()
    if not os.path.isfile(path):
        return
    loaded: list[dict[str, str | int]] = []
    skipped = False
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                loaded.append(json.loads(line))
            except json.JSONDecodeError:
                skipped = True  # crash mid-write: drop the broken last line
                print(
                    f"{_node_id()}: wal skip trailing incomplete record",
                    flush=True,
                )
                break
    _log.extend(loaded)
    if skipped:
        _wal_rewrite(loaded)  # strip the junk so a later start does not stop here
    print(f"{_node_id()}: wal loaded {len(_log)} entries from {path}", flush=True)


def _append_log(operation: str, key: str, value: str | None = None) -> dict[str, str | int]:
    with _lock:
        entry: dict[str, str | int] = {
            "index": _last_index() + 1,
            "term": _term,
            "operation": operation,
            "key": key,
        }
        if value is not None:
            entry["value"] = value
        _wal_append(entry)  # disk first
        _log.append(entry)
        print(f"{_node_id()}: log append {entry}", flush=True)
        return entry


def _send_append_entries(node_id: str, addr: str) -> bool:
    while True:
        with _lock:
            if _role != "leader":
                return False
            last = _last_index()
            ni = min(_next_index.get(node_id, last + 1), last + 1)
            if ni <= _snapshot_index:
                ni = _snapshot_index + 1
            _next_index[node_id] = ni
            prev_index = ni - 1
            prev_term = _term_at(prev_index)
            start = _log_pos(ni)
            entries = [] if start is None else [dict(e) for e in _log[start:]]
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
            if ni <= _snapshot_index + 1:
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
        _persist_meta(_term, _voted_for)  # durable before we act in the new term
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
        _persist_meta(_term, _voted_for)  # before RequestVote leaves this node
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
                    _next_index = {nid: _last_index() + 1 for nid in _other_peers()}
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
            "voted_for": _voted_for,
            "leader": _leader,
            "leader_alive": True if _role == "leader" else _leader_alive,
            "commit_index": _commit_index,
        }


@app.post("/snapshot")
def create_snapshot() -> dict[str, object]:
    global _snapshot_index, _snapshot_term
    with _lock:
        snap = _write_snapshot()  # fsync first; compact only if that succeeded
        if snap is None:
            raise HTTPException(status_code=400, detail="no committed entries to snapshot")
        _snapshot_index = int(snap["last_included_index"])
        _snapshot_term = int(snap["last_included_term"])
        _compact_log(_snapshot_index)
        print(
            f"{_node_id()}: snapshot last_included_index={_snapshot_index} "
            f"last_included_term={_snapshot_term}; wal remaining={len(_log)}",
            flush=True,
        )
        return {"ok": True, "path": _snapshot_path(), **snap}


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
                "last_log_index": _last_index(),
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
                "last_log_index": _last_index(),
            }

        for entry in body.entries:
            idx = int(entry["index"])
            existing = _entry_at(idx)
            if existing is not None:
                existing_term = int(existing["term"])
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
                        "last_log_index": _last_index(),
                    }
                print(
                    f"{_node_id()}: conflict at index={idx} "
                    f"(had term {existing_term}, got {incoming_term}); truncated",
                    flush=True,
                )
                replaced = [e for e in _log if int(e["index"]) < idx] + [dict(entry)]
                _wal_rewrite(replaced)  # cannot edit a JSONL line; rewrite the file
                _log[:] = replaced
                print(f"{_node_id()}: log append {entry}", flush=True)
            elif idx == _last_index() + 1:
                incoming = dict(entry)
                _wal_append(incoming)  # disk first
                _log.append(incoming)
                print(f"{_node_id()}: log append {entry}", flush=True)
            else:
                print(
                    f"{_node_id()}: skip gap index={idx} expected={_last_index() + 1}",
                    flush=True,
                )
                return {
                    "term": _term,
                    "success": False,
                    "last_log_index": _last_index(),
                }

        _advance_commit_from_leader(body.leader_commit)
        return {
            "term": _term,
            "success": True,
            "last_log_index": _last_index(),
        }


@app.post("/internal/heartbeat")
def receive_heartbeat(body: HeartbeatBody) -> dict[str, object]:
    global _role, _term, _leader, _leader_alive
    with _lock:
        if body.term < _term:
            return {
                "term": _term,
                "success": False,
                "last_log_index": _last_index(),
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
            "last_log_index": _last_index(),
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
            _persist_meta(_term, _voted_for)  # before the vote response
            _reset_election_timeout()
            print(
                f"{_node_id()}: granted vote to {body.candidate_id} term={_term}",
                flush=True,
            )
        return {"term": _term, "vote_granted": granted}
